#pragma once

#include <Common/PODArray.h>
#include <Processors/Formats/Impl/Parquet/ReadCommon.h>

#include <functional>
#include <optional>
#include <span>

namespace DB
{
class ReadBuffer;
class SeekableReadBuffer;
}

namespace DB::Parquet
{

class PrefetchHandle;

class Prefetcher
{
private:
    struct RequestState;
    struct Task;

public:
    void init(ReadBuffer * reader_, const ReadOptions & options, FormatParserSharedResourcesPtr parser_shared_resources_);

    /// Waits for in-progress reads to complete, cancels queued reads that haven't started yet.
    ~Prefetcher();

    /// Not thread safe.
    /// All ranges must be registered before any reading happens (except direct readSync).
    /// Ranges are allowed to overlap a little, but this decreases the effectiveness of range
    /// coalescing, and the overlap might be read from file multiple times.
    /// (We use overlap to simplify bloom filter header reading a little.)
    /// If likely_to_be_used is true, Prefetcher will be more eager to piggy-back this range when
    /// reading other ranges.
    PrefetchHandle registerRange(size_t offset, size_t length, bool likely_to_be_used);

    /// Called at most once, after all registerRange calls and before all enqueue/getRangeData calls.
    void finalizeRanges();

    /// Replace a requested range with a set of disjoint smaller ranges contained within it.
    /// `subranges` must be sorted. `diff` is used to reconcile the consumed `request`'s memory token
    /// (if any) - the read-ahead pump may have started `request` and given it a memory token, so it
    /// must be released through `diff` rather than dropped, or per-stage memory accounting leaks.
    std::vector<PrefetchHandle> splitRange(
        PrefetchHandle request, const std::vector<std::pair</*global_offset*/ size_t, /*length*/ size_t>> & subranges, bool likely_to_be_used,
        MemoryUsageDiff * diff = nullptr);

    /// Kicks off background tasks to prefetch these range, if needed (if not already started, and
    /// prefetching is enabled, and handle is valid).
    /// Adds the range's memory usage to MemoryUsageDiff. Remembers memory_usage->stage so that
    /// PrefetchHandle::reset can later subtract from MemoryUsageDiff correctly.
    void startPrefetch(const std::vector<PrefetchHandle *> & requests_to_start, MemoryUsageDiff * diff);

    /// If prefetched, returns prefetched data.
    /// If prefetch in progress, waits for it to complete.
    /// If prefetch not started, reads the data right here.
    /// The returned pointer is valid as long as the PrefetchHandle is alive.
    std::span<const char> getRangeData(const PrefetchHandle & request);

    /// Pass-through read from the underlying ReadBuffer.
    void readSync(char * to, size_t n, size_t offset);

    size_t getFileSize() const { return file_size; }

    /// Number of read Tasks currently submitted-but-not-completed (scheduled or running), and their
    /// total byte size. Used by ReadManager's read-ahead pump to keep the io_runner saturated up to
    /// an independent IO budget, decoupled from decode/max_threads.
    size_t readsInFlight() const { return reads_in_flight.load(std::memory_order_relaxed); }
    size_t bytesInFlight() const { return bytes_in_flight.load(std::memory_order_relaxed); }

    /// Invoked (on the thread that finished a read - an io_runner thread, or a decode thread doing an
    /// inline read) right after each read completes, so the ReadManager's read-ahead pump can refill
    /// the io_runner the instant a slot frees up, keeping it saturated through decode gaps. Set once
    /// before reads begin; the callback must itself be safe against ReadManager teardown.
    void setReadCompletedCallback(std::function<void()> cb) { read_completed_callback = std::move(cb); }

private:
    friend class PrefetchHandle;

    /// Corresponds to PrefetchHandle.
    struct RequestState
    {
        /// State transitions:
        ///
        /// HasRange -> HasTask
        ///       |      |
        ///       v      v
        ///       Cancelled
        ///
        /// Transition to HasTask happen with `mutex` locked, after assigning `task` and `task_offset`.
        enum class State
        {
            HasRange,
            HasTask,
            Cancelled, // PrefetchHandle was reset
        };

        std::atomic<State> state {State::HasRange};

        /// Whether this range can be piggy-backed to nearby other reads.
        std::atomic<bool> allow_incidental_read {true};

        Task * task = nullptr; // if HasTask
        size_t range_set_idx = 0;
        size_t range_idx = UINT64_MAX;
        size_t length = 0;
        size_t task_offset = 0;
    };

    /// Range that the user wants, before coalescing. Overlapping ranges are allowed, but are not
    /// handled optimally and should be avoided when possible.
    struct RangeState
    {
        RequestState * request;

        size_t start;
        size_t end;

        size_t length() const { return end - start; }
    };

    /// A range to read from file. May cover multiple request ranges.
    /// Tasks' ranges may overlap (if requested ranges overlap).
    struct Task
    {
        enum class State : UInt8
        {
            Scheduled,
            Running,
            Done,
            Exception,
            /// This range is no longer needed, `buf` can be deallocated.
            /// Task may still be running; in this case, the runner will deallocate `buf` when done.
            Deallocated,
        };

        /// Back-pointer to the owning Prefetcher, so the static decreaseTaskRefcount can adjust the
        /// in-flight counters when it cancels a not-yet-run Task.
        Prefetcher * owner = nullptr;

        size_t offset{};
        size_t length{};
        double memory_amplification = 1;

        /// TODO [parquet]: If the range is long, it may make sense to have multiple subtasks reading parts of
        ///       the range in parallel (into subranges of one buffer). E.g. if there's a big column
        ///       chunk with no offset index, and we're reading over network.
        PaddedPODArray<char> buf;

        /// When the underlying read buffer supports zero-copy cached reads, and the Task's range
        /// happens to fit in one retained cache cell, we reference that cell here and don't use `buf`.
        /// Lightweight mirror of SeekableReadBuffer::CachedRegion to avoid the heavy include.
        struct CachedReadRegion
        {
            std::shared_ptr<void> handle;
            const char * data = nullptr;
            size_t size = 0;
            size_t file_offset = 0;
        };
        std::optional<CachedReadRegion> cached_region;

        std::atomic<State> state {State::Scheduled};
        /// How many RequestState-s in HasTask state point to this Task.
        std::atomic<size_t> refcount {};
        /// Notified when the state changes from Running to Done or Exception.
        CompletionNotification completion;
        std::exception_ptr exception;
    };

    enum class ReadMode
    {
        /// The normal mode: use reader->readBigAt, no read_mutex.
        RandomRead,
        /// Slow mode: use reader->seek and reader->next with read_mutex.
        SeekAndRead,
        /// The whole file was read into `entire_file`, no further reading required.
        EntireFileIsInMemory,
    };

    struct RangeSet
    {
        /// Pre-registered ranges. Sorted and immutable after finalizeRanges().
        std::vector<RangeState> ranges;
    };

    FormatParserSharedResourcesPtr parser_shared_resources;

    std::mutex read_mutex;
    ReadMode read_mode{};
    SeekableReadBuffer * reader = nullptr;
    PaddedPODArray<char> entire_file;

    size_t file_size{};
    size_t min_bytes_for_seek{};
    size_t bytes_per_read_task{};

    std::shared_ptr<ShutdownHelper> shutdown = std::make_shared<ShutdownHelper>();

    /// Locked when creating a Task.
    std::mutex mutex;

    /// Arenas.
    std::deque<RequestState> requests;
    std::deque<Task> tasks;
    std::deque<RangeSet> range_sets;

    std::atomic<bool> ranges_finalized {false};

    /// Read Tasks submitted (scheduleTask) but not yet completed/cancelled, and their total bytes.
    /// Incremented in scheduleTask; decremented when a Task reaches a terminal state (runTask
    /// finishing the read, or decreaseTaskRefcount cancelling a not-yet-run Task).
    std::atomic<size_t> reads_in_flight {0};
    std::atomic<size_t> bytes_in_flight {0};

    /// Called after each read completes; see setReadCompletedCallback. Set once before reads begin.
    std::function<void()> read_completed_callback;

    /// (One mutex for all tasks because it's not used frequently.)
    std::mutex exception_mutex;

    void determineReadModeAndFileSize(ReadBuffer * reader_, const ReadOptions & options);
    /// Creates and starts a Task covering this request and possibly other nearby ranges.
    ///
    /// If splitting, the request is being cancelled and replaced by a smaller range
    /// (splitAndPrefetchRange), and only subrange [subrange_start, subrange_end) needs to be read.
    void pickRangesAndCreateTaskIfNotExists(RequestState *, const PrefetchHandle &, bool splitting, size_t start_offset, size_t end_offset, std::unique_lock<std::mutex> lock);
    static void decreaseTaskRefcount(Task * task, size_t amount);
    void scheduleTask(Task * task);
    Task::State runTask(Task * task);
    [[noreturn]] void rethrowException(Task * task);
};

/// Pins a pre-registered range that we may want to read.
/// Call reset to mark the range as no longer needed and subtract its memory usage from MemoryUsageDiff.
/// All handles must be destroyed before Prefetcher is destroyed.
class PrefetchHandle
{
public:
    PrefetchHandle() = default;
    PrefetchHandle(PrefetchHandle &&) noexcept;
    PrefetchHandle & operator=(PrefetchHandle &&) noexcept;

    /// Doesn't record deallocated memory in MemoryUsageDiff. Should only be called on shutdown,
    /// otherwise use reset(diff).
    ~PrefetchHandle();

    explicit operator bool() const { return request != nullptr; }

    void reset(MemoryUsageDiff * diff);

private:
    friend class Prefetcher;
    using RequestState = Prefetcher::RequestState;

    RequestState * request = nullptr;
    MemoryUsageToken memory;

    explicit PrefetchHandle(RequestState * request_);
};

}
