#include <gtest/gtest.h>

#include <Storages/ObjectStorage/IObjectIterator.h>

using namespace DB;

/// A Hive-partitioned dataset repeats the same file name in every partition. Sharing one cache entry would let
/// a partition that matched nothing mark its namesakes as having no matching row groups, so they would be
/// skipped unread.
TEST(QueryConditionCacheKey, DistinguishesSameFileNameInDifferentPartitions)
{
    ObjectInfo first("dataset/day=2025-02-05/part-00000.parquet");
    ObjectInfo second("dataset/day=2025-02-27/part-00000.parquet");

    ASSERT_EQ(first.getFileName(), second.getFileName());
    EXPECT_NE(first.getQueryConditionCacheKey(), second.getQueryConditionCacheKey());
}

TEST(QueryConditionCacheKey, IsStableForTheSameObject)
{
    ObjectInfo first("dataset/day=2025-02-05/part-00000.parquet");
    ObjectInfo second("dataset/day=2025-02-05/part-00000.parquet");

    EXPECT_EQ(first.getQueryConditionCacheKey(), second.getQueryConditionCacheKey());
}
