import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml", "configs/backups.xml"],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/storage_conf.xml", "configs/backups.xml"],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)

# Each known issue links to its PR review comment.
REVIEW = "https://github.com/ClickHouse/ClickHouse/pull/108443#discussion_r"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def setup_table(name, extra_settings="", n=node):
    n.query(f"DROP TABLE IF EXISTS {name} SYNC")
    n.query("SYSTEM STOP MERGES")  # keep a single, predictable part
    settings = "min_bytes_for_wide_part = 0"
    if extra_settings:
        settings += ", " + extra_settings
    n.query(
        f"""CREATE TABLE {name} (key UInt64, id UInt64, value String,
            PROJECTION p (SELECT key, id, value ORDER BY id))
            ENGINE = MergeTree ORDER BY key SETTINGS {settings}"""
    )
    n.query(
        f"INSERT INTO {name} SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )


def part_dir(name, n=node):  # absolute container path, no trailing slash: .../all_1_1_0
    return (
        n.query(
            f"SELECT path FROM system.parts WHERE table = '{name}' AND active = 1 LIMIT 1"
        )
        .strip()
        .rstrip("/")
    )


def part_name(name, n=node):
    return part_dir(name, n).split("/")[-1]


def proj_query(name, n=node, extra_settings=""):
    settings = "optimize_use_projections = 1"
    if extra_settings:
        settings += ", " + extra_settings
    return n.query(
        f"SELECT count(), sum(key) FROM {name} WHERE id < 200 SETTINGS {settings}"
    ).strip()


def check_table(name, n=node):
    return n.query(
        f"CHECK TABLE {name} SETTINGS check_query_single_value_result = 1"
    ).strip()


def path_exists(p, n=node):
    return (
        n.exec_in_container(
            ["bash", "-c", f"test -e {p} && echo 1 || echo 0"],
            privileged=True,
            user="root",
        ).strip()
        == "1"
    )


def active_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.parts WHERE table = '{name}' AND active = 1"
    ).strip()


def active_projection_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.projection_parts WHERE table = '{name}' AND active = 1"
    ).strip()


def broken_projection_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.projection_parts WHERE table = '{name}' AND is_broken"
    ).strip()


def outdated_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.parts WHERE table = '{name}' AND active = 0"
    ).strip()


def wait_for(predicate, timeout=60):
    for _ in range(timeout * 2):
        if predicate():
            return
        time.sleep(0.5)


def test_default_nested_layout():
    setup_table("t_nested")
    p = part_dir("t_nested")
    assert path_exists(f"{p}/p.proj")
    assert not path_exists(f"{p}.p.proj")
    baseline = proj_query("t_nested")
    node.restart_clickhouse()
    assert proj_query("t_nested") == baseline
    assert active_parts("t_nested") == "1"


def test_flat_layout_setting():
    setup_table("t_flat_setting", "projection_storage_format = 'flat'")
    p = part_dir("t_flat_setting")
    # server wrote the projection as a flat sibling, not nested
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    baseline = proj_query("t_flat_setting")

    # merge keeps the flat layout
    node.query(
        "INSERT INTO t_flat_setting SELECT number, number * 2, toString(number) FROM numbers(1000, 1000)"
    )
    node.query("SYSTEM START MERGES")
    node.query("OPTIMIZE TABLE t_flat_setting FINAL")
    merged = part_dir("t_flat_setting")
    assert path_exists(f"{merged}.p.proj")
    assert not path_exists(f"{merged}/p.proj")
    assert active_parts("t_flat_setting") == "1"
    assert int(active_projection_parts("t_flat_setting")) >= 1
    assert proj_query("t_flat_setting") == baseline

    # survives restart
    node.restart_clickhouse()
    assert active_parts("t_flat_setting") == "1"
    assert proj_query("t_flat_setting") == baseline


def test_flat_layout_after_relocation():
    setup_table("t_flat")
    p = part_dir("t_flat")
    baseline = proj_query("t_flat")
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"mv {p}/p.proj {p}.p.proj"], privileged=True, user="root"
    )
    node.start_clickhouse()
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    assert active_parts("t_flat") == "1"
    assert int(active_projection_parts("t_flat")) >= 1
    assert proj_query("t_flat") == baseline


# Issue #1: outdated-part cleanup must remove flat projection siblings too.
# @pytest.mark.xfail(reason=REVIEW + "3472535408", strict=False)
def test_remove_cleans_flat_siblings():
    setup_table("t_rm", "projection_storage_format = 'flat'")
    p = part_dir("t_rm")
    assert path_exists(f"{p}.p.proj")
    node.query(f"ALTER TABLE t_rm DROP PART '{part_name('t_rm')}'")
    wait_for(lambda: not path_exists(f"{p}.p.proj"))
    assert not path_exists(f"{p}.p.proj")


# Issue #2: FREEZE must copy flat projection siblings.
# @pytest.mark.xfail(reason=REVIEW + "3472535412", strict=False)
def test_freeze_includes_flat_projection():
    setup_table("t_freeze", "projection_storage_format = 'flat'")
    node.query("ALTER TABLE t_freeze FREEZE WITH NAME 'flatproj'")
    found = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/shadow/flatproj -name '*p.proj' | head -1"],
        privileged=True,
        user="root",
    ).strip()
    assert found != ""


# Issue #2: ATTACH PARTITION FROM (clonePart) must copy flat projection siblings.
# @pytest.mark.xfail(reason=REVIEW + "3472535412", strict=False)
def test_attach_partition_from_clones_flat():
    setup_table("t_src", "projection_storage_format = 'flat'")
    setup_table("t_dst", "projection_storage_format = 'flat'")
    node.query("TRUNCATE TABLE t_dst")
    node.query("ALTER TABLE t_dst ATTACH PARTITION tuple() FROM t_src")
    p = part_dir("t_dst")
    assert path_exists(f"{p}.p.proj")
    assert proj_query("t_dst") == proj_query("t_src")


# Issue #3: a part loaded from disk after restart must keep its flat layout for later operations.
# @pytest.mark.xfail(reason=REVIEW + "3472535414", strict=False)
def test_flat_layout_after_restart_operations():
    setup_table("t_reload", "projection_storage_format = 'flat'")
    baseline = proj_query("t_reload")
    node.restart_clickhouse()
    name = part_name("t_reload")
    node.query(f"ALTER TABLE t_reload DETACH PART '{name}'")
    node.query(f"ALTER TABLE t_reload ATTACH PART '{name}'")
    p = part_dir("t_reload")
    assert path_exists(f"{p}.p.proj")
    assert proj_query("t_reload") == baseline


# Issue #5: replicated fetch must materialize projections in the flat layout.
# @pytest.mark.xfail(reason=REVIEW + "3473479560", strict=False)
def test_replicated_fetch_flat_layout():
    for n, replica in ((node, "1"), (node2, "2")):
        n.query("DROP TABLE IF EXISTS t_repl SYNC")
        n.query("SYSTEM STOP MERGES")
        n.query(
            f"""CREATE TABLE t_repl (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id, value ORDER BY id))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_repl', '{replica}')
                ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
        )
    node.query(
        "INSERT INTO t_repl SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    node2.query("SYSTEM SYNC REPLICA t_repl")
    p = part_dir("t_repl", node2)
    assert path_exists(f"{p}.p.proj", node2)
    assert proj_query("t_repl", node2) == proj_query("t_repl", node)


# Issue #6: CHECK TABLE must classify an unknown flat projection (left over after
# DROP PROJECTION on a detached part) the same way as a nested one: a projection
# problem ("unexpected projection directories"), not a broken part. The nested-only
# directory scan misses flat siblings, so the stale "p.proj" checksums entry is
# never cleaned and the whole part is reported broken; on ReplicatedMergeTree the
# part-check thread would then detach the part and try to re-fetch it.
def test_check_table_after_dropped_projection():
    for tname, extra in (
        ("t_chk_nested", ""),
        ("t_chk_flat", "projection_storage_format = 'flat'"),
    ):
        setup_table(tname, extra)
        name = part_name(tname)
        node.query(f"ALTER TABLE {tname} DETACH PART '{name}'")
        node.query(f"ALTER TABLE {tname} DROP PROJECTION p")
        node.query(f"ALTER TABLE {tname} ATTACH PART '{name}'")
        result = node.query(
            f"CHECK TABLE {tname} SETTINGS check_query_single_value_result = 0"
        )
        assert "unexpected projection" in result, (tname, result)
        # the data itself must stay readable
        assert node.query(f"SELECT count() FROM {tname}").strip() == "1000"



# Issues #7 and #8: DETACH/ATTACH moves the part under detached/; the flat sibling must follow,
# both on disk and in the in-memory projection storage. After ATTACH PART the projection part's
# root must point at the attached location, not at detached/<part>.p.proj, and the projection
# must stay usable without a silent fallback to the parent part.
# @pytest.mark.xfail(reason=REVIEW + "3473543140", strict=False)
def test_detach_attach_flat_part():
    setup_table("t_da", "projection_storage_format = 'flat'")
    baseline = proj_query("t_da")
    name = part_name("t_da")
    node.query(f"ALTER TABLE t_da DETACH PART '{name}'")
    node.query(f"ALTER TABLE t_da ATTACH PART '{name}'")
    p = part_dir("t_da")
    table_root = p.rsplit("/", 1)[0]
    assert path_exists(f"{p}.p.proj")
    # no projection sibling may be left behind under detached/
    leftover = node.exec_in_container(
        ["bash", "-c", f"find {table_root}/detached -maxdepth 1 -name '*.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert leftover == "0"
    # fail closed: the attached part must serve the projection from its new location
    assert broken_projection_parts("t_da") == "0"
    assert (
        proj_query("t_da", extra_settings="force_optimize_projection = 1") == baseline
    )


# Issue #12: reloading a part must not mark a present flat projection as broken.
# The consistency check resolves the "p.proj" checksums entry by probing a nested
# directory under the part dir, so a flat sibling is reported missing on every
# load (server restart, DETACH/ATTACH TABLE) and the projection is silently
# marked broken; queries then fall back to the parent part.
# @pytest.mark.xfail(reason=REVIEW + "3481208077", strict=False)
def test_flat_projection_not_broken_on_reload():
    setup_table("t_consist", "projection_storage_format = 'flat'")
    baseline = proj_query("t_consist")
    assert broken_projection_parts("t_consist") == "0"
    node.restart_clickhouse()
    assert broken_projection_parts("t_consist") == "0"
    # fail closed: the projection must actually be used, not silently skipped
    assert (
        proj_query("t_consist", extra_settings="force_optimize_projection = 1")
        == baseline
    )


# Issue #9: BACKUP/RESTORE must store and find flat projection data. Projections
# must be serialized under their logical name (<part>/p.proj/...), so backups are
# layout-independent: any version can restore them, and restore of an old backup
# keeps working. With the physical sibling name (<part>/<part>.p.proj/...) restore
# recreates a bogus nested directory and the part loads broken.
def test_backup_restore_flat():
    setup_table("t_bk", "projection_storage_format = 'flat'")
    baseline = proj_query("t_bk")
    node.query("DROP TABLE IF EXISTS t_bk2 SYNC")
    node.exec_in_container(
        ["bash", "-c", "rm -rf /var/lib/clickhouse/backups/t_bk"],
        privileged=True,
        user="root",
    )
    node.query("BACKUP TABLE t_bk TO File('/var/lib/clickhouse/backups/t_bk')")
    physical_dirs = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/backups/t_bk -type d -name '*.*.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert physical_dirs == "0"
    logical_dirs = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/backups/t_bk -type d -name 'p.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert logical_dirs == "1"
    node.query("RESTORE TABLE t_bk AS t_bk2 FROM File('/var/lib/clickhouse/backups/t_bk')")
    assert node.query("SELECT count() FROM t_bk2").strip() == "1000"
    assert broken_projection_parts("t_bk2") == "0"
    assert proj_query("t_bk2", extra_settings="force_optimize_projection = 1") == baseline
    assert check_table("t_bk2") == "1"


# Issue #11: on zero-copy storage, a mutation must keep blobs hardlinked by flat
# projections. The mutation records hardlinked projection files in the zero-copy
# keep-list; the removal of the source part filters that list by the logical
# projection dir name ("p.proj/..."), so entries recorded under the physical
# sibling name ("<part>.p.proj/...") never match and the shared blobs are deleted
# from under the mutated part.
#
# The scenario needs two replicas: on the mutating replica the shared blobs are
# protected by the local metadata hardlink ref-counts, so the keep-list only
# decides the fate of the blobs on a replica that zero-copy-fetched the mutated
# part (fresh metadata, ref-count 0) and is the last one to unlock the old part.
# node1 executes the mutation (node2's queues are stopped), node2 fetches the
# mutated part, node1 drops the table (releasing its locks), and node2's delayed
# old-part cleanup then decides whether the shared projection blobs survive.
# The mutation must not touch projection columns, otherwise the projection is
# rebuilt instead of hardlinked.
def test_zero_copy_mutation_preserves_flat_projection():
    for n, replica in ((node, "1"), (node2, "2")):
        n.query("DROP TABLE IF EXISTS t_zc SYNC")
        n.query(
            f"""CREATE TABLE t_zc (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id ORDER BY id))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_zc', '{replica}')
                ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
                    storage_policy = 's3', allow_remote_fs_zero_copy_replication = 1,
                    old_parts_lifetime = 20, cleanup_delay_period = 1, max_cleanup_delay_period = 3"""
        )
    node.query(
        "INSERT INTO t_zc SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    node2.query("SYSTEM SYNC REPLICA t_zc")
    baseline = proj_query("t_zc")
    assert proj_query("t_zc", node2) == baseline
    p2 = part_dir("t_zc", node2)
    assert path_exists(f"{p2}.p.proj", node2)
    # make node1 execute the mutation and node2 zero-copy-fetch its result
    node2.query("SYSTEM STOP REPLICATION QUEUES t_zc")
    node.query(
        "ALTER TABLE t_zc UPDATE value = concat(value, 'x') WHERE 1 SETTINGS mutations_sync = 1"
    )
    node2.query("SYSTEM START REPLICATION QUEUES t_zc")
    node2.query("SYSTEM SYNC REPLICA t_zc")
    assert active_parts("t_zc", node2) == "1"
    # release node1's zero-copy locks before node2 removes the old part, so
    # node2's removal is the one that decides the fate of the shared blobs
    node.query("DROP TABLE t_zc SYNC")
    wait_for(lambda: outdated_parts("t_zc", node2) == "0")
    assert outdated_parts("t_zc", node2) == "0"
    assert broken_projection_parts("t_zc", node2) == "0"
    assert (
        proj_query("t_zc", node2, extra_settings="force_optimize_projection = 1")
        == baseline
    )
    assert check_table("t_zc", node2) == "1"
