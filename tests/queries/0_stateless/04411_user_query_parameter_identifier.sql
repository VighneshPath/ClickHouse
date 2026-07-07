DROP USER IF EXISTS {CLICKHOUSE_DATABASE:Identifier}, {CLICKHOUSE_DATABASE_1:Identifier}, {CLICKHOUSE_DATABASE_2:Identifier};

CREATE USER {CLICKHOUSE_DATABASE:Identifier};
SELECT count() FROM system.users WHERE name = currentDatabase();
ALTER USER {CLICKHOUSE_DATABASE:Identifier} SETTINGS max_threads = 1;

GRANT SELECT ON *.* TO {CLICKHOUSE_DATABASE:Identifier};
SELECT count() FROM system.grants WHERE user_name = currentDatabase() AND access_type = 'SELECT';
REVOKE SELECT ON *.* FROM {CLICKHOUSE_DATABASE:Identifier};
SELECT count() FROM system.grants WHERE user_name = currentDatabase() AND access_type = 'SELECT';

CREATE USER {CLICKHOUSE_DATABASE_1:Identifier}, {CLICKHOUSE_DATABASE_2:Identifier};
GRANT SELECT ON *.* TO {CLICKHOUSE_DATABASE_1:Identifier}, {CLICKHOUSE_DATABASE_2:Identifier};
SELECT count() FROM system.grants WHERE user_name IN (currentDatabase() || '_1', currentDatabase() || '_2') AND access_type = 'SELECT';
REVOKE SELECT ON *.* FROM {CLICKHOUSE_DATABASE_1:Identifier}, {CLICKHOUSE_DATABASE_2:Identifier} EXCEPT {CLICKHOUSE_DATABASE_2:Identifier};
SELECT count() FROM system.grants WHERE user_name = currentDatabase() || '_1' AND access_type = 'SELECT';
SELECT count() FROM system.grants WHERE user_name = currentDatabase() || '_2' AND access_type = 'SELECT';
REVOKE SELECT ON *.* FROM {CLICKHOUSE_DATABASE_2:Identifier};
SELECT count() FROM system.grants WHERE user_name IN (currentDatabase() || '_1', currentDatabase() || '_2') AND access_type = 'SELECT';

SELECT count() FROM system.users WHERE name IN (currentDatabase(), currentDatabase() || '_1', currentDatabase() || '_2');
DROP USER {CLICKHOUSE_DATABASE:Identifier}, {CLICKHOUSE_DATABASE_1:Identifier}, {CLICKHOUSE_DATABASE_2:Identifier};
SELECT count() FROM system.users WHERE name IN (currentDatabase(), currentDatabase() || '_1', currentDatabase() || '_2');

CREATE USER {CLICKHOUSE_DATABASE:Identifier}@'192.168.%.%';
SELECT count() FROM system.users WHERE name = currentDatabase() || '@192.168.%.%';
GRANT SELECT ON *.* TO {CLICKHOUSE_DATABASE:Identifier}@'192.168.%.%';
SELECT count() FROM system.grants WHERE user_name = currentDatabase() || '@192.168.%.%' AND access_type = 'SELECT';
REVOKE SELECT ON *.* FROM {CLICKHOUSE_DATABASE:Identifier}@'192.168.%.%';
DROP USER {CLICKHOUSE_DATABASE:Identifier}@'192.168.%.%';
SELECT count() FROM system.users WHERE name = currentDatabase() || '@192.168.%.%';

GRANT SELECT ON *.* TO {nonexistent_param:Identifier}; -- { serverError UNKNOWN_QUERY_PARAMETER }
REVOKE SELECT ON *.* FROM {nonexistent_param:Identifier}; -- { serverError UNKNOWN_QUERY_PARAMETER }
DROP USER {nonexistent_param:Identifier}; -- { serverError UNKNOWN_QUERY_PARAMETER }

DROP USER IF EXISTS {CLICKHOUSE_DATABASE:Identifier}, {CLICKHOUSE_DATABASE_1:Identifier}, {CLICKHOUSE_DATABASE_2:Identifier};

-- The same parameter works as the entity name in CREATE / ALTER / DROP ROLE.
DROP ROLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}, {CLICKHOUSE_DATABASE_1:Identifier};
CREATE ROLE {CLICKHOUSE_DATABASE:Identifier}, {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM system.roles WHERE name IN (currentDatabase(), currentDatabase() || '_1');
ALTER ROLE {CLICKHOUSE_DATABASE:Identifier} SETTINGS max_threads = 1;
DROP ROLE {CLICKHOUSE_DATABASE:Identifier}, {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM system.roles WHERE name IN (currentDatabase(), currentDatabase() || '_1');
DROP ROLE {nonexistent_param:Identifier}; -- { serverError UNKNOWN_QUERY_PARAMETER }

SELECT formatQuery('GRANT SELECT ON *.* TO {g:Identifier}');
