\set QUIET 1

SELECT current_database() = :'expected_database' AS database_ok
\gset
\if :database_ok
\else
  \echo 'connected to an unexpected database'
  \quit 42
\endif

SELECT count(*) = 5 AS roles_exist
FROM pg_roles
WHERE rolname IN (
  :'runtime_role',
  :'api_role',
  :'webhook_role',
  :'worker_role',
  :'migrate_role'
)
\gset
\if :roles_exist
\else
  \echo 'one or more PerGo database roles are missing'
  \quit 42
\endif

SELECT bool_and(rolcanlogin) AS login_roles_ok
FROM pg_roles
WHERE rolname IN (
  :'api_role',
  :'webhook_role',
  :'worker_role',
  :'migrate_role'
)
\gset
\if :login_roles_ok
\else
  \echo 'every workload role must be LOGIN'
  \quit 42
\endif

SELECT NOT bool_or(
  rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls
) AS roles_are_unprivileged
FROM pg_roles
WHERE rolname IN (
  :'runtime_role',
  :'api_role',
  :'webhook_role',
  :'worker_role',
  :'migrate_role'
)
\gset
\if :roles_are_unprivileged
\else
  \echo 'PerGo roles must not have PostgreSQL administrative attributes'
  \quit 42
\endif

SELECT NOT rolcanlogin AS runtime_is_group_role
FROM pg_roles
WHERE rolname = :'runtime_role'
\gset
\if :runtime_is_group_role
\else
  \echo 'the shared runtime role must be NOLOGIN'
  \quit 42
\endif

SELECT (
  pg_has_role(:'api_role', :'runtime_role', 'MEMBER') AND
  pg_has_role(:'webhook_role', :'runtime_role', 'MEMBER') AND
  pg_has_role(:'worker_role', :'runtime_role', 'MEMBER')
) AS runtime_membership_ok
\gset
\if :runtime_membership_ok
\else
  \echo 'api, webhook and worker must be members of the runtime role'
  \quit 42
\endif

SELECT pg_get_userbyid(datdba) = :'migrate_role' AS database_owner_ok
FROM pg_database
WHERE datname = :'expected_database'
\gset
\if :database_owner_ok
\else
  \echo 'the migrate role must own the PerGo logical database'
  \quit 42
\endif

SELECT (
  has_database_privilege(:'api_role', :'expected_database', 'CONNECT') AND
  has_database_privilege(:'webhook_role', :'expected_database', 'CONNECT') AND
  has_database_privilege(:'worker_role', :'expected_database', 'CONNECT') AND
  has_database_privilege(:'migrate_role', :'expected_database', 'CONNECT') AND
  has_schema_privilege(:'runtime_role', 'public', 'USAGE') AND
  has_schema_privilege(:'migrate_role', 'public', 'USAGE') AND
  has_schema_privilege(:'migrate_role', 'public', 'CREATE')
) AS base_privileges_ok
\gset
\if :base_privileges_ok
\else
  \echo 'PerGo database/schema privileges are incomplete'
  \quit 42
\endif

SELECT NOT EXISTS (
  SELECT 1
  FROM pg_class AS relation
  JOIN pg_namespace AS namespace
    ON namespace.oid = relation.relnamespace
  WHERE namespace.nspname = 'public'
    AND relation.relkind IN ('r', 'p')
    AND NOT (
      has_table_privilege(:'runtime_role', relation.oid, 'SELECT') AND
      has_table_privilege(:'runtime_role', relation.oid, 'INSERT') AND
      has_table_privilege(:'runtime_role', relation.oid, 'UPDATE') AND
      has_table_privilege(:'runtime_role', relation.oid, 'DELETE')
    )
) AS existing_tables_ok
\gset
\if :existing_tables_ok
\else
  \echo 'runtime privileges are incomplete on existing tables'
  \quit 42
\endif

SELECT NOT EXISTS (
  SELECT 1
  FROM pg_class AS relation
  JOIN pg_namespace AS namespace
    ON namespace.oid = relation.relnamespace
  WHERE namespace.nspname = 'public'
    AND relation.relkind = 'S'
    AND NOT (
      has_sequence_privilege(:'runtime_role', relation.oid, 'SELECT') AND
      has_sequence_privilege(:'runtime_role', relation.oid, 'USAGE') AND
      has_sequence_privilege(:'runtime_role', relation.oid, 'UPDATE')
    )
) AS existing_sequences_ok
\gset
\if :existing_sequences_ok
\else
  \echo 'runtime privileges are incomplete on existing sequences'
  \quit 42
\endif

WITH table_privileges AS (
  SELECT count(DISTINCT privilege_type) = 4 AS ok
  FROM pg_default_acl AS defaults
  CROSS JOIN LATERAL aclexplode(defaults.defaclacl) AS acl
  WHERE defaults.defaclrole = (
    SELECT oid FROM pg_roles WHERE rolname = :'migrate_role'
  )
    AND defaults.defaclnamespace = 'public'::regnamespace
    AND defaults.defaclobjtype = 'r'
    AND acl.grantee = (
      SELECT oid FROM pg_roles WHERE rolname = :'runtime_role'
    )
    AND acl.privilege_type IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
),
sequence_privileges AS (
  SELECT count(DISTINCT privilege_type) = 3 AS ok
  FROM pg_default_acl AS defaults
  CROSS JOIN LATERAL aclexplode(defaults.defaclacl) AS acl
  WHERE defaults.defaclrole = (
    SELECT oid FROM pg_roles WHERE rolname = :'migrate_role'
  )
    AND defaults.defaclnamespace = 'public'::regnamespace
    AND defaults.defaclobjtype = 'S'
    AND acl.grantee = (
      SELECT oid FROM pg_roles WHERE rolname = :'runtime_role'
    )
    AND acl.privilege_type IN ('SELECT', 'USAGE', 'UPDATE')
)
SELECT table_privileges.ok AND sequence_privileges.ok AS default_acl_ok
FROM table_privileges, sequence_privileges
\gset
\if :default_acl_ok
\else
  \echo 'migrate default privileges do not cover runtime tables and sequences'
  \quit 42
\endif

\set QUIET 0
\echo 'PerGo role preflight passed'
