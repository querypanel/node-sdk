-- Redshift emulation on plain Postgres for the Redshift adapter integration tests.
--
-- There is no Redshift Docker image, so this fixture recreates the parts of
-- Redshift the SDK relies on: the svv_tables / svv_columns catalog views used
-- for introspection, and a few Redshift-only SQL functions. It validates the
-- SDK's Redshift code path, not real Redshift performance or SQL semantics.

-- ---------------------------------------------------------------------------
-- Data: a "mirror"-style database with several schemas, including the same
-- table name in more than one schema (the shape that broke schema sync).
-- ---------------------------------------------------------------------------

CREATE TABLE public.users (
    id INTEGER PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.users IS 'User accounts table';
COMMENT ON COLUMN public.users.email IS 'User email address';

CREATE TABLE public.events (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    tenant_id VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE VIEW public.active_users AS
    SELECT id, email, tenant_id FROM public.users;

CREATE SCHEMA crm;

-- Same table name as public.users, different columns.
CREATE TABLE crm.users (
    id INTEGER PRIMARY KEY,
    full_name VARCHAR(100),
    tenant_id VARCHAR(50) NOT NULL
);

COMMENT ON TABLE crm.users IS 'CRM contacts';

INSERT INTO public.users (id, email, tenant_id) VALUES
    (1, 'alice@example.com', 'tenant-1'),
    (2, 'bob@example.com', 'tenant-1'),
    (3, 'charlie@example.com', 'tenant-2');

INSERT INTO public.events (id, user_id, tenant_id, name, created_at) VALUES
    (1, 1, 'tenant-1', 'signup', CURRENT_TIMESTAMP - INTERVAL '2 days'),
    (2, 1, 'tenant-1', 'login', CURRENT_TIMESTAMP - INTERVAL '1 day'),
    (3, 2, 'tenant-1', 'signup', CURRENT_TIMESTAMP - INTERVAL '40 days'),
    (4, 3, 'tenant-2', 'signup', CURRENT_TIMESTAMP - INTERVAL '1 day');

INSERT INTO crm.users (id, full_name, tenant_id) VALUES
    (1, 'Alice Smith', 'tenant-1');

-- ---------------------------------------------------------------------------
-- Redshift catalog views. In Redshift these live in pg_catalog and resolve
-- unqualified; here they live in redshift_compat, which is on the search_path
-- and hidden from the views themselves.
-- ---------------------------------------------------------------------------

CREATE SCHEMA redshift_compat;

CREATE VIEW redshift_compat.svv_tables AS
SELECT
    t.table_catalog::varchar AS table_catalog,
    t.table_schema::varchar AS table_schema,
    t.table_name::varchar AS table_name,
    t.table_type::varchar AS table_type,
    obj_description(c.oid, 'pg_class')::varchar AS remarks
FROM information_schema.tables t
JOIN pg_namespace n ON n.nspname = t.table_schema
JOIN pg_class c ON c.relname = t.table_name AND c.relnamespace = n.oid
WHERE t.table_schema <> 'redshift_compat';

CREATE VIEW redshift_compat.svv_columns AS
SELECT
    cols.table_catalog::varchar AS table_catalog,
    cols.table_schema::varchar AS table_schema,
    cols.table_name::varchar AS table_name,
    cols.column_name::varchar AS column_name,
    cols.ordinal_position::integer AS ordinal_position,
    cols.data_type::varchar AS data_type,
    col_description(c.oid, cols.ordinal_position::integer)::varchar AS remarks
FROM information_schema.columns cols
JOIN pg_namespace n ON n.nspname = cols.table_schema
JOIN pg_class c ON c.relname = cols.table_name AND c.relnamespace = n.oid
WHERE cols.table_schema <> 'redshift_compat';

-- ---------------------------------------------------------------------------
-- Redshift-only SQL functions that generated Redshift SQL commonly uses.
-- ---------------------------------------------------------------------------

CREATE FUNCTION redshift_compat.getdate() RETURNS timestamp
    LANGUAGE sql STABLE AS $$ SELECT now()::timestamp $$;

CREATE FUNCTION redshift_compat.dateadd(datepart text, amount integer, ts timestamp)
    RETURNS timestamp
    LANGUAGE sql IMMUTABLE AS $$ SELECT ts + make_interval(
        days => CASE WHEN lower(datepart) IN ('day', 'days', 'd') THEN amount ELSE 0 END,
        hours => CASE WHEN lower(datepart) IN ('hour', 'hours', 'h') THEN amount ELSE 0 END,
        weeks => CASE WHEN lower(datepart) IN ('week', 'weeks', 'w') THEN amount ELSE 0 END,
        months => CASE WHEN lower(datepart) IN ('month', 'months', 'mon') THEN amount ELSE 0 END,
        years => CASE WHEN lower(datepart) IN ('year', 'years', 'y') THEN amount ELSE 0 END
    ) $$;

-- POSTGRES_DB is "dev", Redshift's default database name.
ALTER DATABASE dev SET search_path = "$user", public, redshift_compat;
