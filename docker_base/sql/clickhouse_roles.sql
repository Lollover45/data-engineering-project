-- docker_base/sql/clickhouse_roles.sql
-- Roles, users and grants for ClickHouse

CREATE DATABASE IF NOT EXISTS messud;
USE messud;

-- 1) Roles
CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

-- 2) Users
CREATE USER IF NOT EXISTS analyst_full_user
    IDENTIFIED WITH plaintext_password BY '12345678';

CREATE USER IF NOT EXISTS analyst_limited_user
    IDENTIFIED WITH plaintext_password BY '123456';

-- 3) Attach roles and privileges
GRANT analyst_full    TO analyst_full_user;
GRANT analyst_limited TO analyst_limited_user;

ALTER USER analyst_full_user    DEFAULT ROLE analyst_full;
ALTER USER analyst_limited_user DEFAULT ROLE analyst_limited;

GRANT USAGE ON *.* TO analyst_full;
GRANT USAGE ON *.* TO analyst_limited;

GRANT SELECT ON messud.fact_observations TO analyst_full;
GRANT SELECT ON messud.dim_date          TO analyst_full;
GRANT SELECT ON messud.dim_location      TO analyst_full;
GRANT SELECT ON messud.dim_organism      TO analyst_full;
GRANT SELECT ON messud.v_bee_observations_full
    TO analyst_full;

GRANT SELECT ON messud.fact_observations TO analyst_limited;
GRANT SELECT ON messud.dim_date          TO analyst_limited;
GRANT SELECT ON messud.dim_location      TO analyst_limited;
GRANT SELECT ON messud.dim_organism      TO analyst_limited;
GRANT SELECT ON messud.v_bee_observations_limited
    TO analyst_limited;
