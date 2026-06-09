-- =============================================================================
-- Nimbus Commerce Platform — Postgres initialization
-- =============================================================================
-- This script runs once on first container startup (when the data
-- directory is empty). It creates the test database alongside the
-- default one.
--
-- The production database is ``nimbus``; the test database is
-- ``nimbus_test``. Both share the same user (``nimbus``) and the
-- test database is what the test suite connects to via
-- ``TEST_DATABASE_URL``.
-- =============================================================================

-- Create the test database if it doesn't already exist.
-- The ``nimbus`` database itself is created automatically by the
-- official postgres image's entrypoint from POSTGRES_DB env.
SELECT 'CREATE DATABASE nimbus_test OWNER nimbus'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'nimbus_test')\gexec

-- Grant the application user the same privileges in the test DB
-- as in the primary DB. The default user is the owner, so most
-- permissions are already in place; this is a defensive belt.
GRANT ALL PRIVILEGES ON DATABASE nimbus_test TO nimbus;
