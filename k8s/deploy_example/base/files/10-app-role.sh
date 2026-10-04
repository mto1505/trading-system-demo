#!/bin/sh
# Runs once, on an empty data directory. Creates a least-privilege role so the
# application never connects to PostgreSQL as the superuser.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE ROLE "${APP_DB_USERNAME}" LOGIN PASSWORD '${APP_DB_PASSWORD}';
  CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION "${APP_DB_USERNAME}";
  GRANT CONNECT ON DATABASE "${POSTGRES_DB}" TO "${APP_DB_USERNAME}";
  ALTER ROLE "${APP_DB_USERNAME}" SET search_path = app, public;
  REVOKE ALL ON SCHEMA public FROM PUBLIC;
EOSQL
