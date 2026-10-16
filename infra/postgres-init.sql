-- Idempotent bootstrap for local development; also safe to apply to the original scaffold volume.
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'command_app') THEN
    CREATE ROLE command_app LOGIN PASSWORD 'command_app_local';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sink_app') THEN
    CREATE ROLE sink_app LOGIN PASSWORD 'sink_app_local';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'query_app') THEN
    CREATE ROLE query_app LOGIN PASSWORD 'query_app_local';
  END IF;
END; $$;
CREATE SCHEMA IF NOT EXISTS command AUTHORIZATION command_app;
CREATE SCHEMA IF NOT EXISTS read_model AUTHORIZATION sink_app;
GRANT CONNECT ON DATABASE cost_calculation TO command_app, sink_app, query_app;
GRANT USAGE ON SCHEMA read_model TO query_app;
GRANT SELECT ON ALL TABLES IN SCHEMA read_model TO query_app;
ALTER DEFAULT PRIVILEGES FOR ROLE sink_app IN SCHEMA read_model GRANT SELECT ON TABLES TO query_app;
ALTER ROLE query_app SET default_transaction_read_only = on;
