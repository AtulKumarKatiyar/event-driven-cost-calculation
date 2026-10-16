CREATE TABLE calculation_views (
 calculation_id uuid PRIMARY KEY, request_json text, state varchar(20) NOT NULL,
 state_version bigint NOT NULL DEFAULT 0, final_json text,
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE inbox(event_id uuid PRIMARY KEY, received_at timestamptz NOT NULL DEFAULT now());
