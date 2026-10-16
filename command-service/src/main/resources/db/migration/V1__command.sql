CREATE TABLE calculations (
 calculation_id uuid PRIMARY KEY,
 rp varchar(100) NOT NULL, mp varchar(100) NOT NULL, vbu varchar(100) NOT NULL,
 finance_group varchar(100) NOT NULL, request_json text NOT NULL,
 state varchar(20) NOT NULL CHECK (state IN ('REQUESTED','IN_PROGRESS','COMPLETED','FAILED')),
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX one_active_business_key ON calculations(rp,mp,vbu,finance_group)
 WHERE state IN ('REQUESTED','IN_PROGRESS');
CREATE TABLE outbox (
 id bigserial PRIMARY KEY, calculation_id uuid NOT NULL REFERENCES calculations(calculation_id),
 topic text NOT NULL, payload text NOT NULL, published_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX pending_outbox ON outbox(id) WHERE published_at IS NULL;
CREATE TABLE inbox (event_id uuid PRIMARY KEY, received_at timestamptz NOT NULL DEFAULT now());
