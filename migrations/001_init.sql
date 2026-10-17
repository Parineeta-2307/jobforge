-- jobforge schema. Postgres is the source of truth for schedules and runs.
-- A "run" is one scheduled occurrence of a job. Workers claim runs with
-- SELECT ... FOR UPDATE SKIP LOCKED and hold a time-limited lease.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS tenants (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name                  text        NOT NULL UNIQUE,
    points_quota_per_hour integer     NOT NULL DEFAULT 10000,  -- hourly points quota (Atlassian-style)
    burst_rps             integer     NOT NULL DEFAULT 50,     -- per-second token bucket
    max_concurrency       integer     NOT NULL DEFAULT 5,      -- runs executing at once for this tenant
    created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        uuid        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name             text        NOT NULL,
    job_type         text        NOT NULL,                     -- decides the points cost, see app/ratelimit.py
    payload          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    interval_seconds integer,                                  -- NULL means one-off
    max_attempts     integer     NOT NULL DEFAULT 5,
    retention_days   integer     NOT NULL DEFAULT 7,           -- SLA policy: how long run history is kept
    version          integer     NOT NULL DEFAULT 1,
    enabled          boolean     NOT NULL DEFAULT true,
    created_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS job_runs (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),  -- also the idempotency key for the job's side effects
    job_id           uuid        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    tenant_id        uuid        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE, -- denormalised for fairness queries
    scheduled_for    timestamptz NOT NULL,
    state            text        NOT NULL DEFAULT 'ready'
                     CHECK (state IN ('ready', 'leased', 'succeeded', 'retry_wait', 'dead')),
    attempt          integer     NOT NULL DEFAULT 0,
    available_at     timestamptz NOT NULL DEFAULT now(),       -- when it may next be claimed (backoff lives here)
    lease_owner      text,
    lease_expires_at timestamptz,
    fencing_token    bigint      NOT NULL DEFAULT 0,           -- bumped on every claim; stale workers cannot commit
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    -- Two schedulers can never create the same occurrence twice.
    UNIQUE (job_id, scheduled_for)
);

-- Workers look for claimable runs by state and time.
CREATE INDEX IF NOT EXISTS idx_runs_claimable
    ON job_runs (available_at)
    WHERE state IN ('ready', 'retry_wait');

-- Fairness and concurrency caps count leased runs per tenant.
CREATE INDEX IF NOT EXISTS idx_runs_tenant_state ON job_runs (tenant_id, state);

-- Expired leases are found by this index.
CREATE INDEX IF NOT EXISTS idx_runs_lease_expiry
    ON job_runs (lease_expires_at)
    WHERE state = 'leased';

-- One row per attempt, kept for debugging and the SLA verifier.
CREATE TABLE IF NOT EXISTS run_attempts (
    id          bigserial PRIMARY KEY,
    run_id      uuid        NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    attempt     integer     NOT NULL,
    worker_id   text        NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome     text        CHECK (outcome IN ('succeeded', 'failed', 'lease_expired')),
    error       text
);

-- Runs that ran out of attempts or failed permanently. Redrive is an explicit, audited action.
CREATE TABLE IF NOT EXISTS dead_letters (
    run_id      uuid PRIMARY KEY REFERENCES job_runs(id) ON DELETE CASCADE,
    reason      text        NOT NULL,
    moved_at    timestamptz NOT NULL DEFAULT now(),
    redriven_at timestamptz
);
