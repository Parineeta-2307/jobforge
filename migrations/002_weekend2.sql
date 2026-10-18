-- Weekend 2 schema additions. Safe to run twice (every statement is a no-op the second time).
-- Apply after 001_init.sql. CI applies every file in migrations/ in name order.

-- Redrive (W2-3): a dead run that is redriven gets a fresh set of attempts. We cannot reset
-- job_runs.attempt to 0, because run_attempts rows are matched by (run_id, attempt) and attempt
-- numbers must never repeat. Instead "attempts used since the last redrive" is attempt - attempt_base.
ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS attempt_base integer NOT NULL DEFAULT 0;

-- Idempotent side effect (W2-2): one row per run that has had its visible effect. The primary key
-- on run_id is the guarantee: a second execution of the same run cannot insert a second row.
CREATE TABLE IF NOT EXISTS job_effects (
    run_id     uuid PRIMARY KEY REFERENCES job_runs(id) ON DELETE CASCADE,
    tenant_id  uuid        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Redrive audit trail (W2-3): one row per redrive, so "who brought this back, and why was it dead"
-- stays answerable even after dead_letters.reason is overwritten by a later failure.
CREATE TABLE IF NOT EXISTS redrive_log (
    id              bigserial PRIMARY KEY,
    run_id          uuid        NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    previous_reason text        NOT NULL,
    note            text,
    redriven_at     timestamptz NOT NULL DEFAULT now()
);

-- A tenant name must not be blank. The API already trims names; this makes the database refuse
-- a blank name from any other writer. ADD CONSTRAINT has no IF NOT EXISTS, so check pg_constraint.
-- Note: btrim() with no second argument removes spaces only (not tabs or newlines).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'tenants_name_not_blank' AND conrelid = 'tenants'::regclass
    ) THEN
        ALTER TABLE tenants ADD CONSTRAINT tenants_name_not_blank CHECK (btrim(name) <> '');
    END IF;
END
$$;
