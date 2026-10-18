-- Makes the fair-claim query fast (W2-4). Safe to run twice.
--
-- claim_run asks, for every tenant: "what is this tenant's oldest claimable run?". The only index
-- that could answer it, idx_runs_claimable, is ordered by available_at alone, so each answer meant
-- walking the global list and throwing away other tenants' rows. With (tenant_id, available_at)
-- the answer is one index probe. The WHERE clause keeps the index small: it holds only runs that
-- are waiting to be claimed, and a run leaves it as soon as it is leased.
-- Measured on synthetic data (docs/results/explain_claim_2026-10-18.txt): the tenant-picking query
-- went from about 45-50 ms to about 0.8 ms with 7,900 waiting runs.
--
-- Plain CREATE INDEX (not CONCURRENTLY): it blocks writes to job_runs while it builds. That is
-- fine for a small table; on a big live table you would use CREATE INDEX CONCURRENTLY.
CREATE INDEX IF NOT EXISTS idx_runs_tenant_claimable
    ON job_runs (tenant_id, available_at)
    WHERE state IN ('ready', 'retry_wait');
