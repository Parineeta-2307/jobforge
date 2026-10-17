# Design notes

Write your own answer under each question as you build. An interviewer will ask exactly these.
The one-line hints say where the trade-off is, not what to conclude.

## 1. Why Postgres as the queue (SKIP LOCKED) and not Redis, SQS or Kafka?
Hint: one system holds the durable state and the queue, so there is no gap between "run created"
and "run enqueued". What do you give up at higher throughput?

## 2. Leases and heartbeats
- Why a lease and not "mark as running"? What happens when a worker dies?
- Short lease versus long lease: which failure does each one cause?
- Why does a worker heartbeat, and what does it do when the heartbeat says the lease is gone?

## 3. Fencing tokens
A worker stalls (long GC pause, slow network), its lease expires, another worker takes over,
then the first worker wakes up and tries to finish. How does `fencing_token` stop it from
overwriting the newer result?

## 4. Delivery guarantee
Delivery is at-least-once. Why can it not be exactly-once? What does "one visible outcome per run"
require from the code that does the actual work, and how does the run id help?

## 5. Retries
- Which errors are transient and which are permanent?
- Why exponential backoff with jitter? What is a retry storm?
- What decides when a run goes to dead_letters, and how does redrive stay auditable?

## 6. Duplicate occurrences
Why is `UNIQUE (job_id, scheduled_for)` safer than "only one scheduler runs"?

## 7. After downtime
If the scheduler is down for an hour, what happens to the occurrences it missed: skip, catch up,
or expire? Pick one, say why, and say what it costs.

## 8. Fairness between tenants
One tenant submits 10,000 runs. How do the other tenants still get workers? Describe the claim
query change (concurrency cap, round robin across tenants) and its cost.

## 9. Rate limiting (modelled on Jira Cloud's published design)
- Burst: token bucket per tenant per endpoint. Quota: hourly points, heavier operations cost more.
- Why two limits and not one? What does each protect against?
- Why a Lua script in Redis? What breaks without one?
- Headers: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-NearLimit, Retry-After on 429.
- What should a well-behaved client do on a 429?

## 10. What breaks first at 10x, and at 100x?
Name the bottleneck and the next step (partition the runs table, move the queue out of Postgres,
shard by tenant). This is the question that turns a project into a system design answer.
