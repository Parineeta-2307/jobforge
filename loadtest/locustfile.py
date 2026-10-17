"""Load test for the API. Run: locust -f loadtest/locustfile.py --host http://localhost:8000, then open http://localhost:8089.

TODO (Weekend 3): create a few tenants with different quotas in on_start, then have users
submit jobs. Record in the README: requests per second, p95 latency, and the share of 429s
for a tenant that exceeds its quota (that share proves the limiter works).
"""

from locust import HttpUser, between, task


class ApiUser(HttpUser):
    wait_time = between(0.01, 0.1)

    @task
    def health(self):
        self.client.get("/health")
