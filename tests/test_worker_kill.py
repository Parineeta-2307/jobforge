"""The test that makes the project credible. Write it in Weekend 3.

Plan:
  1. Start the stack with 3 workers and submit N jobs whose work takes a couple of seconds.
  2. While they run, kill workers at random (docker kill) and let them restart.
  3. Wait until the queue drains, then assert:
       - every run is succeeded or dead (none lost),
       - every run has exactly one visible side effect (none doubled), judged by idempotency key,
       - no stale worker overwrote a newer worker's result (fencing token).
  4. Record how long recovery took after each kill. Those numbers go in the README and resume.
"""

import pytest


@pytest.mark.skip(reason="Weekend 3")
def test_no_run_lost_or_doubled_when_workers_are_killed():
    raise NotImplementedError
