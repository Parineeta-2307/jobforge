"""Tests for POST /jobs. Needs DATABASE_URL and the schema applied."""

import os
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.main import app

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set (needs a Postgres with the schema)"
)

client = TestClient(app)


@pytest.fixture
def tenant_id():
    # The database keeps its data between runs, so the name gets a random suffix.
    r = client.post("/tenants", json={"name": f"test-jobs-{uuid4()}"})
    assert r.status_code == 200
    tid = r.json()["id"]
    yield tid
    # ON DELETE CASCADE removes the tenant's jobs too.
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        c.execute("DELETE FROM tenants WHERE id = %s", (tid,))


def _job(tenant_id, **changes):
    body = {"tenant_id": tenant_id, "name": "job-1", "job_type": "noop"}
    body.update(changes)
    return body


def test_create_job(tenant_id):
    r = client.post("/jobs", json=_job(tenant_id))
    assert r.status_code == 200
    data = r.json()
    assert data["id"]
    assert data["tenant_id"] == tenant_id
    assert data["name"] == "job-1"


def test_same_job_twice_is_409(tenant_id):
    assert client.post("/jobs", json=_job(tenant_id)).status_code == 200
    r = client.post("/jobs", json=_job(tenant_id))
    assert r.status_code == 409
    assert r.json()["detail"] == "job name already exists"


def test_unknown_tenant_is_404():
    r = client.post("/jobs", json=_job(str(uuid4())))
    assert r.status_code == 404
    assert r.json()["detail"] == "tenant not found"


def test_bad_job_type_is_422(tenant_id):
    assert client.post("/jobs", json=_job(tenant_id, job_type="banana")).status_code == 422


def test_blank_name_is_422(tenant_id):
    assert client.post("/jobs", json=_job(tenant_id, name="   ")).status_code == 422


def test_interval_zero_is_422(tenant_id):
    assert client.post("/jobs", json=_job(tenant_id, interval_seconds=0)).status_code == 422


def test_max_attempts_11_is_422(tenant_id):
    assert client.post("/jobs", json=_job(tenant_id, max_attempts=11)).status_code == 422


def test_payload_round_trips(tenant_id):
    payload = {"seconds": 3, "tags": ["a", "b"], "nested": {"x": 1}}
    r = client.post("/jobs", json=_job(tenant_id, job_type="sleep", payload=payload))
    assert r.status_code == 200
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        row = c.execute("SELECT payload FROM jobs WHERE id = %s", (r.json()["id"],)).fetchone()
    # psycopg turns a jsonb column back into a Python dict.
    assert row[0] == payload


@pytest.mark.parametrize("job_type", ["record", "flaky"])
def test_record_and_flaky_job_types_are_accepted(tenant_id, job_type):
    r = client.post("/jobs", json=_job(tenant_id, job_type=job_type))
    assert r.status_code == 200
