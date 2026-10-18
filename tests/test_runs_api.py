"""Tests for GET /runs/{id}. Needs DATABASE_URL and the schema applied."""

import os
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.worker import claim_run, complete

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set (needs a Postgres with the schema)"
)

client = TestClient(app)


def test_get_run_returns_the_run_and_its_attempts(db, make_job, make_run):
    run_id = make_run(make_job())
    run = claim_run(db, "w1")
    complete(db, run)

    r = client.get(f"/runs/{run_id}")
    assert r.status_code == 200
    data = r.json()
    assert data["id"] == str(run_id)
    assert data["state"] == "succeeded"
    assert len(data["attempts"]) == 1
    assert data["attempts"][0]["worker_id"] == "w1"
    assert data["attempts"][0]["outcome"] == "succeeded"


def test_unknown_run_is_404():
    r = client.get(f"/runs/{uuid4()}")
    assert r.status_code == 404
    assert r.json()["detail"] == "run not found"


def test_malformed_run_id_is_422():
    assert client.get("/runs/not-a-uuid").status_code == 422
