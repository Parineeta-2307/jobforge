"""The read-only status page and the numbers behind it."""

import os

import pytest
from fastapi.testclient import TestClient

from app.main import app

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set (needs a Postgres with the schema)"
)

client = TestClient(app)


def test_the_page_and_its_files_are_served():
    page = client.get("/")
    assert page.status_code == 200
    assert "jobforge" in page.text
    assert "/static/app.js" in page.text
    assert client.get("/static/style.css").status_code == 200
    assert client.get("/static/app.js").status_code == 200


def test_status_counts_a_tenants_runs(db, tenant, make_tenant):
    # Insert straight into the tables, so the test does not need an empty queue.
    job_id = db.execute(
        "INSERT INTO jobs (tenant_id, name, job_type) VALUES (%s, 'j', 'noop') RETURNING id",
        (tenant,),
    ).fetchone()[0]
    db.execute(
        "INSERT INTO job_runs (job_id, tenant_id, scheduled_for, available_at) "
        "SELECT %s, %s, now() + g * interval '1 second', now() + interval '1 day' "
        "FROM generate_series(1, 3) g",
        (job_id, tenant),
    )
    data = client.get("/status").json()
    mine = [t for t in data["tenants"] if t["waiting"] == 3 and t["max_concurrency"] == 5]
    assert mine, "the tenant with 3 waiting runs is listed"
    assert data["run_counts"]["ready"] >= 3
    assert set(data) == {"as_of", "run_counts", "tenants", "recent_runs", "dead_letters"}


def test_status_is_read_only_and_survives_html_in_names(db, make_tenant):
    name = "<script>alert(1)</script>"
    tenant_id = make_tenant()
    db.execute("UPDATE tenants SET name = %s WHERE id = %s", (name + "-test", tenant_id))
    response = client.get("/status")
    assert response.status_code == 200  # names are plain JSON strings; app.js uses textContent
    assert client.post("/status").status_code == 405
