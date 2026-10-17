from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_unimplemented_routes_say_so():
    # Delete this test as you implement the routes.
    assert client.post("/jobs").status_code == 501
