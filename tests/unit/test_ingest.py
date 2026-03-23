import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock

from app.core.main import app

client = TestClient(app)


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


@patch("app.core.main.kafka_manager.send_batch", new_callable=AsyncMock)
def test_ingest_telemetry_success(mock_send_batch):
    mock_send_batch.return_value = "telemetry.raw"

    payload = [
        {"schema_version": "1.0", "payload": {"temp": 22}, "serial_number": "TEST-001"},
        {"schema_version": "1.0", "payload": {"temp": 25}, "serial_number": "TEST-001"},
    ]

    headers = {
        "X-Device-Serial-Number": "TEST-DEVICE-001",
        "Idempotency-Key": "test-key-123",
    }

    response = client.post("/api/v1/telemetry/", json=payload, headers=headers)

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["count"] == 2
    assert "request_id" in data

    mock_send_batch.assert_called_once()


def test_ingest_missing_header():
    payload = [{"schema_version": "1.0", "payload": {"temp": 22}}]

    response = client.post("/api/v1/telemetry/", json=payload)

    assert response.status_code == 400
    assert "details" in response.json()


def test_ingest_empty_batch():
    headers = {"X-Device-Serial-Number": "TEST-DEVICE-001"}

    response = client.post("/api/v1/telemetry/", json=[], headers=headers)

    assert response.status_code == 400
    assert "details" in response.json()
