import hashlib
import json
import uuid
from datetime import datetime
from typing import Any

from fastapi import FastAPI, Header, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from .kafka_producer import kafka_manager
from .schemas import TelemetryRequest

app = FastAPI(title="HTTP Ingestion Service")


@app.on_event("startup")
async def startup():
    await kafka_manager.start()


@app.on_event("shutdown")
async def shutdown():
    await kafka_manager.stop()


@app.get("/health")
async def healt_check():
    return {
        "status": "alive",
        "kafka": "connected" if kafka_manager.producer else "disconnected",
    }


def build_idempotency_key(serial_number: str, payload: Any) -> str:
    canonical_payload = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    hasher = hashlib.sha256()
    hasher.update(serial_number.encode("utf-8"))
    hasher.update(b"|")
    hasher.update(canonical_payload.encode("utf-8"))

    return f"http:{hasher.hexdigest()}"


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = exc.errors()
    if any(err["loc"] == ("header", "x-device-serial-number") for err in errors):
        return JSONResponse(
            {"error": "X-Device-Serial-Number header is required"},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    return JSONResponse(
        {"error": "Validation failed", "details": {"errors": errors}},
        status_code=status.HTTP_400_BAD_REQUEST,
    )


@app.post("/api/v1/telemetry/", status_code=status.HTTP_202_ACCEPTED)
async def ingest_telemetry(
    request: Request,
    x_device_serial_number: str = Header(..., alias="X-Device-Serial-Number"),
    idempotency_key: str = Header(None, alias="Idempotency-Key"),
):
    try:
        raw_body = await request.json()
    except json.JSONDecodeError:
        return JSONResponse(
            {"error": "Invalid JSON payload"}, status_code=status.HTTP_400_BAD_REQUEST
        )

    is_batch = isinstance(raw_body, list)

    if is_batch:
        for item in raw_body:
            if isinstance(item, dict):
                item["serial_number"] = x_device_serial_number
                item.pop("ssn", None)
    else:
        if isinstance(raw_body, dict):
            raw_body["serial_number"] = x_device_serial_number
            raw_body.pop("ssn", None)

    if not idempotency_key:
        idempotency_key = build_idempotency_key(x_device_serial_number, raw_body)

    try:
        validated_data = TelemetryRequest.model_validate(raw_body)
    except ValidationError as exc:
        return JSONResponse(
            {
                "error": "validation failed",
                "details": {
                    "errors": jsonable_encoder(exc.errors()),
                },
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    request_id = str(uuid.uuid4())
    received_at = datetime.utcnow().isoformat()
    events = []

    for index, item in enumerate(validated_data.items):
        event_idempotency = (
            f"{idempotency_key}:{index}"
            if is_batch and idempotency_key
            else idempotency_key
        )
        events.append(
            {
                "source": "http",
                "serial_number": x_device_serial_number,
                "request_id": request_id,
                "idempotency_key": event_idempotency,
                "ingest_index": index,
                "received_at": received_at,
                "data": item.model_dump(),
            }
        )

    try:
        target_topic = await kafka_manager.send_batch(events)
    except Exception:
        return JSONResponse(
            {"error": "Kafka publish failed"},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    response_data = {
        "status": "accepted",
        "request_id": request_id,
        "count": len(events),
        "task_id": "kafka",
    }

    if idempotency_key:
        response_data["idempotency_key"] = idempotency_key

    response_data["topic"] = target_topic or "telemetry.raw"
    response_data["pipeline_mode"] = "kafka"

    return JSONResponse(response_data, status_code=status.HTTP_202_ACCEPTED)
