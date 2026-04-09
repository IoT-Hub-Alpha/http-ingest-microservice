from typing import Any, List, Optional, Union

from pydantic import BaseModel, RootModel, model_validator

TELEMETRY_MAX_BATCH_SIZE = 1000


class TelemetryItem(BaseModel):
    timestamp: Optional[str] = None
    schema_version: str
    value: float | int
    serial_number: Optional[str] = None
    device_token: str
    model_config = {"extra": "allow"}

    @model_validator(mode="before")
    @classmethod
    def reject_payload_field(cls, data: Any):
        if isinstance(data, dict) and "payload" in data:
            raise ValueError(
                "payload field is not supported; send value at top level"
            )
        return data


class TelemetryRequest(RootModel):
    root: Union[List[TelemetryItem], TelemetryItem]

    @property
    def items(self) -> List[TelemetryItem]:
        return self.root if isinstance(self.root, list) else [self.root]

    @model_validator(mode="before")
    @classmethod
    def validate_and_mutate_batch(cls, data: Any):
        if isinstance(data, list) and len(data) == 0:
            raise ValueError("Empty batch provided")

        if isinstance(data, list) and len(data) > TELEMETRY_MAX_BATCH_SIZE:
            raise ValueError(
                "Batch size exceeds maximum limit of ",
                TELEMETRY_MAX_BATCH_SIZE,
            )

        return data

    def is_batch(self) -> bool:
        return isinstance(self.root, list)
