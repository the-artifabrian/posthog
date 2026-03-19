import dataclasses
from enum import StrEnum
from typing import Optional


class SloArea(StrEnum):
    ANALYTIC_PLATFORM = "analytic-platform"


class SloOperation(StrEnum):
    EXPORT = "export"
    SUBSCRIPTION_DELIVERY = "subscription_delivery"


class SloOutcome(StrEnum):
    SUCCESS = "success"
    SYSTEM_ERROR = "system_error"
    TIMEOUT = "timeout"
    USER_ERROR = "user_error"


class ResultQuality(StrEnum):
    OK = "ok"
    EMPTY = "empty"
    DEGRADED = "degraded"
    ERROR = "error"
    STALE = "stale"


@dataclasses.dataclass
class SloStartedProperties:
    """Canonical properties for slo_operation_started events."""

    operation: SloOperation
    operation_type: str
    operation_id: str
    area: SloArea
    team_id: int
    resource_id: Optional[str] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in dataclasses.asdict(self).items() if v is not None}


@dataclasses.dataclass
class SloCompletedProperties:
    """Canonical properties for slo_operation_completed events."""

    operation: SloOperation
    operation_type: str
    operation_id: str
    area: SloArea
    team_id: int
    outcome: SloOutcome
    result_quality: ResultQuality
    resource_id: Optional[str] = None
    duration_ms: Optional[float] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in dataclasses.asdict(self).items() if v is not None}
