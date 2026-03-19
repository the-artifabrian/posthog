import os
from uuid import NAMESPACE_DNS, uuid5

import posthoganalytics

from posthog.slo.types import SloCompletedProperties, SloStartedProperties


def emit_slo_started(
    *,
    properties: SloStartedProperties,
    idempotency_key: str,
    extra_properties: dict | None = None,
) -> None:
    """Emit an slo_operation_started event with canonical + extra properties.

    Args:
        properties: Canonical SLO properties (operation, type, ID, area, team).
        idempotency_key: Seed for deterministic UUID generation (e.g., asset ID).
        extra_properties: Domain-specific properties merged into the event.
    """
    all_properties = properties.to_dict()
    all_properties["deploy_sha"] = os.environ.get("COMMIT_SHA")
    if extra_properties:
        all_properties.update(extra_properties)

    posthoganalytics.capture(
        distinct_id=str(properties.team_id),
        event="slo_operation_started",
        uuid=str(uuid5(NAMESPACE_DNS, f"slo-operation-started-{idempotency_key}")),
        properties=all_properties,
    )


def emit_slo_completed(
    *,
    properties: SloCompletedProperties,
    idempotency_key: str,
    extra_properties: dict | None = None,
) -> None:
    """Emit an slo_operation_completed event with canonical + extra properties.

    Args:
        properties: Canonical SLO properties including outcome and result quality.
        idempotency_key: Seed for deterministic UUID generation (e.g., asset ID).
        extra_properties: Domain-specific properties merged into the event.
    """
    all_properties = properties.to_dict()
    all_properties["deploy_sha"] = os.environ.get("COMMIT_SHA")
    if extra_properties:
        all_properties.update(extra_properties)

    posthoganalytics.capture(
        distinct_id=str(properties.team_id),
        event="slo_operation_completed",
        uuid=str(uuid5(NAMESPACE_DNS, f"slo-operation-completed-{idempotency_key}")),
        properties=all_properties,
    )
