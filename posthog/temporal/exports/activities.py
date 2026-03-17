import os
from uuid import NAMESPACE_DNS, uuid5

import structlog
import posthoganalytics
import temporalio.activity

from posthog.event_usage import EventSource
from posthog.models.exported_asset import ExportedAsset
from posthog.sync import database_sync_to_async
from posthog.tasks import exporter
from posthog.tasks.exports.failure_handler import FAILURE_TYPE_TIMEOUT_GENERATION, FAILURE_TYPE_USER
from posthog.temporal.common.heartbeat import Heartbeater
from posthog.temporal.exports.types import (
    EmitExportOutcomeInput,
    ExportAssetActivityInputs,
    ExportAssetResult,
    ExportOutcome,
)

logger = structlog.get_logger(__name__)


@temporalio.activity.defn
async def export_asset_activity(inputs: ExportAssetActivityInputs) -> ExportAssetResult:
    """Export a single ExportedAsset. Retried by Temporal on transient failures."""
    async with Heartbeater():
        asset = await database_sync_to_async(
            lambda: ExportedAsset.objects_including_ttl_deleted.select_related(
                "created_by", "team", "team__organization"
            ).get(pk=inputs.exported_asset_id),
            thread_sensitive=False,
        )()

        logger.info(
            "export_asset_activity.starting",
            exported_asset_id=asset.id,
            team_id=asset.team_id,
        )

        try:
            await database_sync_to_async(exporter.export_asset_direct, thread_sensitive=False)(
                asset,
                limit=inputs.limit,
                max_height_pixels=inputs.max_height_pixels,
                source=EventSource(inputs.source) if inputs.source else None,
            )
        except Exception:
            await database_sync_to_async(asset.refresh_from_db, thread_sensitive=False)()
            logger.warning(
                "export_asset_activity.failed",
                exported_asset_id=asset.id,
                team_id=asset.team_id,
                failure_type=asset.failure_type,
            )
            raise

        await database_sync_to_async(asset.refresh_from_db, thread_sensitive=False)()

        return ExportAssetResult(
            exported_asset_id=asset.id,
            success=asset.has_content,
            failure_type=asset.failure_type,
            insight_id=asset.insight_id,
        )


@temporalio.activity.defn
async def emit_export_outcome_events(inputs: EmitExportOutcomeInput) -> None:
    """Emit slo_operation_completed events for each export asset. Workflow-level activity for guaranteed delivery."""
    for asset_data in inputs.assets:
        outcome = ExportOutcome.SUCCESS if asset_data.success else _classify_outcome(asset_data.failure_type)
        result_quality = "ok" if asset_data.success else "error"
        posthoganalytics.capture(
            distinct_id=str(inputs.team_id),
            event="slo_operation_completed",
            uuid=str(uuid5(NAMESPACE_DNS, f"slo-operation-completed-{asset_data.exported_asset_id}")),
            properties={
                # Canonical SLO properties
                "operation": "export",
                "operation_type": inputs.export_format,
                "operation_id": str(asset_data.exported_asset_id),
                "resource_id": str(asset_data.insight_id) if asset_data.insight_id else None,
                "team_id": inputs.team_id,
                "outcome": outcome,
                "result_quality": result_quality,
                "duration_ms": asset_data.duration_ms,
                "deploy_sha": os.environ.get("COMMIT_SHA"),
                # Export-specific properties
                "exported_asset_id": asset_data.exported_asset_id,
                "source": inputs.source,
                "total_attempts": asset_data.attempts,
                "failure_type": asset_data.failure_type,
            },
        )
        logger.info(
            "emit_export_outcome_events.emitted",
            exported_asset_id=asset_data.exported_asset_id,
            outcome=outcome,
            result_quality=result_quality,
        )


def _classify_outcome(failure_type: str | None) -> str:
    if failure_type == FAILURE_TYPE_USER:
        return ExportOutcome.USER_ERROR
    if failure_type == FAILURE_TYPE_TIMEOUT_GENERATION:
        return ExportOutcome.TIMEOUT
    return ExportOutcome.SYSTEM_ERROR
