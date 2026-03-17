from uuid import NAMESPACE_DNS, uuid5

import structlog
import posthoganalytics
import temporalio.activity

from posthog.models.exported_asset import ExportedAsset
from posthog.sync import database_sync_to_async
from posthog.tasks import exporter
from posthog.tasks.exports.failure_handler import FAILURE_TYPE_TIMEOUT_GENERATION, FAILURE_TYPE_USER
from posthog.temporal.common.heartbeat import Heartbeater
from posthog.temporal.exports.retry_policy import EXPORT_RETRY_POLICY
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

        is_last_attempt = temporalio.activity.info().attempt >= EXPORT_RETRY_POLICY.maximum_attempts
        try:
            await database_sync_to_async(exporter.export_asset_direct, thread_sensitive=False)(
                asset,
                limit=inputs.limit,
                max_height_pixels=inputs.max_height_pixels,
                source=inputs.source,
                is_last_attempt=is_last_attempt,
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
            is_stale=asset.is_stale,
            data_last_refresh=asset.data_last_refresh.isoformat() if asset.data_last_refresh else None,
        )


@temporalio.activity.defn
async def emit_export_outcome_events(inputs: EmitExportOutcomeInput) -> None:
    """Emit slo_export_completed events for each asset. Workflow-level activity for guaranteed delivery."""
    for asset_data in inputs.assets:
        outcome = ExportOutcome.SUCCESS if asset_data.success else _classify_outcome(asset_data.failure_type)
        posthoganalytics.capture(
            distinct_id=str(inputs.team_id),
            event="slo_export_completed",
            uuid=str(uuid5(NAMESPACE_DNS, f"slo-export-completed-{asset_data.exported_asset_id}")),
            properties={
                "exported_asset_id": asset_data.exported_asset_id,
                "team_id": inputs.team_id,
                "source": inputs.source,
                "format": inputs.export_format,
                "outcome": outcome,
                "total_attempts": asset_data.attempts,
                "total_duration_ms": asset_data.duration_ms,
                "failure_type": asset_data.failure_type,
                "is_stale": asset_data.is_stale,
                "data_last_refresh": asset_data.data_last_refresh,
            },
        )
        logger.info(
            "emit_export_outcome_events.emitted",
            exported_asset_id=asset_data.exported_asset_id,
            outcome=outcome,
        )


def _classify_outcome(failure_type: str | None) -> str:
    if failure_type == FAILURE_TYPE_USER:
        return ExportOutcome.USER_ERROR
    if failure_type == FAILURE_TYPE_TIMEOUT_GENERATION:
        return ExportOutcome.TIMEOUT
    return ExportOutcome.SYSTEM_ERROR
