import structlog
import temporalio.activity

from posthog.event_usage import EventSource
from posthog.models.exported_asset import ExportedAsset
from posthog.slo.events import emit_slo_completed
from posthog.slo.types import ResultQuality, SloArea, SloCompletedProperties, SloOperation, SloOutcome
from posthog.sync import database_sync_to_async
from posthog.tasks import exporter
from posthog.tasks.exports.failure_handler import FAILURE_TYPE_TIMEOUT_GENERATION, FAILURE_TYPE_USER
from posthog.temporal.common.heartbeat import Heartbeater
from posthog.temporal.exports.types import EmitExportOutcomeInput, ExportAssetActivityInputs, ExportAssetResult

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
        outcome = SloOutcome.SUCCESS if asset_data.success else _classify_outcome(asset_data.failure_type)
        result_quality = ResultQuality.OK if asset_data.success else ResultQuality.ERROR
        emit_slo_completed(
            properties=SloCompletedProperties(
                operation=SloOperation.EXPORT,
                operation_type=inputs.export_format,
                operation_id=str(asset_data.exported_asset_id),
                area=SloArea.ANALYTIC_PLATFORM,
                team_id=inputs.team_id,
                outcome=outcome,
                result_quality=result_quality,
                resource_id=str(asset_data.insight_id) if asset_data.insight_id else None,
                duration_ms=asset_data.duration_ms,
            ),
            idempotency_key=str(asset_data.exported_asset_id),
            extra_properties={
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


def _classify_outcome(failure_type: str | None) -> SloOutcome:
    if failure_type == FAILURE_TYPE_USER:
        return SloOutcome.USER_ERROR
    if failure_type == FAILURE_TYPE_TIMEOUT_GENERATION:
        return SloOutcome.TIMEOUT
    return SloOutcome.SYSTEM_ERROR
