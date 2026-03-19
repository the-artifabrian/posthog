import json
import asyncio
import datetime as dt

import temporalio.common
import temporalio.workflow

from posthog.event_usage import EventSource
from posthog.temporal.common.base import PostHogWorkflow
from posthog.temporal.exports.activities import emit_export_outcome_events, export_asset_activity
from posthog.temporal.exports.retry_policy import EXPORT_RETRY_POLICY
from posthog.temporal.exports.types import (
    EmitExportOutcomeInput,
    ExportAssetActivityInputs,
    ExportAssetResult,
    ExportOutcomeAsset,
)
from posthog.temporal.subscriptions.activities import (
    create_export_assets,
    deliver_subscription,
    fetch_due_subscriptions_activity,
)
from posthog.temporal.subscriptions.types import (
    CreateExportAssetsInputs,
    DeliverSubscriptionInputs,
    FetchDueSubscriptionsActivityInputs,
    ProcessSubscriptionWorkflowInputs,
    ScheduleAllSubscriptionsWorkflowInputs,
)


def _extract_error_details(exc: BaseException) -> tuple[str | None, float | None, str, int]:
    """Extract failure metadata from a Temporal activity exception chain.

    export_asset_activity wraps failures in ApplicationError with details:
    [failure_type, duration_ms, export_format, attempt]. This lets us classify
    SLO outcomes without a database round-trip.
    """
    cause = getattr(exc, "cause", None)
    if cause is not None:
        try:
            details = cause.details
            failure_type = details[0] if len(details) >= 1 and isinstance(details[0], str) else None
            duration_ms = details[1] if len(details) >= 2 and isinstance(details[1], (int, float)) else None
            export_format = details[2] if len(details) >= 3 and isinstance(details[2], str) else ""
            attempts = details[3] if len(details) >= 4 and isinstance(details[3], int) else 1
            return failure_type, duration_ms, export_format, attempts
        except Exception:
            pass
    return None, None, "", 1


def _build_outcome_assets(
    asset_ids: list[int],
    export_results: list[ExportAssetResult | BaseException],
) -> tuple[list[ExportOutcomeAsset], list[int]]:
    """Classify export results into outcome assets and collect successful asset IDs.

    BaseException objects from asyncio.gather(return_exceptions=True) aren't
    serializable across the Temporal activity boundary, so this classification
    must happen in the workflow, not in an activity.
    """
    outcome_assets: list[ExportOutcomeAsset] = []
    successful_asset_ids: list[int] = []
    for asset_id, result in zip(asset_ids, export_results):
        if isinstance(result, BaseException):
            failure_type, duration_ms, export_format, attempts = _extract_error_details(result)
            outcome_assets.append(
                ExportOutcomeAsset(
                    exported_asset_id=asset_id,
                    success=False,
                    failure_type=failure_type,
                    duration_ms=duration_ms,
                    export_format=export_format,
                    attempts=attempts,
                )
            )
        else:
            outcome_assets.append(
                ExportOutcomeAsset(
                    exported_asset_id=result.exported_asset_id,
                    success=result.success,
                    failure_type=result.failure_type,
                    insight_id=result.insight_id,
                    duration_ms=result.duration_ms,
                    export_format=result.export_format,
                    attempts=result.attempts,
                )
            )
            if result.success:
                successful_asset_ids.append(result.exported_asset_id)
    return outcome_assets, successful_asset_ids


@temporalio.workflow.defn(name="schedule-all-subscriptions")
class ScheduleAllSubscriptionsWorkflow(PostHogWorkflow):
    """Workflow to schedule all subscriptions that are due for delivery."""

    @staticmethod
    def parse_inputs(inputs: list[str]) -> ScheduleAllSubscriptionsWorkflowInputs:
        if not inputs:
            return ScheduleAllSubscriptionsWorkflowInputs()

        loaded = json.loads(inputs[0])
        return ScheduleAllSubscriptionsWorkflowInputs(**loaded)

    @temporalio.workflow.run
    async def run(self, inputs: ScheduleAllSubscriptionsWorkflowInputs) -> None:
        # Fetch subscription IDs that are due
        fetch_inputs = FetchDueSubscriptionsActivityInputs(buffer_minutes=inputs.buffer_minutes)
        subscription_ids: list[int] = await temporalio.workflow.execute_activity(
            fetch_due_subscriptions_activity,
            fetch_inputs,
            start_to_close_timeout=dt.timedelta(minutes=5),
            retry_policy=temporalio.common.RetryPolicy(
                initial_interval=dt.timedelta(seconds=10),
                maximum_interval=dt.timedelta(minutes=5),
                maximum_attempts=3,
            ),
        )

        # Fan-out child workflows — one per subscription, fully isolated.
        # Idempotent ID (no run_id suffix) prevents duplicate deliveries when
        # schedule runs overlap: if the previous run's child is still executing,
        # Temporal rejects the duplicate start and we log it below.
        tasks = []
        for sub_id in subscription_ids:
            task = temporalio.workflow.execute_child_workflow(
                ProcessSubscriptionWorkflow.run,
                ProcessSubscriptionWorkflowInputs(subscription_id=sub_id),
                id=f"process-subscription-{sub_id}",
                parent_close_policy=temporalio.workflow.ParentClosePolicy.ABANDON,
                execution_timeout=dt.timedelta(hours=2),
            )
            tasks.append(task)

        if tasks:
            # return_exceptions=True: individual subscription failures are isolated —
            # one failing subscription should not prevent others from being delivered.
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for sub_id, result in zip(subscription_ids, results):
                if isinstance(result, BaseException):
                    temporalio.workflow.logger.warning(
                        "process_subscription.child_workflow_error",
                        extra={"subscription_id": sub_id, "error": str(result)},
                    )


@temporalio.workflow.defn(name="process-subscription")
class ProcessSubscriptionWorkflow(PostHogWorkflow):
    """Child workflow that handles a single subscription: prepare -> export -> emit -> deliver."""

    @staticmethod
    def parse_inputs(inputs: list[str]) -> ProcessSubscriptionWorkflowInputs:
        loaded = json.loads(inputs[0])
        return ProcessSubscriptionWorkflowInputs(**loaded)

    @temporalio.workflow.run
    async def run(self, inputs: ProcessSubscriptionWorkflowInputs) -> None:
        # Phase 1: Prepare — create ExportedAssets, emit slo_operation_started
        # previous_value is passed so the activity can skip asset creation if the
        # target value hasn't changed (avoids orphaned assets and unpaired SLO events)
        prepare_result = await temporalio.workflow.execute_activity(
            create_export_assets,
            CreateExportAssetsInputs(
                subscription_id=inputs.subscription_id,
                previous_value=inputs.previous_value,
            ),
            start_to_close_timeout=dt.timedelta(minutes=5),
            retry_policy=temporalio.common.RetryPolicy(
                initial_interval=dt.timedelta(seconds=10),
                maximum_interval=dt.timedelta(minutes=2),
                maximum_attempts=3,
            ),
        )

        if not prepare_result.exported_asset_ids:
            return

        # Phase 2: Fan-out export — one activity per insight, independent retry
        export_tasks = []
        for asset_id in prepare_result.exported_asset_ids:
            task = temporalio.workflow.execute_activity(
                export_asset_activity,
                ExportAssetActivityInputs(
                    exported_asset_id=asset_id,
                    source=EventSource.SUBSCRIPTION,
                ),
                start_to_close_timeout=dt.timedelta(hours=1),
                heartbeat_timeout=dt.timedelta(minutes=2),
                retry_policy=EXPORT_RETRY_POLICY,
            )
            export_tasks.append((asset_id, task))

        # Gather results — continue on failure (partial success OK)
        export_results: list[ExportAssetResult | BaseException] = await asyncio.gather(
            *[task for _, task in export_tasks],
            return_exceptions=True,
        )

        # Build outcome data for SLO events
        asset_ids = [aid for aid, _ in export_tasks]
        outcome_assets, successful_asset_ids = _build_outcome_assets(asset_ids, export_results)

        # Phase 3: Emit SLO outcome events — close the export SLO before delivery
        # so the started->completed duration measures only the export, not delivery
        await temporalio.workflow.execute_activity(
            emit_export_outcome_events,
            EmitExportOutcomeInput(
                team_id=prepare_result.team_id,
                source="subscription",
                assets=outcome_assets,
            ),
            start_to_close_timeout=dt.timedelta(minutes=2),
            retry_policy=temporalio.common.RetryPolicy(
                initial_interval=dt.timedelta(seconds=5),
                maximum_interval=dt.timedelta(minutes=1),
                maximum_attempts=3,
            ),
        )

        # Phase 4: Deliver — send with whatever assets we have
        delivery_asset_ids = successful_asset_ids if successful_asset_ids else prepare_result.exported_asset_ids

        # is_new is true when previous_value is set (target change), false for scheduled delivery
        is_new = inputs.previous_value is not None

        await temporalio.workflow.execute_activity(
            deliver_subscription,
            DeliverSubscriptionInputs(
                subscription_id=inputs.subscription_id,
                exported_asset_ids=delivery_asset_ids,
                total_insight_count=prepare_result.total_insight_count,
                is_new_subscription_target=is_new,
                previous_value=inputs.previous_value,
                invite_message=inputs.invite_message,
            ),
            start_to_close_timeout=dt.timedelta(minutes=5),
            retry_policy=temporalio.common.RetryPolicy(
                initial_interval=dt.timedelta(seconds=10),
                maximum_interval=dt.timedelta(minutes=2),
                maximum_attempts=3,
            ),
        )


@temporalio.workflow.defn(name="handle-subscription-value-change")
class HandleSubscriptionValueChangeWorkflow(PostHogWorkflow):
    @staticmethod
    def parse_inputs(inputs: list[str]) -> ProcessSubscriptionWorkflowInputs:
        loaded = json.loads(inputs[0])
        return ProcessSubscriptionWorkflowInputs(**loaded)

    @temporalio.workflow.run
    async def run(self, inputs: ProcessSubscriptionWorkflowInputs) -> None:
        await temporalio.workflow.execute_child_workflow(
            ProcessSubscriptionWorkflow.run,
            inputs,
            id=f"process-subscription-change-{inputs.subscription_id}",
            parent_close_policy=temporalio.workflow.ParentClosePolicy.ABANDON,
            execution_timeout=dt.timedelta(hours=2),
        )
