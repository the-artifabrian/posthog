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
        # Include the parent workflow run ID in the child ID to avoid collisions
        # when a previous schedule run's child is still executing.
        run_id = temporalio.workflow.info().run_id
        tasks = []
        for sub_id in subscription_ids:
            task = temporalio.workflow.execute_child_workflow(
                ProcessSubscriptionWorkflow.run,
                ProcessSubscriptionWorkflowInputs(subscription_id=sub_id),
                id=f"process-subscription-{sub_id}-{run_id}",
                parent_close_policy=temporalio.workflow.ParentClosePolicy.ABANDON,
                execution_timeout=dt.timedelta(hours=2),
            )
            tasks.append(task)

        if tasks:
            # return_exceptions=True: individual subscription failures are isolated —
            # one failing subscription should not prevent others from being delivered.
            await asyncio.gather(*tasks, return_exceptions=True)


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
        outcome_assets = []
        successful_asset_ids = []
        for (asset_id, _), result in zip(export_tasks, export_results):
            if isinstance(result, BaseException):
                outcome_assets.append(ExportOutcomeAsset(exported_asset_id=asset_id, success=False))
            else:
                outcome_assets.append(
                    ExportOutcomeAsset(
                        exported_asset_id=result.exported_asset_id,
                        success=result.success,
                        failure_type=result.failure_type,
                        insight_id=result.insight_id,
                    )
                )
                if result.success:
                    successful_asset_ids.append(result.exported_asset_id)

        # Phase 3: Emit SLO outcome events — close the export SLO before delivery
        # so the started→completed duration measures only the export, not delivery
        await temporalio.workflow.execute_activity(
            emit_export_outcome_events,
            EmitExportOutcomeInput(
                team_id=prepare_result.team_id,
                source="subscription",
                export_format="image/png",
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

        # API passes previous_value="" for new subscriptions and the actual
        # old value for updates — treat both None and "" as "not a target change"
        is_new = bool(inputs.previous_value)

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
