import os
import json
import typing
import asyncio
import datetime as dt
import dataclasses
from uuid import NAMESPACE_DNS, uuid5

import posthoganalytics
import temporalio.common
import temporalio.activity
import temporalio.workflow
from slack_sdk.errors import SlackApiError
from structlog import get_logger

from posthog.event_usage import EventSource
from posthog.exceptions_capture import capture_exception
from posthog.models.exported_asset import ExportedAsset
from posthog.models.subscription import Subscription
from posthog.sync import database_sync_to_async
from posthog.temporal.common.base import PostHogWorkflow
from posthog.temporal.exports.activities import emit_export_outcome_events, export_asset_activity
from posthog.temporal.exports.retry_policy import EXPORT_RETRY_POLICY
from posthog.temporal.exports.types import (
    EmitExportOutcomeInput,
    ExportAssetActivityInputs,
    ExportAssetResult,
    ExportOutcomeAsset,
)

from ee.tasks.subscriptions import (
    SLACK_USER_CONFIG_ERRORS,
    SUPPORTED_TARGET_TYPES,
    _capture_delivery_failed_event,
    get_subscription_failure_metric,
    get_subscription_queued_metric,
    get_subscription_success_metric,
)
from ee.tasks.subscriptions.email_subscriptions import send_email_subscription_report
from ee.tasks.subscriptions.slack_subscriptions import (
    get_slack_integration_for_team,
    send_slack_message_with_integration_async,
)
from ee.tasks.subscriptions.subscription_utils import DEFAULT_MAX_ASSET_COUNT

LOGGER = get_logger(__name__)

# Changed 20260109-01


@dataclasses.dataclass
class FetchDueSubscriptionsActivityInputs:
    """Inputs for `fetch_due_subscriptions_activity`."""

    buffer_minutes: int = 15

    @property
    def properties_to_log(self) -> dict[str, typing.Any]:
        return {
            "buffer_minutes": self.buffer_minutes,
        }


@temporalio.activity.defn
async def fetch_due_subscriptions_activity(inputs: FetchDueSubscriptionsActivityInputs) -> list[int]:
    """Return a list of subscription IDs that are due for delivery."""
    logger = LOGGER.bind()
    await logger.ainfo("Starting subscription fetch activity")

    now_with_buffer = dt.datetime.utcnow() + dt.timedelta(minutes=inputs.buffer_minutes)
    await logger.ainfo(f"Looking for subscriptions due before {now_with_buffer}")

    @database_sync_to_async(thread_sensitive=False)
    def get_subscription_ids() -> list[int]:
        return list(
            Subscription.objects.filter(next_delivery_date__lte=now_with_buffer, deleted=False)
            .exclude(dashboard__deleted=True)
            .exclude(insight__deleted=True)
            .values_list("id", flat=True)
        )

    await logger.ainfo("Starting database query for subscriptions")
    subscription_ids = await get_subscription_ids()
    await logger.ainfo(f"Database query completed, found {len(subscription_ids)} subscriptions")

    return subscription_ids


@dataclasses.dataclass
class PrepareSubscriptionAssetsInputs:
    subscription_id: int
    max_asset_count: int = DEFAULT_MAX_ASSET_COUNT
    previous_value: typing.Optional[str] = None


@dataclasses.dataclass
class PrepareSubscriptionAssetsResult:
    subscription_id: int
    exported_asset_ids: list[int]
    total_insight_count: int
    target_type: str
    target_value: str
    team_id: int = 0
    is_new_subscription_target: bool = False
    previous_value: typing.Optional[str] = None
    invite_message: typing.Optional[str] = None


@temporalio.activity.defn
async def prepare_subscription_assets(inputs: PrepareSubscriptionAssetsInputs) -> PrepareSubscriptionAssetsResult:
    """Load subscription, determine insights, bulk-create ExportedAssets, emit slo_operation_started."""
    subscription = await database_sync_to_async(
        Subscription.objects.select_related("created_by", "insight", "dashboard", "team").get,
        thread_sensitive=False,
    )(pk=inputs.subscription_id)

    team = subscription.team
    dashboard = subscription.dashboard

    # Early exit if target value hasn't changed — avoids creating orphaned assets
    # and emitting unpaired slo_operation_started events
    if inputs.previous_value is not None and subscription.target_value == inputs.previous_value:
        return PrepareSubscriptionAssetsResult(
            subscription_id=subscription.id,
            exported_asset_ids=[],
            total_insight_count=0,
            target_type=subscription.target_type,
            target_value=subscription.target_value,
            team_id=team.id,
        )

    if dashboard:
        tiles = await database_sync_to_async(
            lambda: list(
                dashboard.tiles.select_related("insight").filter(insight__isnull=False, insight__deleted=False).all()
            ),
            thread_sensitive=False,
        )()
        tiles.sort(
            key=lambda x: (
                (x.layouts or {}).get("sm", {}).get("y", 100),
                (x.layouts or {}).get("sm", {}).get("x", 100),
            )
        )
        insights = [tile.insight for tile in tiles if tile.insight]

        selected_ids = await database_sync_to_async(
            lambda: set(subscription.dashboard_export_insights.values_list("id", flat=True))
            if subscription.dashboard_export_insights.exists()
            else None,
            thread_sensitive=False,
        )()
        if selected_ids:
            insights = [i for i in insights if i.id in selected_ids]
    elif subscription.insight:
        insights = [subscription.insight]
    else:
        raise Exception("There are no insights to be sent for this Subscription")

    expiry = ExportedAsset.compute_expires_after(ExportedAsset.ExportFormat.PNG)
    assets = [
        ExportedAsset(
            team=team,
            export_format=ExportedAsset.ExportFormat.PNG,
            insight=insight,
            dashboard=dashboard,
            expires_after=expiry,
        )
        for insight in insights[: inputs.max_asset_count]
    ]
    await database_sync_to_async(ExportedAsset.objects.bulk_create, thread_sensitive=False)(assets)

    for asset in assets:
        posthoganalytics.capture(
            distinct_id=str(team.id),
            event="slo_operation_started",
            uuid=str(uuid5(NAMESPACE_DNS, f"slo-operation-started-{asset.id}")),
            properties={
                # Canonical SLO properties
                "operation": "export",
                "operation_type": asset.export_format,
                "operation_id": str(asset.id),
                "resource_id": str(asset.insight_id) if asset.insight_id else None,
                "team_id": team.id,
                "deploy_sha": os.environ.get("COMMIT_SHA"),
                # Export-specific properties
                "exported_asset_id": asset.id,
                "source": "subscription",
            },
        )

    return PrepareSubscriptionAssetsResult(
        subscription_id=subscription.id,
        exported_asset_ids=[a.id for a in assets],
        total_insight_count=len(insights),
        target_type=subscription.target_type,
        target_value=subscription.target_value,
        team_id=team.id,
    )


@dataclasses.dataclass
class DeliverSubscriptionInputs:
    subscription_id: int
    exported_asset_ids: list[int]
    total_insight_count: int
    is_new_subscription_target: bool = False
    previous_value: typing.Optional[str] = None
    invite_message: typing.Optional[str] = None


@temporalio.activity.defn
async def deliver_subscription(inputs: DeliverSubscriptionInputs) -> None:
    """Deliver a subscription report using pre-exported assets.

    Re-raises transient delivery errors so Temporal can retry.
    Swallows user-configuration errors (Slack not_in_channel, etc.) since retrying won't help.
    """
    subscription = await database_sync_to_async(
        Subscription.objects.select_related("created_by", "insight", "dashboard", "team", "integration").get,
        thread_sensitive=False,
    )(pk=inputs.subscription_id)

    if subscription.target_type not in SUPPORTED_TARGET_TYPES:
        LOGGER.warning(
            "deliver_subscription.unsupported_target",
            subscription_id=inputs.subscription_id,
            target_type=subscription.target_type,
        )
        return

    assets = await database_sync_to_async(
        lambda: list(
            ExportedAsset.objects_including_ttl_deleted.select_related("insight").filter(
                pk__in=inputs.exported_asset_ids
            )
        ),
        thread_sensitive=False,
    )()

    if not assets:
        LOGGER.warning("deliver_subscription.no_assets", subscription_id=inputs.subscription_id)
        return

    if subscription.target_type == "email":
        get_subscription_queued_metric("email", "temporal").add(1)

        emails = subscription.target_value.split(",")
        if inputs.is_new_subscription_target:
            previous_emails = inputs.previous_value.split(",") if inputs.previous_value else []
            emails = list(set(emails) - set(previous_emails))

        last_error: Exception | None = None
        success_count = 0
        for email in emails:
            try:
                await database_sync_to_async(send_email_subscription_report, thread_sensitive=False)(
                    email,
                    subscription,
                    assets,
                    invite_message=inputs.invite_message or "" if inputs.is_new_subscription_target else None,
                    total_asset_count=inputs.total_insight_count,
                    send_async=False,
                )
                get_subscription_success_metric("email", "temporal").add(1)
                success_count += 1
            except Exception as e:
                get_subscription_failure_metric("email", "temporal").add(1)
                _capture_delivery_failed_event(subscription, e)
                LOGGER.error(
                    "deliver_subscription.email_failed",
                    subscription_id=subscription.id,
                    email=email,
                    exc_info=True,
                )
                capture_exception(e)
                last_error = e

        # Only retry if ALL recipients failed — partial success is acceptable
        # to avoid duplicate sends to already-delivered recipients
        if last_error is not None and success_count == 0:
            raise last_error

    elif subscription.target_type == "slack":
        get_subscription_queued_metric("slack", "temporal").add(1)

        try:
            integration = subscription.integration
            if integration is None or (integration and integration.kind != "slack"):
                integration = await database_sync_to_async(get_slack_integration_for_team, thread_sensitive=False)(
                    subscription.team_id
                )

            if not integration:
                LOGGER.warning(
                    "deliver_subscription.no_slack_integration",
                    subscription_id=inputs.subscription_id,
                )
                return

            delivery_result = await send_slack_message_with_integration_async(
                integration,
                subscription,
                assets,
                total_asset_count=inputs.total_insight_count,
                is_new_subscription=inputs.is_new_subscription_target,
            )

            if delivery_result.is_complete_success:
                get_subscription_success_metric("slack", "temporal").add(1)
            elif delivery_result.is_partial_failure:
                get_subscription_failure_metric("slack", "temporal", failure_type="partial").add(1)

        except SlackApiError as e:
            is_user_config_error = e.response.get("error") in SLACK_USER_CONFIG_ERRORS
            if not is_user_config_error:
                get_subscription_failure_metric("slack", "temporal", failure_type="complete").add(1)
            _capture_delivery_failed_event(subscription, e)
            LOGGER.error(
                "deliver_subscription.slack_failed",
                subscription_id=subscription.id,
                exc_info=True,
            )
            capture_exception(e)
            if not is_user_config_error:
                raise  # Transient Slack errors — let Temporal retry
        except Exception as e:
            get_subscription_failure_metric("slack", "temporal", failure_type="complete").add(1)
            _capture_delivery_failed_event(subscription, e)
            LOGGER.error(
                "deliver_subscription.slack_failed",
                subscription_id=subscription.id,
                exc_info=True,
            )
            capture_exception(e)
            raise  # Unknown errors — let Temporal retry

    # Update next delivery date (unless this is a new-subscription-target delivery)
    if not inputs.is_new_subscription_target:
        subscription.set_next_delivery_date(subscription.next_delivery_date)
        await database_sync_to_async(subscription.save, thread_sensitive=False)(update_fields=["next_delivery_date"])


@dataclasses.dataclass
class DeliverSubscriptionWorkflowInputs:
    subscription_id: int
    previous_value: typing.Optional[str] = None
    invite_message: typing.Optional[str] = None


@temporalio.workflow.defn(name="deliver-subscription")
class DeliverSubscriptionWorkflow(PostHogWorkflow):
    """Child workflow that handles a single subscription: prepare -> export -> deliver -> emit."""

    @staticmethod
    def parse_inputs(inputs: list[str]) -> DeliverSubscriptionWorkflowInputs:
        loaded = json.loads(inputs[0])
        return DeliverSubscriptionWorkflowInputs(**loaded)

    @temporalio.workflow.run
    async def run(self, inputs: DeliverSubscriptionWorkflowInputs) -> None:
        # Phase 1: Prepare — create ExportedAssets, emit slo_operation_started
        # previous_value is passed so the activity can skip asset creation if the
        # target value hasn't changed (avoids orphaned assets and unpaired SLO events)
        prepare_result = await temporalio.workflow.execute_activity(
            prepare_subscription_assets,
            PrepareSubscriptionAssetsInputs(
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

        # Phase 3: Deliver — send with whatever assets we have.
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

        # Phase 4: Emit SLO outcome events (workflow-level, guaranteed delivery)
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


@dataclasses.dataclass
class ScheduleAllSubscriptionsWorkflowInputs:
    """Inputs for the `ScheduleAllSubscriptionsWorkflow`."""

    buffer_minutes: int = 15

    @property
    def properties_to_log(self) -> dict[str, typing.Any]:
        return {
            "buffer_minutes": self.buffer_minutes,
        }


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
        """Run the workflow to schedule all subscriptions."""

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
                non_retryable_error_types=[],
            ),
        )

        # Fan-out child workflows — one per subscription, fully isolated.
        # Include the parent workflow run ID in the child ID to avoid collisions
        # when a previous schedule run's child is still executing.
        run_id = temporalio.workflow.info().run_id
        tasks = []
        for sub_id in subscription_ids:
            task = temporalio.workflow.execute_child_workflow(
                DeliverSubscriptionWorkflow.run,
                DeliverSubscriptionWorkflowInputs(subscription_id=sub_id),
                id=f"deliver-subscription-{sub_id}-{run_id}",
                parent_close_policy=temporalio.workflow.ParentClosePolicy.ABANDON,
                execution_timeout=dt.timedelta(hours=2),
            )
            tasks.append(task)

        if tasks:
            # return_exceptions=True: individual subscription failures are isolated —
            # one failing subscription should not prevent others from being delivered.
            await asyncio.gather(*tasks, return_exceptions=True)


@temporalio.workflow.defn(name="handle-subscription-value-change")
class HandleSubscriptionValueChangeWorkflow(PostHogWorkflow):
    @staticmethod
    def parse_inputs(inputs: list[str]) -> DeliverSubscriptionWorkflowInputs:
        loaded = json.loads(inputs[0])
        return DeliverSubscriptionWorkflowInputs(**loaded)

    @temporalio.workflow.run
    async def run(self, inputs: DeliverSubscriptionWorkflowInputs) -> None:
        await temporalio.workflow.execute_child_workflow(
            DeliverSubscriptionWorkflow.run,
            inputs,
            id=f"deliver-subscription-change-{inputs.subscription_id}",
            parent_close_policy=temporalio.workflow.ParentClosePolicy.ABANDON,
            execution_timeout=dt.timedelta(hours=2),
        )
