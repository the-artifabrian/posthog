from datetime import UTC, datetime
from typing import Any

import pytest
from posthog.test.base import APIBaseTest
from unittest.mock import mock_open, patch

from django.db import OperationalError

from boto3 import resource
from botocore.client import Config

from posthog.api.insight_variable import map_stale_to_latest
from posthog.caching.fetch_from_cache import InsightResult, NothingInCacheResult
from posthog.hogql_queries.query_runner import ExecutionMode
from posthog.models import Dashboard, ExportedAsset, Insight, InsightVariable
from posthog.models.dashboard_tile import DashboardTile
from posthog.settings import (
    OBJECT_STORAGE_ACCESS_KEY_ID,
    OBJECT_STORAGE_BUCKET,
    OBJECT_STORAGE_ENDPOINT,
    OBJECT_STORAGE_SECRET_ACCESS_KEY,
)
from posthog.storage import object_storage
from posthog.storage.object_storage import ObjectStorageError
from posthog.tasks.exports import image_exporter


def make_insight_result(cache_key: str) -> InsightResult:
    """Helper to create InsightResult with required fields for testing."""
    return InsightResult(
        result=[],
        last_refresh=datetime.now(),
        cache_key=cache_key,
        is_cached=False,
        timezone="UTC",
    )


def make_stale_insight_result(cache_key: str, last_refresh: datetime) -> InsightResult:
    return InsightResult(
        result=[{"data": "stale"}],
        last_refresh=last_refresh,
        cache_key=cache_key,
        is_cached=True,
        timezone="UTC",
    )


TEST_PREFIX = "Test-Exports"


@patch("posthog.tasks.exports.image_exporter._screenshot_asset")
@patch(
    "posthog.tasks.exports.image_exporter.open",
    new_callable=mock_open,
    read_data=b"image_data",
)
@patch("os.remove")
class TestImageExporter(APIBaseTest):
    exported_asset: ExportedAsset

    def setup_method(self, method):
        insight = Insight.objects.create(team=self.team)
        asset = ExportedAsset.objects.create(
            team=self.team,
            export_format=ExportedAsset.ExportFormat.PNG,
            insight=insight,
        )
        self.exported_asset = asset

    def teardown_method(self, method):
        s3 = resource(
            "s3",
            endpoint_url=OBJECT_STORAGE_ENDPOINT,
            aws_access_key_id=OBJECT_STORAGE_ACCESS_KEY_ID,
            aws_secret_access_key=OBJECT_STORAGE_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        bucket = s3.Bucket(OBJECT_STORAGE_BUCKET)
        bucket.objects.filter(Prefix=TEST_PREFIX).delete()

    def test_image_exporter_writes_to_asset_when_object_storage_is_disabled(self, *args) -> None:
        with self.settings(OBJECT_STORAGE_ENABLED=False):
            image_exporter.export_image(self.exported_asset)

            assert self.exported_asset.content == b"image_data"
            assert self.exported_asset.content_location is None

    @patch("posthog.models.exported_asset.UUIDT")
    def test_image_exporter_writes_to_object_storage_when_object_storage_is_enabled(self, mocked_uuidt, *args) -> None:
        mocked_uuidt.return_value = "a-guid"
        with self.settings(OBJECT_STORAGE_ENABLED=True, OBJECT_STORAGE_EXPORTS_FOLDER="Test-Exports"):
            image_exporter.export_image(self.exported_asset)

            assert (
                self.exported_asset.content_location
                == f"{TEST_PREFIX}/png/team-{self.team.id}/task-{self.exported_asset.id}/a-guid"
            )

            content = object_storage.read_bytes(self.exported_asset.content_location)
            assert content == b"image_data"

            assert self.exported_asset.content is None

    @patch("posthog.models.exported_asset.UUIDT")
    @patch("posthog.models.exported_asset.object_storage.write")
    def test_image_exporter_writes_to_object_storage_when_object_storage_write_fails(
        self, mocked_object_storage_write, mocked_uuidt, *args
    ) -> None:
        mocked_uuidt.return_value = "a-guid"
        mocked_object_storage_write.side_effect = ObjectStorageError("mock write failed")

        with self.settings(OBJECT_STORAGE_ENABLED=True, OBJECT_STORAGE_EXPORTS_FOLDER="Test-Exports"):
            image_exporter.export_image(self.exported_asset)

            assert self.exported_asset.content_location is None

            assert self.exported_asset.content == b"image_data"

    @patch("posthog.tasks.exports.image_exporter.calculate_for_query_based_insight")
    def test_dashboard_export_calculates_all_insights(self, mock_calculate: Any, *args: Any) -> None:
        mock_calculate.return_value = make_insight_result("test_cache_key")

        dashboard = Dashboard.objects.create(team=self.team, name="Test Dashboard")
        insight_count = 3

        insights = []
        for i in range(insight_count):
            insight = Insight.objects.create(
                team=self.team,
                name=f"SQL Insight {i}",
                query={
                    "kind": "DataVisualizationNode",
                    "source": {"kind": "HogQLQuery", "query": f"SELECT {i} as value"},
                },
            )
            insights.append(insight)
            DashboardTile.objects.create(dashboard=dashboard, insight=insight)

        dashboard_asset = ExportedAsset.objects.create(
            team=self.team,
            export_format=ExportedAsset.ExportFormat.PNG,
            dashboard=dashboard,
            insight=None,
        )

        with self.settings(OBJECT_STORAGE_ENABLED=False):
            image_exporter.export_image(dashboard_asset)

        assert mock_calculate.call_count == insight_count, (
            f"Expected cache warming for {insight_count} insights, got {mock_calculate.call_count} calls"
        )

        for i, call in enumerate(mock_calculate.call_args_list):
            call_kwargs = call[1]

            assert call_kwargs["dashboard"].id == dashboard.id, f"Call {i + 1} missing dashboard"

            assert call_kwargs["execution_mode"] == ExecutionMode.CALCULATE_BLOCKING_ALWAYS, (
                f"Call {i + 1} should use CALCULATE_BLOCKING_ALWAYS, got {call_kwargs['execution_mode']}"
            )

            # First positional arg is the insight
            called_insight = call[0][0]
            assert called_insight.id in [ins.id for ins in insights], (
                f"Call {i + 1} has unexpected insight {called_insight.id}"
            )

    @patch("posthog.tasks.exports.image_exporter.calculate_for_query_based_insight")
    def test_export_captures_cache_keys_and_passes_to_url(
        self,
        mock_calculate: Any,
        mock_remove: Any,
        mock_open: Any,
        mock_screenshot_asset: Any,
    ) -> None:
        """Test that cache keys from warming are captured and passed to the screenshot URL."""
        insight = Insight.objects.create(
            team=self.team,
            name="Test Insight",
            query={"kind": "DataVisualizationNode", "source": {"kind": "HogQLQuery", "query": "SELECT 1 as value"}},
        )
        exported_asset = ExportedAsset.objects.create(
            team=self.team,
            export_format=ExportedAsset.ExportFormat.PNG,
            insight=insight,
        )

        mock_calculate.return_value = make_insight_result("test_cache_key_123")

        with self.settings(OBJECT_STORAGE_ENABLED=False):
            image_exporter.export_image(exported_asset)

        # Verify _screenshot_asset was called with a URL containing cache_keys
        assert mock_screenshot_asset.called
        call_args = mock_screenshot_asset.call_args
        url_to_render = call_args[0][1]  # Second positional arg is the URL

        assert "cache_keys=" in url_to_render, f"URL should contain cache_keys parameter: {url_to_render}"
        assert "test_cache_key_123" in url_to_render, f"URL should contain the cache key: {url_to_render}"

    @patch("posthog.tasks.exports.image_exporter.calculate_for_query_based_insight")
    def test_dashboard_export_captures_all_cache_keys(
        self,
        mock_calculate: Any,
        mock_remove: Any,
        mock_open: Any,
        mock_screenshot_asset: Any,
    ) -> None:
        """Test that cache keys for all insights in a dashboard are captured and passed to URL."""
        dashboard = Dashboard.objects.create(team=self.team, name="Test Dashboard")

        insights = []
        for i in range(3):
            insight = Insight.objects.create(
                team=self.team,
                name=f"Insight {i}",
                query={"kind": "DataVisualizationNode", "source": {"kind": "HogQLQuery", "query": f"SELECT {i}"}},
            )
            insights.append(insight)
            DashboardTile.objects.create(dashboard=dashboard, insight=insight)

        dashboard_asset = ExportedAsset.objects.create(
            team=self.team,
            export_format=ExportedAsset.ExportFormat.PNG,
            dashboard=dashboard,
        )

        def mock_calc(insight: Any, **kwargs: Any) -> InsightResult:
            return make_insight_result(f"cache_key_for_insight_{insight.id}")

        mock_calculate.side_effect = mock_calc

        with self.settings(OBJECT_STORAGE_ENABLED=False):
            image_exporter.export_image(dashboard_asset)

        # Verify _screenshot_asset was called with URL containing all cache keys
        assert mock_screenshot_asset.called
        url_to_render = mock_screenshot_asset.call_args[0][1]

        # URL should contain cache_keys for all insights
        for insight in insights:
            assert f"cache_key_for_insight_{insight.id}" in url_to_render, (
                f"URL should contain cache key for insight {insight.id}"
            )

    @patch("posthog.tasks.exports.image_exporter._screenshot_asset")
    @patch("posthog.tasks.exports.image_exporter.open", new_callable=mock_open, read_data=b"image_data")
    @patch("os.remove")
    def test_export_includes_dashboard_variables(self, *args: Any) -> None:
        dashboard = Dashboard.objects.create(
            team=self.team,
            name="Test Dashboard with Variables",
            variables={"test_var": {"id": "var_123", "name": "test_var", "type": "String", "default": "value1"}},
        )

        InsightVariable.objects.create(team=self.team, name="test_var", type="String", default_value="value1")

        insight = Insight.objects.create(
            team=self.team,
            name="Test Insight",
            query={"kind": "DataVisualizationNode", "source": {"kind": "HogQLQuery", "query": "SELECT 1 as value"}},
        )
        DashboardTile.objects.create(dashboard=dashboard, insight=insight)

        exported_asset = ExportedAsset.objects.create(
            team=self.team,
            export_format=ExportedAsset.ExportFormat.PNG,
            dashboard=dashboard,
            insight=insight,
        )

        with patch("posthog.tasks.exports.image_exporter.calculate_for_query_based_insight") as mock_calculate:
            mock_calculate.return_value = make_insight_result("test_key")

            with self.settings(OBJECT_STORAGE_ENABLED=False):
                image_exporter.export_image(exported_asset)

            assert mock_calculate.call_count == 1
            call_kwargs = mock_calculate.call_args[1]

            assert "variables_override" in call_kwargs, "variables_override parameter missing"
            assert call_kwargs["variables_override"] is not None, (
                "variables_override should not be None when dashboard has variables"
            )

            variables = list(InsightVariable.objects.filter(team=self.team).all())
            expected_variables = map_stale_to_latest(dashboard.variables or {}, variables)
            assert call_kwargs["variables_override"] == expected_variables, (
                "variables_override should match the transformed dashboard variables"
            )

    @patch("posthog.tasks.exports.image_exporter._screenshot_asset")
    @patch("posthog.tasks.exports.image_exporter.open", new_callable=mock_open, read_data=b"image_data")
    @patch("os.remove")
    def test_export_includes_tile_filter_overrides(self, *args: Any) -> None:
        dashboard = Dashboard.objects.create(team=self.team, name="Dashboard with Tile Filters")
        insight = Insight.objects.create(
            team=self.team,
            name="Test Insight",
            query={"kind": "DataVisualizationNode", "source": {"kind": "HogQLQuery", "query": "SELECT 1"}},
        )
        tile_filters = {"date_from": "-7d", "properties": [{"key": "$browser", "value": "Chrome"}]}
        DashboardTile.objects.create(dashboard=dashboard, insight=insight, filters_overrides=tile_filters)

        exported_asset = ExportedAsset.objects.create(
            team=self.team,
            export_format=ExportedAsset.ExportFormat.PNG,
            dashboard=dashboard,
            insight=insight,
        )

        with patch("posthog.tasks.exports.image_exporter.calculate_for_query_based_insight") as mock_calculate:
            mock_calculate.return_value = make_insight_result("test_key")

            with self.settings(OBJECT_STORAGE_ENABLED=False):
                image_exporter.export_image(exported_asset)

            assert mock_calculate.call_count == 1
            call_kwargs = mock_calculate.call_args[1]

            assert "tile_filters_override" in call_kwargs, "tile_filters_override parameter missing"
            assert call_kwargs["tile_filters_override"] == tile_filters, (
                "tile_filters_override should match tile filters"
            )


class TestExportedAssetStalenessFields(APIBaseTest):
    def test_staleness_fields_default_values(self):
        insight = Insight.objects.create(team=self.team)
        asset = ExportedAsset.objects.create(
            team=self.team,
            export_format=ExportedAsset.ExportFormat.PNG,
            insight=insight,
        )
        assert asset.is_stale is False
        assert asset.data_last_refresh is None

    def test_staleness_fields_can_be_set(self):
        from django.utils.timezone import now as tz_now

        insight = Insight.objects.create(team=self.team)
        refresh_time = tz_now()
        asset = ExportedAsset.objects.create(
            team=self.team,
            export_format=ExportedAsset.ExportFormat.PNG,
            insight=insight,
            is_stale=True,
            data_last_refresh=refresh_time,
        )
        asset.refresh_from_db()
        assert asset.is_stale is True
        assert asset.data_last_refresh == refresh_time


@patch("posthog.tasks.exports.image_exporter._screenshot_asset")
@patch(
    "posthog.tasks.exports.image_exporter.open",
    new_callable=mock_open,
    read_data=b"image_data",
)
@patch("os.remove")
class TestImageExporterCacheFallback(APIBaseTest):
    def setup_method(self, method):
        self.insight = Insight.objects.create(
            team=self.team,
            query={"kind": "DataVisualizationNode", "source": {"kind": "HogQLQuery", "query": "SELECT 1"}},
        )
        self.exported_asset = ExportedAsset.objects.create(
            team=self.team,
            export_format=ExportedAsset.ExportFormat.PNG,
            insight=self.insight,
        )

    @patch("posthog.tasks.exports.image_exporter.calculate_for_query_based_insight")
    def test_fallback_serves_stale_cache_on_last_attempt(self, mock_calculate, *args):
        stale_time = datetime(2026, 3, 15, 14, 30, 0, tzinfo=UTC)
        mock_calculate.side_effect = [
            OperationalError("CH down"),
            make_stale_insight_result("stale_key_123", stale_time),
        ]

        image_exporter.export_image(self.exported_asset, is_last_attempt=True)

        self.exported_asset.refresh_from_db()
        assert self.exported_asset.has_content
        assert self.exported_asset.is_stale is True
        assert self.exported_asset.data_last_refresh == stale_time

        assert mock_calculate.call_count == 2
        assert mock_calculate.call_args_list[1][1]["execution_mode"] == ExecutionMode.CACHE_ONLY_NEVER_CALCULATE

    @patch("posthog.tasks.exports.image_exporter.calculate_for_query_based_insight")
    def test_fallback_raises_when_no_cache_available(self, mock_calculate, *args):
        mock_calculate.side_effect = [
            OperationalError("CH down"),
            NothingInCacheResult(),
        ]

        with pytest.raises(OperationalError, match="CH down"):
            image_exporter.export_image(self.exported_asset, is_last_attempt=True)

    @patch("posthog.tasks.exports.image_exporter.calculate_for_query_based_insight")
    def test_no_fallback_when_not_last_attempt(self, mock_calculate, *args):
        mock_calculate.side_effect = OperationalError("CH down")

        with pytest.raises(OperationalError, match="CH down"):
            image_exporter.export_image(self.exported_asset, is_last_attempt=False)

        assert mock_calculate.call_count == 1

    @patch("posthog.tasks.exports.image_exporter.calculate_for_query_based_insight")
    def test_fallback_raises_original_when_cache_read_fails(self, mock_calculate, *args):
        mock_calculate.side_effect = [
            OperationalError("CH down"),
            Exception("Redis connection refused"),
        ]

        with pytest.raises(OperationalError, match="CH down"):
            image_exporter.export_image(self.exported_asset, is_last_attempt=True)

    @patch("posthog.tasks.exports.image_exporter.calculate_for_query_based_insight")
    def test_dashboard_fallback_per_tile(self, mock_calculate, *args):
        dashboard = Dashboard.objects.create(team=self.team, name="Test Dashboard")
        insights = []
        for i in range(3):
            insight = Insight.objects.create(
                team=self.team,
                name=f"Insight {i}",
                query={"kind": "DataVisualizationNode", "source": {"kind": "HogQLQuery", "query": f"SELECT {i}"}},
            )
            insights.append(insight)
            DashboardTile.objects.create(dashboard=dashboard, insight=insight)

        dashboard_asset = ExportedAsset.objects.create(
            team=self.team,
            export_format=ExportedAsset.ExportFormat.PNG,
            dashboard=dashboard,
        )

        stale_time_1 = datetime(2026, 3, 14, 10, 0, 0, tzinfo=UTC)
        stale_time_2 = datetime(2026, 3, 15, 14, 0, 0, tzinfo=UTC)

        # Tile 0: fresh succeeds, Tile 1: fails then stale, Tile 2: fails then stale
        mock_calculate.side_effect = [
            make_insight_result("fresh_key_0"),
            OperationalError("CH down"),
            make_stale_insight_result("stale_1", stale_time_1),
            OperationalError("CH down"),
            make_stale_insight_result("stale_2", stale_time_2),
        ]

        image_exporter.export_image(dashboard_asset, is_last_attempt=True)

        dashboard_asset.refresh_from_db()
        assert dashboard_asset.is_stale is True
        assert dashboard_asset.data_last_refresh == stale_time_1  # min() of stale timestamps
