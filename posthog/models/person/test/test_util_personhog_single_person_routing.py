from types import SimpleNamespace

from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from parameterized import parameterized

from posthog.models.person.util import (
    _fetch_person_by_distinct_id_via_personhog,
    _fetch_person_by_uuid_via_personhog,
    _validate_uuids_via_personhog,
    get_person_by_distinct_id,
    get_person_by_uuid,
    validate_person_uuids_exist,
)


class TestGetPersonByUuidRouting(SimpleTestCase):
    @parameterized.expand(
        [
            (
                "personhog_success",
                True,
                "mock_person",
                None,
                "personhog",
            ),
            (
                "personhog_returns_none",
                True,
                None,
                None,
                "personhog",
            ),
            (
                "personhog_failure_falls_back_to_orm",
                True,
                None,
                RuntimeError("grpc timeout"),
                "django_orm",
            ),
            (
                "gate_off_uses_orm_directly",
                False,
                None,
                None,
                "django_orm",
            ),
        ]
    )
    @patch("posthog.models.person.util.Person.objects")
    @patch("posthog.models.person.util._fetch_person_by_uuid_via_personhog")
    @patch("posthog.personhog_client.gate.use_personhog")
    @patch("posthog.models.person.util.PERSONHOG_ROUTING_TOTAL")
    @patch("posthog.models.person.util.PERSONHOG_ROUTING_ERRORS_TOTAL")
    def test_routing(
        self,
        _name,
        gate_on,
        personhog_data,
        grpc_exception,
        expected_source,
        mock_errors_counter,
        mock_routing_counter,
        mock_use_personhog,
        mock_fetch_personhog,
        mock_objects,
    ):
        mock_use_personhog.return_value = gate_on

        if grpc_exception is not None:
            mock_fetch_personhog.side_effect = grpc_exception
        else:
            mock_fetch_personhog.return_value = personhog_data

        mock_qs = MagicMock()
        mock_qs.first.return_value = MagicMock()
        mock_objects.db_manager.return_value.filter.return_value = mock_qs

        result = get_person_by_uuid(1, "some-uuid")

        if gate_on and grpc_exception is None:
            assert result == personhog_data
            mock_objects.db_manager.assert_not_called()
        else:
            mock_objects.db_manager.assert_called()

        mock_routing_counter.labels.assert_called_with(
            operation="get_person_by_uuid", source=expected_source, client_name="posthog-django"
        )

        if grpc_exception is not None and gate_on:
            mock_errors_counter.labels.assert_called_once()


class TestGetPersonByDistinctIdRouting(SimpleTestCase):
    @parameterized.expand(
        [
            (
                "personhog_success",
                True,
                "mock_person",
                None,
                "personhog",
            ),
            (
                "personhog_returns_none",
                True,
                None,
                None,
                "personhog",
            ),
            (
                "personhog_failure_falls_back_to_orm",
                True,
                None,
                RuntimeError("grpc timeout"),
                "django_orm",
            ),
            (
                "gate_off_uses_orm_directly",
                False,
                None,
                None,
                "django_orm",
            ),
        ]
    )
    @patch("posthog.models.person.util.Person.objects")
    @patch("posthog.models.person.util._fetch_person_by_distinct_id_via_personhog")
    @patch("posthog.personhog_client.gate.use_personhog")
    @patch("posthog.models.person.util.PERSONHOG_ROUTING_TOTAL")
    @patch("posthog.models.person.util.PERSONHOG_ROUTING_ERRORS_TOTAL")
    def test_routing(
        self,
        _name,
        gate_on,
        personhog_data,
        grpc_exception,
        expected_source,
        mock_errors_counter,
        mock_routing_counter,
        mock_use_personhog,
        mock_fetch_personhog,
        mock_objects,
    ):
        mock_use_personhog.return_value = gate_on

        if grpc_exception is not None:
            mock_fetch_personhog.side_effect = grpc_exception
        else:
            mock_fetch_personhog.return_value = personhog_data

        mock_qs = MagicMock()
        mock_qs.first.return_value = MagicMock()
        mock_objects.db_manager.return_value.filter.return_value = mock_qs

        result = get_person_by_distinct_id(1, "some-distinct-id")

        if gate_on and grpc_exception is None:
            assert result == personhog_data
            mock_objects.db_manager.assert_not_called()
        else:
            mock_objects.db_manager.assert_called()

        mock_routing_counter.labels.assert_called_with(
            operation="get_person_by_distinct_id", source=expected_source, client_name="posthog-django"
        )

        if grpc_exception is not None and gate_on:
            mock_errors_counter.labels.assert_called_once()


class TestValidatePersonUuidsExistRouting(SimpleTestCase):
    @parameterized.expand(
        [
            (
                "personhog_success",
                True,
                ["uuid-1", "uuid-2"],
                None,
                "personhog",
            ),
            (
                "personhog_failure_falls_back_to_orm",
                True,
                None,
                RuntimeError("grpc timeout"),
                "django_orm",
            ),
            (
                "gate_off_uses_orm_directly",
                False,
                None,
                None,
                "django_orm",
            ),
        ]
    )
    @patch("posthog.models.person.util.Person.objects")
    @patch("posthog.models.person.util._validate_uuids_via_personhog")
    @patch("posthog.personhog_client.gate.use_personhog")
    @patch("posthog.models.person.util.PERSONHOG_ROUTING_TOTAL")
    @patch("posthog.models.person.util.PERSONHOG_ROUTING_ERRORS_TOTAL")
    def test_routing(
        self,
        _name,
        gate_on,
        personhog_data,
        grpc_exception,
        expected_source,
        mock_errors_counter,
        mock_routing_counter,
        mock_use_personhog,
        mock_fetch_personhog,
        mock_objects,
    ):
        mock_use_personhog.return_value = gate_on

        if grpc_exception is not None:
            mock_fetch_personhog.side_effect = grpc_exception
        else:
            mock_fetch_personhog.return_value = personhog_data

        mock_qs = MagicMock()
        mock_qs.__iter__ = MagicMock(return_value=iter([]))
        mock_objects.db_manager.return_value.filter.return_value.values_list.return_value = mock_qs

        result = validate_person_uuids_exist(1, ["uuid-1", "uuid-2"])

        if gate_on and grpc_exception is None:
            assert result == personhog_data
            mock_objects.db_manager.assert_not_called()
        else:
            mock_objects.db_manager.assert_called()

        mock_routing_counter.labels.assert_called_with(
            operation="validate_person_uuids_exist", source=expected_source, client_name="posthog-django"
        )

        if grpc_exception is not None and gate_on:
            mock_errors_counter.labels.assert_called_once()


class TestFetchPersonByUuidFiltering(SimpleTestCase):
    @patch("posthog.personhog_client.client.get_personhog_client")
    def test_returns_person_with_distinct_ids(self, mock_get_client):
        person = SimpleNamespace(
            id=42,
            uuid="550e8400-e29b-41d4-a716-446655440000",
            team_id=1,
            properties=b'{"email": "test@example.com"}',
            is_identified=True,
            created_at=1700000000000,
            last_seen_at=0,
        )
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.get_person_by_uuid.return_value = SimpleNamespace(person=person)
        mock_client.get_distinct_ids_for_person.return_value = SimpleNamespace(
            distinct_ids=[SimpleNamespace(distinct_id="did-1"), SimpleNamespace(distinct_id="did-2")]
        )

        result = _fetch_person_by_uuid_via_personhog(team_id=1, uuid="550e8400-e29b-41d4-a716-446655440000")

        assert result is not None
        assert result.id == 42
        assert result.properties == {"email": "test@example.com"}
        assert result.distinct_ids == ["did-1", "did-2"]

    @patch("posthog.personhog_client.client.get_personhog_client")
    def test_returns_none_when_person_not_found(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.get_person_by_uuid.return_value = SimpleNamespace(person=None)

        result = _fetch_person_by_uuid_via_personhog(team_id=1, uuid="nonexistent")

        assert result is None
        mock_client.get_distinct_ids_for_person.assert_not_called()

    @patch("posthog.personhog_client.client.get_personhog_client")
    def test_returns_none_when_empty_person_id(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.get_person_by_uuid.return_value = SimpleNamespace(person=SimpleNamespace(id=0, team_id=1))

        result = _fetch_person_by_uuid_via_personhog(team_id=1, uuid="some-uuid")

        assert result is None
        mock_client.get_distinct_ids_for_person.assert_not_called()

    @patch("posthog.models.person.util.PERSONHOG_TEAM_MISMATCH_TOTAL")
    @patch("posthog.personhog_client.client.get_personhog_client")
    def test_returns_none_on_team_mismatch(self, mock_get_client, mock_mismatch_counter):
        person = SimpleNamespace(
            id=42,
            uuid="550e8400-e29b-41d4-a716-446655440000",
            team_id=999,
            properties=b"{}",
            is_identified=False,
            created_at=0,
            last_seen_at=0,
        )
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.get_person_by_uuid.return_value = SimpleNamespace(person=person)

        result = _fetch_person_by_uuid_via_personhog(team_id=1, uuid="550e8400-e29b-41d4-a716-446655440000")

        assert result is None
        mock_mismatch_counter.labels.assert_called_once()
        mock_client.get_distinct_ids_for_person.assert_not_called()


class TestFetchPersonByDistinctIdFiltering(SimpleTestCase):
    @patch("posthog.personhog_client.client.get_personhog_client")
    def test_returns_person_with_distinct_ids(self, mock_get_client):
        person = SimpleNamespace(
            id=42,
            uuid="550e8400-e29b-41d4-a716-446655440000",
            team_id=1,
            properties=b'{"email": "test@example.com"}',
            is_identified=True,
            created_at=1700000000000,
            last_seen_at=0,
        )
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.get_person_by_distinct_id.return_value = SimpleNamespace(person=person)
        mock_client.get_distinct_ids_for_person.return_value = SimpleNamespace(
            distinct_ids=[SimpleNamespace(distinct_id="did-1"), SimpleNamespace(distinct_id="did-2")]
        )

        result = _fetch_person_by_distinct_id_via_personhog(team_id=1, distinct_id="did-1")

        assert result is not None
        assert result.id == 42
        assert result.distinct_ids == ["did-1", "did-2"]

    @patch("posthog.personhog_client.client.get_personhog_client")
    def test_returns_none_when_person_not_found(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.get_person_by_distinct_id.return_value = SimpleNamespace(person=None)

        result = _fetch_person_by_distinct_id_via_personhog(team_id=1, distinct_id="nonexistent")

        assert result is None
        mock_client.get_distinct_ids_for_person.assert_not_called()

    @patch("posthog.models.person.util.PERSONHOG_TEAM_MISMATCH_TOTAL")
    @patch("posthog.personhog_client.client.get_personhog_client")
    def test_returns_none_on_team_mismatch(self, mock_get_client, mock_mismatch_counter):
        person = SimpleNamespace(
            id=42,
            uuid="550e8400-e29b-41d4-a716-446655440000",
            team_id=999,
            properties=b"{}",
            is_identified=False,
            created_at=0,
            last_seen_at=0,
        )
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.get_person_by_distinct_id.return_value = SimpleNamespace(person=person)

        result = _fetch_person_by_distinct_id_via_personhog(team_id=1, distinct_id="some-did")

        assert result is None
        mock_mismatch_counter.labels.assert_called_once()


class TestValidateUuidsViaPersonhog(SimpleTestCase):
    @patch("posthog.personhog_client.client.get_personhog_client")
    def test_returns_matching_uuids(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.get_persons_by_uuids.return_value = SimpleNamespace(
            persons=[
                SimpleNamespace(uuid="uuid-1", team_id=1),
                SimpleNamespace(uuid="uuid-2", team_id=1),
            ]
        )

        result = _validate_uuids_via_personhog(team_id=1, uuids=["uuid-1", "uuid-2", "uuid-missing"])

        assert result == ["uuid-1", "uuid-2"]

    @patch("posthog.personhog_client.client.get_personhog_client")
    def test_filters_out_wrong_team(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.get_persons_by_uuids.return_value = SimpleNamespace(
            persons=[
                SimpleNamespace(uuid="uuid-1", team_id=1),
                SimpleNamespace(uuid="uuid-2", team_id=999),
            ]
        )

        result = _validate_uuids_via_personhog(team_id=1, uuids=["uuid-1", "uuid-2"])

        assert result == ["uuid-1"]

    @patch("posthog.personhog_client.client.get_personhog_client")
    def test_raises_when_client_not_configured(self, mock_get_client):
        mock_get_client.return_value = None

        with self.assertRaises(RuntimeError, msg="personhog client not configured"):
            _validate_uuids_via_personhog(team_id=1, uuids=["uuid-1"])
