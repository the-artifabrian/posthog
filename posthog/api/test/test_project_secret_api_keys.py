from posthog.test.base import APIBaseTest

from posthog.api.project_secret_api_key import MAX_PROJECT_SECRET_API_KEYS_PER_TEAM
from posthog.models import Organization, OrganizationMembership, Team
from posthog.models.project_secret_api_key import ProjectSecretAPIKey


class TestProjectSecretAPIKeysAPI(APIBaseTest):
    def setUp(self):
        super().setUp()
        self.organization_membership.level = OrganizationMembership.Level.ADMIN
        self.organization_membership.save()

    def test_create_project_secret_api_key(self):
        label = "the key to rule them all"
        response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys", {"label": label, "scopes": ["endpoint:read"]}
        )
        assert response.status_code == 201
        data = response.json()

        key = ProjectSecretAPIKey.objects.get(id=data["id"])
        assert data["id"] == key.id
        assert data["label"] == label
        assert data["scopes"] == ["endpoint:read"]
        assert data["last_rolled_at"] is None
        assert data["last_used_at"] is None
        assert data["value"].startswith("phs_")

    def test_create_too_many_api_keys(self):
        for i in range(0, MAX_PROJECT_SECRET_API_KEYS_PER_TEAM):
            self.client.post(
                f"/api/projects/{self.team.id}/project_secret_api_keys",
                {"label": i, "scopes": ["endpoint:read"]},
            )
        response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "not the one", "scopes": ["endpoint:read"]},
        )
        assert response.status_code == 400
        self.assertIn("You can only have", response.json()["detail"])

    def test_label_required(self):
        response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "", "scopes": ["endpoint:read"]},
        )
        assert response.status_code == 400

    def test_scopes_required(self):
        response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "my key"},
        )
        assert response.status_code == 400

    def test_wildcard_scope_not_allowed(self):
        response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "my key", "scopes": ["*"]},
        )
        assert response.status_code == 400
        assert "Wildcard" in response.json()["detail"]

    def test_only_allowed_scopes_accepted(self):
        response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "my key", "scopes": ["insight:read"]},
        )
        assert response.status_code == 400
        assert "can not be assigned" in response.json()["detail"]

    def test_invalid_scope_format_rejected(self):
        response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "my key", "scopes": ["notavalidscope"]},
        )
        assert response.status_code == 400
        assert "Invalid scope" in response.json()["detail"]

    def test_update_label(self):
        create_response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "original", "scopes": ["endpoint:read"]},
        )
        key_id = create_response.json()["id"]

        response = self.client.patch(
            f"/api/projects/{self.team.id}/project_secret_api_keys/{key_id}",
            {"label": "updated"},
        )
        assert response.status_code == 200
        assert response.json()["label"] == "updated"

    def test_update_scopes_to_empty_rejected(self):
        create_response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "my key", "scopes": ["endpoint:read"]},
        )
        key_id = create_response.json()["id"]

        response = self.client.patch(
            f"/api/projects/{self.team.id}/project_secret_api_keys/{key_id}",
            {"scopes": []},
        )
        assert response.status_code == 400

    def test_delete(self):
        create_response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "deleteme", "scopes": ["endpoint:read"]},
        )
        key_id = create_response.json()["id"]

        response = self.client.delete(f"/api/projects/{self.team.id}/project_secret_api_keys/{key_id}")
        assert response.status_code == 204
        assert not ProjectSecretAPIKey.objects.filter(id=key_id).exists()

    def test_roll(self):
        create_response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "rollme", "scopes": ["endpoint:read"]},
        )
        data = create_response.json()
        key_id = data["id"]
        original_mask = data["mask_value"]

        response = self.client.post(f"/api/projects/{self.team.id}/project_secret_api_keys/{key_id}/roll")
        assert response.status_code == 200
        rolled = response.json()

        assert rolled["label"] == "rollme"
        assert rolled["scopes"] == ["endpoint:read"]
        assert rolled["mask_value"] != original_mask
        assert rolled["last_rolled_at"] is not None
        assert rolled["value"] is not None

        key = ProjectSecretAPIKey.objects.get(id=key_id)
        assert key.secure_value != create_response.json().get("secure_value")

    def test_list_only_current_team_keys(self):
        self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "team1 key", "scopes": ["endpoint:read"]},
        )

        other_team = Team.objects.create(organization=self.organization, name="other team")
        ProjectSecretAPIKey.objects.create(
            team=other_team,
            label="team2 key",
            secure_value="sha256$abc123",
            mask_value="phs_...1234",
            scopes=["endpoint:read"],
        )

        response = self.client.get(f"/api/projects/{self.team.id}/project_secret_api_keys")
        assert response.status_code == 200
        results = response.json()["results"]
        assert len(results) == 1
        assert results[0]["label"] == "team1 key"

    def test_cannot_access_other_teams_keys(self):
        other_org = Organization.objects.create(name="other org")
        other_team = Team.objects.create(organization=other_org, name="other team")
        ProjectSecretAPIKey.objects.create(
            team=other_team,
            label="secret",
            secure_value="sha256$other",
            mask_value="phs_...5678",
            scopes=["endpoint:read"],
        )

        response = self.client.get(f"/api/projects/{other_team.id}/project_secret_api_keys")
        assert response.status_code == 403

    def test_duplicate_label_rejected(self):
        self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "unique-name", "scopes": ["endpoint:read"]},
        )
        response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "unique-name", "scopes": ["endpoint:read"]},
        )
        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]

    def test_value_only_returned_on_create(self):
        create_response = self.client.post(
            f"/api/projects/{self.team.id}/project_secret_api_keys",
            {"label": "my key", "scopes": ["endpoint:read"]},
        )
        assert create_response.json()["value"] is not None

        key_id = create_response.json()["id"]
        get_response = self.client.get(f"/api/projects/{self.team.id}/project_secret_api_keys/{key_id}")
        assert get_response.json()["value"] is None
