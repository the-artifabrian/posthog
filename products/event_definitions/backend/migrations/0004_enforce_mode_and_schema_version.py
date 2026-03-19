from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("event_definitions", "0003_schemapropertygroupproperty_validation_rules"),
    ]

    operations = [
        migrations.AlterField(
            model_name="eventdefinition",
            name="enforcement_mode",
            field=models.CharField(
                choices=[("allow", "Allow"), ("enforce", "Enforce"), ("reject", "Reject")],
                default="allow",
                max_length=10,
            ),
        ),
        migrations.AddField(
            model_name="eventdefinition",
            name="schema_version",
            field=models.PositiveIntegerField(default=1),
        ),
        migrations.RemoveIndex(
            model_name="eventdefinition",
            name="posthog_eventdef_enforce_idx",
        ),
        migrations.AddIndex(
            model_name="eventdefinition",
            index=models.Index(
                condition=models.Q(("enforcement_mode__in", ["reject", "enforce"])),
                fields=["team_id"],
                name="posthog_eventdef_enforce_idx",
            ),
        ),
    ]
