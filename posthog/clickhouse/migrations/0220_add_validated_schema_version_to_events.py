from posthog.clickhouse.client.migration_tools import NodeRole, run_sql_with_exceptions
from posthog.models.event.sql import EVENTS_TABLE_JSON_MV_SQL, KAFKA_EVENTS_TABLE_JSON_SQL

ALTER_EVENTS_TABLE_ADD_COLUMN = """
ALTER TABLE IF EXISTS {table_name}
    ADD COLUMN IF NOT EXISTS validated_schema_version Int16 DEFAULT 0
"""

DROP_KAFKA_EVENTS_TABLE_JSON = """
    DROP TABLE IF EXISTS kafka_events_json
"""

DROP_EVENTS_TABLE_JSON_MV = """
    DROP TABLE IF EXISTS events_json_mv
"""


operations = [
    # First drop the materialized view
    run_sql_with_exceptions(DROP_EVENTS_TABLE_JSON_MV),
    # Then drop the kafka table
    run_sql_with_exceptions(DROP_KAFKA_EVENTS_TABLE_JSON),
    # Add column to sharded_events (data nodes, one per shard)
    run_sql_with_exceptions(
        ALTER_EVENTS_TABLE_ADD_COLUMN.format(table_name="sharded_events"),
        sharded=True,
    ),
    # Add column to writable_events (distributed write table)
    run_sql_with_exceptions(
        ALTER_EVENTS_TABLE_ADD_COLUMN.format(table_name="writable_events"),
    ),
    # Add column to events (distributed read table, all nodes)
    run_sql_with_exceptions(
        ALTER_EVENTS_TABLE_ADD_COLUMN.format(table_name="events"),
        node_roles=[NodeRole.DATA, NodeRole.COORDINATOR],
    ),
    # Recreate the kafka table (with the new column from EVENTS_TABLE_BASE_SQL)
    run_sql_with_exceptions(KAFKA_EVENTS_TABLE_JSON_SQL()),
    # Recreate the materialized view (with validated_schema_version in SELECT)
    run_sql_with_exceptions(EVENTS_TABLE_JSON_MV_SQL()),
]
