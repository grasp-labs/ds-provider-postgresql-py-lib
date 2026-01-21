"""
**File:** ``02_dataset_read.py``
**Region:** ``examples/02_dataset_read``

Example 02: Read data from a PostgreSQL table using PostgreSQLDataset.

This example demonstrates how to:
- Create a PostgreSQL dataset
- Configure read (columns, filters, ordering, limit)
- Read data from a table
"""

from __future__ import annotations

from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException

from ds_provider_postgresql_py_lib.dataset.postgresql import (
    PostgreSQLDataset,
    PostgreSQLDatasetSettings,
    ReadSettings,
)
from ds_provider_postgresql_py_lib.linked_service.postgresql import (
    PostgreSQLLinkedService,
    PostgreSQLLinkedServiceSettings,
)

Logger()
logger = Logger.get_logger(__name__)


def main() -> None:
    """Main function demonstrating PostgreSQL dataset read operation."""
    dataset = PostgreSQLDataset(
        linked_service=PostgreSQLLinkedService(
            settings=PostgreSQLLinkedServiceSettings(
                uri="postgresql://user:password@localhost:5432/mydb",
            ),
        ),
        settings=PostgreSQLDatasetSettings(
            schema="public",
            table="users",
            read=ReadSettings(
                columns=["id", "name", "email", "created_at"],
                filters={"status": "active"},
                order_by=[("created_at", "desc")],
                limit=100,
            ),
        ),
    )

    try:
        dataset.linked_service.connect()
        logger.info(f"Reading data from table: {dataset.settings.schema}.{dataset.settings.table}")
        dataset.read()
        logger.info(f"Successfully read {len(dataset.output)} rows")
        logger.info(f"Columns: {list(dataset.output.columns)}")
        logger.info(f"Schema: {dataset.schema}")
    except ResourceException as exc:
        logger.error(f"Failed to read data: {exc.__dict__}")
        raise


if __name__ == "__main__":
    main()
