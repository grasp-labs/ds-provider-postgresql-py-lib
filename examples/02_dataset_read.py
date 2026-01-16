"""
**File:** ``02_dataset_read.py``
**Region:** ``examples/02_dataset_read``

Example 02: Read data from a PostgreSQL table using PostgreSQLDataset.

This example demonstrates how to:
- Create a PostgreSQL dataset
- Configure read properties (columns, filters, ordering, limit)
- Read data from a table
"""

from __future__ import annotations

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException

from ds_provider_postgresql_py_lib.dataset.postgresql import (
    PostgreSQLDataset,
    PostgreSQLDatasetTypedProperties,
    ReadTypedProperties,
)
from ds_provider_postgresql_py_lib.linked_service.postgresql import (
    PostgreSQLLinkedService,
    PostgreSQLLinkedServiceTypedProperties,
)

Logger()
logger = Logger.get_logger(__name__)


def main() -> None:
    """Main function demonstrating PostgreSQL dataset read operation."""
    dataset = PostgreSQLDataset(
        linked_service=PostgreSQLLinkedService(
            typed_properties=PostgreSQLLinkedServiceTypedProperties(
                uri="postgresql://user:password@localhost:5432/mydb",
            ),
        ),
        typed_properties=PostgreSQLDatasetTypedProperties(
            schema="public",
            table="users",
            read=ReadTypedProperties(
                columns=["id", "name", "email", "created_at"],
                filters={"status": "active"},
                order_by=[("created_at", "desc")],
                limit=100,
            ),
        ),
    )

    try:
        dataset.linked_service.connect()
        logger.info(f"Reading data from table: {dataset.typed_properties.schema}.{dataset.typed_properties.table}")
        dataset.read()
        df: pd.DataFrame = dataset.content
        logger.info(f"Successfully read {len(df)} rows")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info(f"Schema: {dataset.schema}")
    except ResourceException as exc:
        logger.error(f"Failed to read data: {exc.__dict__}")
        raise


if __name__ == "__main__":
    main()
