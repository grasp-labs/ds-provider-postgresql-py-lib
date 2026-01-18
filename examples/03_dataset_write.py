"""
**File:** ``03_dataset_write.py``
**Region:** ``examples/03_dataset_write``

Example 03: Write data to a PostgreSQL table using PostgreSQLDataset.

This example demonstrates how to:
- Create a PostgreSQL dataset
- Configure write settings (mode: append, replace, delete_rows)
- Write data to a table
"""

from __future__ import annotations

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException

from ds_provider_postgresql_py_lib.dataset.postgresql import (
    CreateSettings,
    PostgreSQLDatasetSettings,
    PostgreSQLDataset,
)
from ds_provider_postgresql_py_lib.linked_service.postgresql import (
    PostgreSQLLinkedService,
    PostgreSQLLinkedServiceSettings,
)

Logger()
logger = Logger.get_logger(__name__)


def main() -> None:
    """Main function demonstrating PostgreSQL dataset write operation."""
    data = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "email": [
                "alice@example.com",
                "bob@example.com",
                "charlie@example.com",
                "diana@example.com",
                "eve@example.com",
            ],
            "age": [25, 30, 35, 28, 32],
            "is_active": [True, True, False, True, True],
        }
    )

    dataset = PostgreSQLDataset(
        linked_service=PostgreSQLLinkedService(
            settings=PostgreSQLLinkedServiceSettings(
                uri="postgresql://user:password@localhost:5432/mydb",
            ),
        ),
        settings=PostgreSQLDatasetSettings(
            schema="public",
            table="users",
            create=CreateSettings(
                mode="replace",
                index=False,
            ),
        ),
    )

    try:
        logger.info("Writing data to PostgreSQL table...")
        dataset.input = data
        dataset.linked_service.connect()
        dataset.create()
        logger.info("Data written successfully")
    except ResourceException as exc:
        logger.error(f"Failed to write data: {exc.__dict__}")
        raise


if __name__ == "__main__":
    main()
