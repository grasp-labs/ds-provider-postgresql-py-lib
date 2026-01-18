"""
**File:** ``01_linked_service_connect.py``
**Region:** ``examples/01_linked_service_connect``

Example 01: Connect to PostgreSQL database using PostgreSQLLinkedService.

This example demonstrates how to:
- Create a PostgreSQL linked service
- Connect to the database
- Test the connection
"""

from __future__ import annotations

from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException

from ds_provider_postgresql_py_lib.linked_service.postgresql import (
    PostgreSQLLinkedService,
    PostgreSQLLinkedServiceSettings,
)

Logger()
logger = Logger.get_logger(__name__)


def main() -> None:
    """Main function demonstrating PostgreSQL linked service connection."""
    linked_service = PostgreSQLLinkedService(
        settings=PostgreSQLLinkedServiceSettings(
            uri="postgresql://user:password@localhost:5432/mydb",
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600,
        ),
    )

    try:
        logger.info("Connecting to PostgreSQL database...")
        linked_service.connect()

        logger.info("Testing connection...")
        success, message = linked_service.test_connection()
        if success:
            logger.info(f"Connection test successful: {message}")
            logger.info(f"Connection pool size: {linked_service.settings.pool_size}")
        else:
            raise ResourceException(message=message)
    except ResourceException as exc:
        logger.error(f"Failed to connect to PostgreSQL database: {exc.message}")
        raise
    except Exception as exc:
        logger.error(f"Unexpected error: {exc!s}")
        raise


if __name__ == "__main__":
    main()
