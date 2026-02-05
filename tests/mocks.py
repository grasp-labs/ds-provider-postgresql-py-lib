"""
**File:** ``mocks.py``
**Region:** ``tests/mocks``

Shared mock utilities for PostgreSQL provider tests.

Provides reusable mocks for SQLAlchemy engines, connections, pools, and tables
to enable isolated unit testing without requiring actual database connections.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import pandas as pd
from sqlalchemy.pool import Pool

if TYPE_CHECKING:
    from sqlalchemy import Engine

from ds_provider_postgresql_py_lib.dataset.postgresql import (
    PostgreSQLDataset,
    PostgreSQLDatasetSettings,
    ReadSettings,
)
from ds_provider_postgresql_py_lib.linked_service.postgresql import (
    PostgreSQLLinkedService,
    PostgreSQLLinkedServiceSettings,
)


class MockEngine:
    """
    Mock SQLAlchemy Engine for testing.

    Provides a mock engine with a mock pool and connection context manager.
    """

    def __init__(self) -> None:
        self.pool = MagicMock(spec=Pool)
        self._connection = MagicMock()
        self._connection.execute = MagicMock(return_value=MagicMock(fetchone=MagicMock(return_value=(1,))))

    def begin(self) -> Any:
        """
        Return a context manager that yields a mock connection.
        """
        context_manager = MagicMock()
        context_manager.__enter__ = MagicMock(return_value=self._connection)
        context_manager.__exit__ = MagicMock(return_value=None)
        return context_manager


class MockTable:
    """
    Mock SQLAlchemy Table for testing.

    Provides a mock table with configurable columns.
    """

    def __init__(self, columns: list[str]) -> None:
        self.c = MagicMock()
        self._columns = columns
        for col_name in columns:
            col_mock = MagicMock()
            col_mock.name = col_name
            setattr(self.c, col_name, col_mock)
        self.c.keys = MagicMock(return_value=columns)
        self.c.__contains__ = lambda self, key: key in columns


def create_mock_linked_service(uri: str = "postgresql://user:pass@localhost/db") -> PostgreSQLLinkedService:
    """
    Create a mock PostgreSQLLinkedService for testing.

    Args:
        uri: The connection URI to use.

    Returns:
        PostgreSQLLinkedService: A linked service instance with mocked engine.
    """
    props = PostgreSQLLinkedServiceSettings(uri=uri)
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    linked_service._engine = cast("Engine", MockEngine())
    return linked_service


def create_mock_dataset(
    table: str = "test_table",
    schema: str = "public",
    linked_service: PostgreSQLLinkedService | None = None,
    read_props: ReadSettings | None = None,
) -> PostgreSQLDataset:
    """
    Create a mock PostgreSQLDataset for testing.

    Args:
        table: The table name.
        schema: The schema name.
        linked_service: Optional linked service. If None, creates a mock one.
        read_props: Optional read settings.

    Returns:
        PostgreSQLDataset: A dataset instance ready for testing.
    """
    if linked_service is None:
        linked_service = create_mock_linked_service()

    props = PostgreSQLDatasetSettings(
        table=table,
        schema=schema,
        read=read_props,
    )
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    return dataset


def create_test_dataframe(rows: int = 3) -> pd.DataFrame:
    """
    Create a test pandas DataFrame for testing.

    Args:
        rows: Number of rows to create.

    Returns:
        pd.DataFrame: A DataFrame with test data.
    """
    return pd.DataFrame(
        {
            "id": list(range(1, rows + 1)),
            "name": [f"Name{i}" for i in range(1, rows + 1)],
            "status": ["active", "inactive", "pending"][:rows],
            "amount": [10.5, 20.0, 30.75][:rows],
            "is_active": [True, False, True][:rows],
        }
    )
