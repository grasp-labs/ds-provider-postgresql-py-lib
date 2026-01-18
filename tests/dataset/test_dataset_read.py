"""
**File:** ``test_dataset_read.py``
**Region:** ``tests/dataset/test_dataset_read``

PostgreSQLDataset read() method tests.

Covers:
- read() method with columns, filters, order_by, limit.
- Error handling (connection errors, read errors).
- Schema setting from content.
- Exception wrapping into ReadError.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import ConnectionError
from sqlalchemy import Column, Integer, MetaData, String, Table

from ds_provider_postgresql_py_lib.dataset.postgresql import (
    PostgreSQLDataset,
    PostgreSQLDatasetSettings,
    ReadSettings,
)
from ds_provider_postgresql_py_lib.linked_service.postgresql import (
    PostgreSQLLinkedService,
    PostgreSQLLinkedServiceSettings,
)
from tests.mocks import create_mock_linked_service, create_test_dataframe


def test_read_raises_when_connection_is_missing() -> None:
    """
    It raises ConnectionError when read is called without an initialized connection.
    """
    props = PostgreSQLDatasetSettings(table="test_table")
    linked_service = PostgreSQLLinkedService(settings=PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db"))
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    with pytest.raises(ConnectionError):
        dataset.read()


@patch("pandas.read_sql")
@patch("ds_provider_postgresql_py_lib.dataset.postgresql.Table")
def test_read_reads_all_columns_when_none_specified(mock_table: MagicMock, mock_read_sql: MagicMock) -> None:
    """
    It reads all columns when no columns are specified.
    """
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("name", String),
        Column("status", String),
    )
    mock_table.return_value = real_table
    mock_read_sql.return_value = [create_test_dataframe()]

    props = PostgreSQLDatasetSettings(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    dataset.read()
    assert dataset.output is not None
    assert isinstance(dataset.output, pd.DataFrame)
    assert dataset.next is False


@patch("pandas.read_sql")
@patch("ds_provider_postgresql_py_lib.dataset.postgresql.Table")
def test_read_applies_column_selection(mock_table: MagicMock, mock_read_sql: MagicMock) -> None:
    """
    It applies column selection when specified.
    """
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("name", String),
        Column("status", String),
    )
    mock_table.return_value = real_table
    mock_read_sql.return_value = [create_test_dataframe()]

    props = PostgreSQLDatasetSettings(
        table="test_table",
        read=ReadSettings(columns=["id", "name"]),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    dataset.read()
    assert dataset.output is not None


@patch("pandas.read_sql")
@patch("ds_provider_postgresql_py_lib.dataset.postgresql.Table")
def test_read_applies_filters(mock_table: MagicMock, mock_read_sql: MagicMock) -> None:
    """
    It applies filters to the WHERE clause.
    """
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("name", String),
        Column("status", String),
    )
    mock_table.return_value = real_table
    mock_read_sql.return_value = [create_test_dataframe()]

    props = PostgreSQLDatasetSettings(
        table="test_table",
        read=ReadSettings(filters={"status": "active"}),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    dataset.read()
    assert dataset.output is not None


@patch("pandas.read_sql")
@patch("ds_provider_postgresql_py_lib.dataset.postgresql.Table")
def test_read_applies_order_by(mock_table: MagicMock, mock_read_sql: MagicMock) -> None:
    """
    It applies order_by to the ORDER BY clause.
    """
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("name", String),
    )
    mock_table.return_value = real_table
    mock_read_sql.return_value = [create_test_dataframe()]

    props = PostgreSQLDatasetSettings(
        table="test_table",
        read=ReadSettings(order_by=["id"]),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    dataset.read()
    assert dataset.output is not None


@patch("pandas.read_sql")
@patch("ds_provider_postgresql_py_lib.dataset.postgresql.Table")
def test_read_applies_limit(mock_table: MagicMock, mock_read_sql: MagicMock) -> None:
    """
    It applies limit to the query.
    """
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("name", String),
    )
    mock_table.return_value = real_table
    mock_read_sql.return_value = [create_test_dataframe()]

    props = PostgreSQLDatasetSettings(
        table="test_table",
        read=ReadSettings(limit=10),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    dataset.read()
    assert dataset.output is not None


@patch("pandas.read_sql")
@patch("ds_provider_postgresql_py_lib.dataset.postgresql.Table")
def test_read_sets_schema_from_content(mock_table: MagicMock, mock_read_sql: MagicMock) -> None:
    """
    It sets schema from content after reading.
    """
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("name", String),
    )
    mock_table.return_value = real_table
    df = create_test_dataframe()
    mock_read_sql.return_value = [df]

    props = PostgreSQLDatasetSettings(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    dataset.read()
    assert dataset.schema is not None
    assert len(dataset.schema) > 0


@patch("pandas.read_sql")
@patch("ds_provider_postgresql_py_lib.dataset.postgresql.Table")
def test_read_wraps_exception_into_read_error(mock_table: MagicMock, mock_read_sql: MagicMock) -> None:
    """
    It wraps exceptions into ReadError with correct details.
    """
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("name", String),
    )
    mock_table.return_value = real_table
    mock_read_sql.side_effect = Exception("Database error")

    props = PostgreSQLDatasetSettings(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    with pytest.raises(ReadError) as exc_info:
        dataset.read()
    assert exc_info.value.status_code == 500
    assert "failed to read" in exc_info.value.message.lower()
    assert exc_info.value.details["table"] == "test_table"
