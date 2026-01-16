"""
**File:** ``test_dataset_create.py``
**Region:** ``tests/dataset/test_dataset_create``

PostgreSQLDataset create() method tests.

Covers:
- create() method with various modes (append, replace, delete_rows).
- Error handling (connection errors, empty content).
- Schema and index configuration.
- Exception wrapping into WriteError.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from ds_resource_plugin_py_lib.common.resource.dataset.errors import WriteError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import ConnectionError

from ds_provider_postgresql_py_lib.dataset.postgresql import (
    CreateTypedProperties,
    PostgreSQLDataset,
    PostgreSQLDatasetTypedProperties,
)
from ds_provider_postgresql_py_lib.linked_service.postgresql import (
    PostgreSQLLinkedService,
    PostgreSQLLinkedServiceTypedProperties,
)
from tests.mocks import create_mock_linked_service, create_test_dataframe


def test_create_raises_when_connection_is_missing() -> None:
    """
    It raises ConnectionError when called without an initialized connection.
    """
    props = PostgreSQLDatasetTypedProperties(table="test_table")
    linked_service = PostgreSQLLinkedService(
        typed_properties=PostgreSQLLinkedServiceTypedProperties(uri="postgresql://user:pass@localhost/db")
    )
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    dataset.content = create_test_dataframe()
    with pytest.raises(ConnectionError):
        dataset.create()


def test_create_raises_when_content_is_empty() -> None:
    """
    It raises WriteError when content is empty or None.
    """
    props = PostgreSQLDatasetTypedProperties(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    dataset.content = pd.DataFrame()
    with pytest.raises(WriteError) as exc_info:
        dataset.create()
    assert exc_info.value.status_code == 400
    assert "empty" in exc_info.value.message.lower()


def test_create_raises_when_content_is_none() -> None:
    """
    It raises WriteError when content is None.
    """
    props = PostgreSQLDatasetTypedProperties(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    dataset.content = None
    with pytest.raises(WriteError) as exc_info:
        dataset.create()
    assert exc_info.value.status_code == 400


@patch("pandas.DataFrame.to_sql")
def test_create_writes_data_with_append_mode(mock_to_sql: MagicMock) -> None:
    """
    It writes data using append mode.
    """
    props = PostgreSQLDatasetTypedProperties(
        table="test_table",
        create=CreateTypedProperties(mode="append"),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    df = create_test_dataframe()
    dataset.content = df
    dataset.create()
    mock_to_sql.assert_called_once()
    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs["name"] == "test_table"
    assert call_kwargs["if_exists"] == "append"
    assert call_kwargs["schema"] == "public"


@patch("pandas.DataFrame.to_sql")
def test_create_writes_data_with_replace_mode(mock_to_sql: MagicMock) -> None:
    """
    It writes data using replace mode.
    """
    props = PostgreSQLDatasetTypedProperties(
        table="test_table",
        create=CreateTypedProperties(mode="replace"),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    df = create_test_dataframe()
    dataset.content = df
    dataset.create()
    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs["if_exists"] == "replace"


@patch("pandas.DataFrame.to_sql")
def test_create_writes_data_with_delete_rows_mode(mock_to_sql: MagicMock) -> None:
    """
    It writes data using delete_rows mode.
    """
    props = PostgreSQLDatasetTypedProperties(
        table="test_table",
        create=CreateTypedProperties(mode="delete_rows"),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    df = create_test_dataframe()
    dataset.content = df
    dataset.create()
    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs["if_exists"] == "delete_rows"


@patch("pandas.DataFrame.to_sql")
def test_create_uses_index_when_specified(mock_to_sql: MagicMock) -> None:
    """
    It includes index when index property is True.
    """
    props = PostgreSQLDatasetTypedProperties(
        table="test_table",
        create=CreateTypedProperties(index=True),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    df = create_test_dataframe()
    dataset.content = df
    dataset.create()
    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs["index"] is True


@patch("pandas.DataFrame.to_sql")
def test_create_uses_custom_schema(mock_to_sql: MagicMock) -> None:
    """
    It uses custom schema when specified.
    """
    props = PostgreSQLDatasetTypedProperties(
        table="test_table",
        schema="custom_schema",
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    df = create_test_dataframe()
    dataset.content = df
    dataset.create()
    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs["schema"] == "custom_schema"


@patch("pandas.DataFrame.to_sql")
def test_create_wraps_exception_into_write_error(mock_to_sql: MagicMock) -> None:
    """
    It wraps exceptions into WriteError with correct details.
    """
    mock_to_sql.side_effect = Exception("Database error")
    props = PostgreSQLDatasetTypedProperties(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    df = create_test_dataframe()
    dataset.content = df
    with pytest.raises(WriteError) as exc_info:
        dataset.create()
    assert exc_info.value.status_code == 500
    assert "failed to write" in exc_info.value.message.lower()
    assert exc_info.value.details["table"] == "test_table"
