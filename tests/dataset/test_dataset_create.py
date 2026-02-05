"""
**File:** ``test_dataset_create.py``
**Region:** ``tests/dataset/test_dataset_create``

PostgreSQLDataset create() method tests.

Covers:
- create() method with various modes (append, replace, fail).
- Error handling (connection errors, empty content).
- Schema and index configuration.
- Exception wrapping into WriteError.
"""

from __future__ import annotations

import uuid
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import ConnectionError

from ds_provider_postgresql_py_lib.dataset.postgresql import (
    CreateSettings,
    PostgreSQLDataset,
    PostgreSQLDatasetSettings,
)
from ds_provider_postgresql_py_lib.linked_service.postgresql import (
    PostgreSQLLinkedService,
    PostgreSQLLinkedServiceSettings,
)
from tests.mocks import create_mock_linked_service, create_test_dataframe


def test_create_raises_when_connection_is_missing() -> None:
    """
    It raises ConnectionError when called without an initialized connection.
    """
    props = PostgreSQLDatasetSettings(table="test_table")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=PostgreSQLLinkedServiceSettings(
            uri="postgresql://user:pass@localhost/db",
        ),
    )
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    dataset.input = create_test_dataframe()
    with pytest.raises(ConnectionError):
        dataset.create()


def test_create_raises_when_input_is_empty() -> None:
    """
    It raises CreateError when input is empty or None.
    """
    props = PostgreSQLDatasetSettings(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    dataset.input = pd.DataFrame()
    with pytest.raises(CreateError) as exc_info:
        dataset.create()
    assert exc_info.value.status_code == 400
    assert "empty" in exc_info.value.message.lower()


def test_create_raises_when_input_is_none() -> None:
    """
    It raises CreateError when input is None.
    """
    props = PostgreSQLDatasetSettings(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    dataset.input = None
    with pytest.raises(CreateError) as exc_info:
        dataset.create()
    assert exc_info.value.status_code == 400


@patch("pandas.DataFrame.to_sql")
def test_create_writes_data_with_append_mode(mock_to_sql: MagicMock) -> None:
    """
    It writes data using append mode.
    """
    props = PostgreSQLDatasetSettings(
        table="test_table",
        create=CreateSettings(mode="append"),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    df = create_test_dataframe()
    dataset.input = df
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
    props = PostgreSQLDatasetSettings(
        table="test_table",
        create=CreateSettings(mode="replace"),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    df = create_test_dataframe()
    dataset.input = df
    dataset.create()
    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs["if_exists"] == "replace"


@patch("pandas.DataFrame.to_sql")
def test_create_writes_data_with_fail_mode(mock_to_sql: MagicMock) -> None:
    """
    It writes data using fail mode.
    """
    props = PostgreSQLDatasetSettings(
        table="test_table",
        create=CreateSettings(mode="fail"),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    df = create_test_dataframe()
    dataset.input = df
    dataset.create()
    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs["if_exists"] == "fail"


@patch("pandas.DataFrame.to_sql")
def test_create_uses_index_when_specified(mock_to_sql: MagicMock) -> None:
    """
    It includes index when index property is True.
    """
    props = PostgreSQLDatasetSettings(
        table="test_table",
        create=CreateSettings(index=True),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    df = create_test_dataframe()
    dataset.input = df
    dataset.create()
    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs["index"] is True


@patch("pandas.DataFrame.to_sql")
def test_create_uses_custom_schema(mock_to_sql: MagicMock) -> None:
    """
    It uses custom schema when specified.
    """
    props = PostgreSQLDatasetSettings(
        table="test_table",
        schema="custom_schema",
        create=CreateSettings(index=True),
    )
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    df = create_test_dataframe()
    dataset.input = df
    dataset.create()
    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs["schema"] == "custom_schema"


@patch("pandas.DataFrame.to_sql")
def test_create_wraps_exception_into_write_error(mock_to_sql: MagicMock) -> None:
    """
    It wraps exceptions into CreateError with correct details.
    """
    mock_to_sql.side_effect = Exception("Database error")
    props = PostgreSQLDatasetSettings(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    df = create_test_dataframe()
    dataset.input = df
    with pytest.raises(CreateError) as exc_info:
        dataset.create()
    assert exc_info.value.status_code == 500
    assert "failed to write" in exc_info.value.message.lower()
    assert exc_info.value.details["table"] == "test_table"
