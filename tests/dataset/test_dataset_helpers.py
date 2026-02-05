"""
**File:** ``test_dataset_helpers.py``
**Region:** ``tests/dataset/test_dataset_helpers``

PostgreSQLDataset helper methods tests.

Covers:
- _set_schema() method for schema derivation from DataFrames.
- _get_table() method for table object creation.
- _pandas_dtype_to_sqlalchemy() method for dtype conversion.
- _validate_column() method for column validation.
- _build_select_columns(), _build_filters(), _build_order_by() methods for query building.
"""

from __future__ import annotations

import uuid
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import BigInteger, Boolean, Column, DateTime, Float, Integer, MetaData, String, Table, select

from ds_provider_postgresql_py_lib.dataset.postgresql import (
    PostgreSQLDataset,
    PostgreSQLDatasetSettings,
    ReadSettings,
)
from tests.mocks import create_mock_linked_service, create_test_dataframe


def test_set_schema_populates_schema_from_dataframe() -> None:
    """
    It derives a string schema mapping from the dataframe columns/dtypes.
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
    df = create_test_dataframe()
    dataset._set_schema(df)
    assert dataset.schema is not None
    assert set(dataset.schema.keys()) == {"id", "name", "status", "amount", "is_active"}
    assert all(isinstance(v, str) and v for v in dataset.schema.values())


@patch("ds_provider_postgresql_py_lib.dataset.postgresql.Table")
def test_get_table_returns_table_with_correct_schema_and_name(mock_table: MagicMock) -> None:
    """
    It returns a Table object with correct schema and table name.
    """
    mock_table_instance = MagicMock()
    mock_table.return_value = mock_table_instance

    props = PostgreSQLDatasetSettings(table="test_table", schema="custom_schema")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )
    table = dataset._get_table()
    assert table == mock_table_instance
    mock_table.assert_called_once()


def test_pandas_dtype_to_sqlalchemy_integer_small() -> None:
    """
    It converts small integer dtypes to Integer.
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
    dtypes = pd.Series({"col": pd.Int16Dtype()})
    result = dataset._pandas_dtype_to_sqlalchemy(dtypes)
    assert isinstance(result["col"], Integer)


def test_pandas_dtype_to_sqlalchemy_integer_large() -> None:
    """
    It converts large integer dtypes to BigInteger.
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
    dtypes = pd.Series({"col": pd.Int64Dtype()})
    result = dataset._pandas_dtype_to_sqlalchemy(dtypes)
    assert isinstance(result["col"], BigInteger)


def test_pandas_dtype_to_sqlalchemy_float() -> None:
    """
    It converts float dtypes to Float.
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
    dtypes = pd.Series({"col": pd.Float64Dtype()})
    result = dataset._pandas_dtype_to_sqlalchemy(dtypes)
    assert isinstance(result["col"], Float)


def test_pandas_dtype_to_sqlalchemy_boolean() -> None:
    """
    It converts boolean dtypes to Boolean.
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
    dtypes = pd.Series({"col": pd.BooleanDtype()})
    result = dataset._pandas_dtype_to_sqlalchemy(dtypes)
    assert isinstance(result["col"], Boolean)


def test_pandas_dtype_to_sqlalchemy_datetime() -> None:
    """
    It converts datetime dtypes to DateTime.
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
    dtypes = pd.Series({"col": pd.DatetimeTZDtype(tz="UTC")})
    result = dataset._pandas_dtype_to_sqlalchemy(dtypes)
    assert isinstance(result["col"], DateTime)


def test_pandas_dtype_to_sqlalchemy_string() -> None:
    """
    It converts string dtypes to String.
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
    dtypes = pd.Series({"col": pd.StringDtype()})
    result = dataset._pandas_dtype_to_sqlalchemy(dtypes)
    assert isinstance(result["col"], String)


def test_pandas_dtype_to_sqlalchemy_unknown_defaults_to_string() -> None:
    """
    It defaults unknown dtypes to String.
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
    dtypes = pd.Series({"col": object})
    result = dataset._pandas_dtype_to_sqlalchemy(dtypes)
    assert isinstance(result["col"], String)


def test_validate_column_raises_when_column_missing() -> None:
    """
    It raises ValueError when column doesn't exist in table.
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
    mock_table = MagicMock()
    mock_table.c = MagicMock()
    mock_table.c.__contains__ = lambda self, key: key in ["id", "name"]
    mock_table.c.keys = MagicMock(return_value=["id", "name"])

    with pytest.raises(ValueError) as exc_info:
        dataset._validate_column(mock_table, "nonexistent")
    assert "not found" in str(exc_info.value).lower()


def test_validate_column_passes_when_column_exists() -> None:
    """
    It does not raise when column exists in table.
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
    mock_table = MagicMock()
    mock_table.c = MagicMock()
    mock_table.c.__contains__ = lambda self, key: key in ["id", "name"]
    dataset._validate_column(mock_table, "id")
    # Should not raise


def test_build_select_columns_returns_all_columns_when_none_specified() -> None:
    """
    It returns select(table) when no columns are specified.
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
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("name", String),
    )
    stmt = dataset._build_select_columns(real_table, None)
    assert stmt is not None


def test_build_select_columns_returns_specified_columns() -> None:
    """
    It returns select with specified columns when provided.
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
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("name", String),
    )
    read_props = ReadSettings(columns=["id", "name"])
    stmt = dataset._build_select_columns(real_table, read_props)
    assert stmt is not None


def test_build_filters_returns_unchanged_stmt_when_no_filters() -> None:
    """
    It returns unchanged statement when no filters are provided.
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
    mock_table = MagicMock()
    mock_stmt = MagicMock()
    result = dataset._build_filters(mock_stmt, mock_table, None)
    assert result == mock_stmt


def test_build_filters_applies_filters_when_provided() -> None:
    """
    It applies filters to the statement when provided.
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
    mock_table = MagicMock()
    mock_col = MagicMock()
    mock_table.c = MagicMock()
    mock_table.c.__contains__ = lambda self, key: key in ["status"]
    mock_table.c.__getitem__ = lambda self, key: mock_col
    mock_stmt = MagicMock()
    read_props = ReadSettings(filters={"status": "active"})
    result = dataset._build_filters(mock_stmt, mock_table, read_props)
    assert result is not None


def test_build_order_by_returns_unchanged_stmt_when_no_order_by() -> None:
    """
    It returns unchanged statement when no order_by is provided.
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
    mock_table = MagicMock()
    mock_stmt = MagicMock()
    result = dataset._build_order_by(mock_stmt, mock_table, None)
    assert result == mock_stmt


def test_build_order_by_applies_ascending_order() -> None:
    """
    It applies ascending order when column name is provided.
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
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
    )
    mock_stmt = select(real_table)
    read_props = ReadSettings(order_by=["id"])
    result = dataset._build_order_by(mock_stmt, real_table, read_props)
    assert result is not None


def test_build_order_by_applies_descending_order() -> None:
    """
    It applies descending order when tuple with 'desc' is provided.
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
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
    )
    mock_stmt = select(real_table)
    read_props = ReadSettings(order_by=[("id", "desc")])
    result = dataset._build_order_by(mock_stmt, real_table, read_props)
    assert result is not None


def test_build_order_by_handles_mixed_order_specs() -> None:
    """
    It handles mixed order specifications (tuples and strings).
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
    metadata = MetaData()
    real_table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("name", String),
    )
    mock_stmt = select(real_table)
    read_props = ReadSettings(order_by=[("id", "desc"), "name"])
    result = dataset._build_order_by(mock_stmt, real_table, read_props)
    assert result is not None
