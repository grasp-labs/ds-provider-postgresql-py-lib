"""
**File:** ``test_dataset_properties.py``
**Region:** ``tests/dataset/test_dataset_properties``

PostgreSQLDataset properties and initialization tests.

Covers:
- Dataset kind property.
- Typed properties initialization and default values.
- ReadTypedProperties and CreateTypedProperties initialization.
"""

from __future__ import annotations

from typing import Any, cast

from ds_provider_postgresql_py_lib.dataset.postgresql import (
    CreateTypedProperties,
    PostgreSQLDataset,
    PostgreSQLDatasetTypedProperties,
    ReadTypedProperties,
)
from ds_provider_postgresql_py_lib.enums import ResourceKind
from tests.mocks import create_mock_linked_service


def test_dataset_kind_is_dataset() -> None:
    """
    It exposes dataset kind.
    """
    props = PostgreSQLDatasetTypedProperties(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    assert dataset.kind == ResourceKind.DATASET


def test_typed_properties_initialization() -> None:
    """
    It initializes typed properties with default values.
    """
    props = PostgreSQLDatasetTypedProperties(table="test_table")
    assert props.table == "test_table"
    assert props.schema == "public"
    assert props.read is None
    assert props.create is None


def test_typed_properties_with_custom_schema() -> None:
    """
    It accepts custom schema value.
    """
    props = PostgreSQLDatasetTypedProperties(table="test_table", schema="custom_schema")
    assert props.schema == "custom_schema"


def test_read_typed_properties_initialization() -> None:
    """
    It initializes ReadTypedProperties with default values.
    """
    read_props = ReadTypedProperties()
    assert read_props.limit is None
    assert read_props.columns is None
    assert read_props.filters is None
    assert read_props.order_by is None


def test_read_typed_properties_with_values() -> None:
    """
    It accepts custom read property values.
    """
    read_props = ReadTypedProperties(
        limit=100,
        columns=["id", "name"],
        filters={"status": "active"},
        order_by=["created_at"],
    )
    assert read_props.limit == 100
    assert read_props.columns == ["id", "name"]
    assert read_props.filters == {"status": "active"}
    assert read_props.order_by == ["created_at"]


def test_create_typed_properties_initialization() -> None:
    """
    It initializes CreateTypedProperties with default values.
    """
    create_props = CreateTypedProperties()
    assert create_props.mode == "append"
    assert create_props.index is False


def test_create_typed_properties_with_values() -> None:
    """
    It accepts custom create property values.
    """
    create_props = CreateTypedProperties(mode="replace", index=True)
    assert create_props.mode == "replace"
    assert create_props.index is True
