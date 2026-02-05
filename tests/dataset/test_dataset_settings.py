"""
**File:** ``test_dataset_settings.py``
**Region:** ``tests/dataset/test_dataset_settings``

PostgreSQLDataset settings and initialization tests.

Covers:
- Dataset type.
- Settings initialization and default values.
- ReadSettings and CreateSettings initialization.
"""

from __future__ import annotations

import uuid
from typing import Any, cast
from unittest.mock import MagicMock

from ds_provider_postgresql_py_lib.dataset.postgresql import (
    CreateSettings,
    PostgreSQLDataset,
    PostgreSQLDatasetSettings,
    ReadSettings,
)
from ds_provider_postgresql_py_lib.enums import ResourceType
from tests.mocks import create_mock_linked_service


def test_dataset_type_is_dataset() -> None:
    """
    It exposes dataset type.
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
    assert dataset.type == ResourceType.DATASET


def test_settings_initialization() -> None:
    """
    It initializes settings with default values.
    """
    props = PostgreSQLDatasetSettings(table="test_table")
    assert props.table == "test_table"
    assert props.schema == "public"
    assert props.read is None
    assert props.create is None


def test_settings_with_custom_schema() -> None:
    """
    It accepts custom schema value.
    """
    props = PostgreSQLDatasetSettings(table="test_table", schema="custom_schema")
    assert props.schema == "custom_schema"


def test_read_settings_initialization() -> None:
    """
    It initializes ReadSettings with default values.
    """
    read_props = ReadSettings()
    assert read_props.limit is None
    assert read_props.columns is None
    assert read_props.filters is None
    assert read_props.order_by is None


def test_read_settings_with_values() -> None:
    """
    It accepts custom read property values.
    """
    read_props = ReadSettings(
        limit=100,
        columns=["id", "name"],
        filters={"status": "active"},
        order_by=["created_at"],
    )
    assert read_props.limit == 100
    assert read_props.columns == ["id", "name"]
    assert read_props.filters == {"status": "active"}
    assert read_props.order_by == ["created_at"]


def test_create_settings_initialization() -> None:
    """
    It initializes CreateSettings with default values.
    """
    create_props = CreateSettings()
    assert create_props.mode == "fail"
    assert create_props.index is False


def test_create_settings_with_values() -> None:
    """
    It accepts custom create property values.
    """
    create_props = CreateSettings(mode="replace", index=True)
    assert create_props.mode == "replace"
    assert create_props.index is True


def test_close_closes_linked_service() -> None:
    """
    It closes the linked service when close() is called.
    """
    props = PostgreSQLDatasetSettings(table="test_table")
    linked_service = create_mock_linked_service()
    # Add a close method mock to verify it's called
    linked_service.close = MagicMock()
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )

    dataset.close()

    linked_service.close.assert_called_once()


def test_close_can_be_called_multiple_times() -> None:
    """
    It can be called multiple times without error.
    """
    props = PostgreSQLDatasetSettings(table="test_table")
    linked_service = create_mock_linked_service()
    linked_service.close = MagicMock()
    dataset = PostgreSQLDataset(
        id=uuid.uuid4(),
        name="test-dataset",
        version="1.0.0",
        linked_service=cast("Any", linked_service),
        settings=props,
    )

    dataset.close()
    dataset.close()
    dataset.close()

    # Should call linked_service.close() multiple times
    assert linked_service.close.call_count == 3
