"""
**File:** ``test_dataset_unimplemented.py``
**Region:** ``tests/dataset/test_dataset_unimplemented``

PostgreSQLDataset unimplemented methods tests.

Covers:
- delete(), update(), and rename() methods raising NotImplementedError.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from ds_provider_postgresql_py_lib.dataset.postgresql import (
    PostgreSQLDataset,
    PostgreSQLDatasetTypedProperties,
)
from tests.mocks import create_mock_linked_service


def test_delete_raises_not_implemented_error() -> None:
    """
    It raises NotImplementedError for delete operation.
    """
    props = PostgreSQLDatasetTypedProperties(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    with pytest.raises(NotImplementedError):
        dataset.delete()


def test_update_raises_not_implemented_error() -> None:
    """
    It raises NotImplementedError for update operation.
    """
    props = PostgreSQLDatasetTypedProperties(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    with pytest.raises(NotImplementedError):
        dataset.update()


def test_rename_raises_not_implemented_error() -> None:
    """
    It raises NotImplementedError for rename operation.
    """
    props = PostgreSQLDatasetTypedProperties(table="test_table")
    linked_service = create_mock_linked_service()
    dataset = PostgreSQLDataset(
        linked_service=cast("Any", linked_service),
        typed_properties=props,
    )
    with pytest.raises(NotImplementedError):
        dataset.rename()
