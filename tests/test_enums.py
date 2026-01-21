"""
**File:** ``test_enums.py``
**Region:** ``tests/test_enums``

ResourceType enum tests.

Covers:
- Enum value definitions and string representations.
- Enum membership and comparison operations.
"""

from __future__ import annotations

from ds_provider_postgresql_py_lib.enums import ResourceType


def test_resource_type_linked_service_value() -> None:
    """
    It exposes the correct linked service type value.
    """
    assert ResourceType.LINKED_SERVICE == "DS.RESOURCE.LINKED_SERVICE.POSTGRESQL"
    assert isinstance(ResourceType.LINKED_SERVICE, str)


def test_resource_type_dataset_value() -> None:
    """
    It exposes the correct dataset type value.
    """
    assert ResourceType.DATASET == "DS.RESOURCE.DATASET.POSTGRESQL"
    assert isinstance(ResourceType.DATASET, str)


def test_resource_type_enum_membership() -> None:
    """
    It allows checking enum membership.
    """
    assert ResourceType.LINKED_SERVICE in ResourceType
    assert ResourceType.DATASET in ResourceType


def test_resource_type_enum_comparison() -> None:
    """
    It supports equality comparison with strings.
    """
    assert ResourceType.LINKED_SERVICE == "DS.RESOURCE.LINKED_SERVICE.POSTGRESQL"
    assert ResourceType.DATASET == "DS.RESOURCE.DATASET.POSTGRESQL"
    assert ResourceType.LINKED_SERVICE != ResourceType.DATASET
