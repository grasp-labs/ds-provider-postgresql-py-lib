"""
**File:** ``test_linked_service_properties.py``
**Region:** ``tests/linked_service/test_linked_service_properties``

PostgreSQLLinkedService properties and initialization tests.

Covers:
- Linked service kind property.
- Engine and pool property access before connection.
- Typed properties initialization and default values.
"""

from __future__ import annotations

from ds_provider_postgresql_py_lib.enums import ResourceKind
from ds_provider_postgresql_py_lib.linked_service.postgresql import (
    PostgreSQLLinkedService,
    PostgreSQLLinkedServiceTypedProperties,
)


def test_linked_service_kind_is_linked_service() -> None:
    """
    It exposes linked service kind.
    """
    props = PostgreSQLLinkedServiceTypedProperties(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(typed_properties=props)
    assert linked_service.kind == ResourceKind.LINKED_SERVICE


def test_engine_is_none_before_connect() -> None:
    """
    It returns None for engine property before connect() is called.
    """
    props = PostgreSQLLinkedServiceTypedProperties(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(typed_properties=props)
    assert linked_service.engine is None


def test_pool_is_none_before_connect() -> None:
    """
    It returns None for pool property before connect() is called.
    """
    props = PostgreSQLLinkedServiceTypedProperties(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(typed_properties=props)
    assert linked_service.pool is None


def test_typed_properties_initialization() -> None:
    """
    It initializes typed properties with default values.
    """
    props = PostgreSQLLinkedServiceTypedProperties(uri="postgresql://user:pass@localhost/db")
    assert props.uri == "postgresql://user:pass@localhost/db"
    assert props.pool_size == 5
    assert props.max_overflow == 10
    assert props.pool_timeout == 30
    assert props.pool_recycle == 3600


def test_typed_properties_custom_values() -> None:
    """
    It accepts custom pool parameter values.
    """
    props = PostgreSQLLinkedServiceTypedProperties(
        uri="postgresql://user:pass@localhost/db",
        pool_size=10,
        max_overflow=20,
        pool_timeout=60,
        pool_recycle=7200,
    )
    assert props.pool_size == 10
    assert props.max_overflow == 20
    assert props.pool_timeout == 60
    assert props.pool_recycle == 7200
