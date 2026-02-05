"""
**File:** ``test_linked_service_settings.py``
**Region:** ``tests/linked_service/test_linked_service_settings``

PostgreSQLLinkedService settings and initialization tests.

Covers:
- Linked service type.
- Engine and pool access before connection.
- Settings initialization and default values.
"""

from __future__ import annotations

import uuid

from ds_provider_postgresql_py_lib.enums import ResourceType
from ds_provider_postgresql_py_lib.linked_service.postgresql import (
    PostgreSQLLinkedService,
    PostgreSQLLinkedServiceSettings,
)


def test_linked_service_type_is_linked_service() -> None:
    """
    It exposes linked service type.
    """
    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    assert linked_service.type == ResourceType.LINKED_SERVICE


def test_engine_is_none_before_connect() -> None:
    """
    It returns None for engine property before connect() is called.
    """
    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    assert linked_service.engine is None


def test_pool_is_none_before_connect() -> None:
    """
    It returns None for pool property before connect() is called.
    """
    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    assert linked_service.pool is None


def test_settings_initialization() -> None:
    """
    It initializes settings with default values.
    """
    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    assert props.uri == "postgresql://user:pass@localhost/db"
    assert props.pool_size == 5
    assert props.max_overflow == 10
    assert props.pool_timeout == 30
    assert props.pool_recycle == 3600


def test_settings_custom_values() -> None:
    """
    It accepts custom pool parameter values.
    """
    props = PostgreSQLLinkedServiceSettings(
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
