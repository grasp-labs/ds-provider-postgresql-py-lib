"""
**File:** ``test_linked_service_connect.py``
**Region:** ``tests/linked_service/test_linked_service_connect``

PostgreSQLLinkedService connection management tests.

Covers:
- Connection initialization via connect() call.
- Engine creation and pool configuration.
- Idempotent connection behavior.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

from sqlalchemy import Engine
from sqlalchemy.pool import Pool

from ds_provider_postgresql_py_lib.linked_service.postgresql import (
    PostgreSQLLinkedService,
    PostgreSQLLinkedServiceSettings,
)


@patch("ds_provider_postgresql_py_lib.linked_service.postgresql.create_engine")
def test_connect_creates_engine_on_first_call(mock_create_engine: MagicMock) -> None:
    """
    It creates an engine on the first connect() call.
    """
    mock_engine = MagicMock(spec=Engine)
    mock_engine.pool = MagicMock(spec=Pool)
    mock_create_engine.return_value = mock_engine

    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    linked_service.connect()

    assert linked_service.engine is not None
    assert linked_service.engine == mock_engine
    mock_create_engine.assert_called_once()


@patch("ds_provider_postgresql_py_lib.linked_service.postgresql.create_engine")
def test_connect_uses_correct_pool_parameters(mock_create_engine: MagicMock) -> None:
    """
    It passes correct pool parameters to create_engine.
    """
    mock_engine = MagicMock(spec=Engine)
    mock_engine.pool = MagicMock(spec=Pool)
    mock_create_engine.return_value = mock_engine

    props = PostgreSQLLinkedServiceSettings(
        uri="postgresql://user:pass@localhost/db",
        pool_size=10,
        max_overflow=20,
        pool_timeout=60,
        pool_recycle=7200,
    )
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    linked_service.connect()

    mock_create_engine.assert_called_once_with(
        url="postgresql://user:pass@localhost/db",
        pool_size=10,
        max_overflow=20,
        pool_timeout=60,
        pool_recycle=7200,
    )


@patch("ds_provider_postgresql_py_lib.linked_service.postgresql.create_engine")
def test_connect_is_idempotent(mock_create_engine: MagicMock) -> None:
    """
    It does not recreate engine on subsequent connect() calls.
    """
    mock_engine = MagicMock(spec=Engine)
    mock_engine.pool = MagicMock(spec=Pool)
    mock_create_engine.return_value = mock_engine

    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    linked_service.connect()
    linked_service.connect()
    linked_service.connect()

    assert linked_service.engine is not None
    mock_create_engine.assert_called_once()


@patch("ds_provider_postgresql_py_lib.linked_service.postgresql.create_engine")
def test_pool_returns_engine_pool_after_connect(mock_create_engine: MagicMock) -> None:
    """
    It returns the engine's pool after connect() is called.
    """
    mock_engine = MagicMock(spec=Engine)
    mock_pool = MagicMock(spec=Pool)
    mock_engine.pool = mock_pool
    mock_create_engine.return_value = mock_engine

    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    linked_service.connect()

    assert linked_service.pool is not None
    assert linked_service.pool == mock_pool


@patch("ds_provider_postgresql_py_lib.linked_service.postgresql.create_engine")
def test_close_disposes_engine_when_connected(mock_create_engine: MagicMock) -> None:
    """
    It disposes the engine and sets it to None when close() is called after connect().
    """
    mock_engine = MagicMock(spec=Engine)
    mock_engine.pool = MagicMock(spec=Pool)
    mock_create_engine.return_value = mock_engine

    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    linked_service.connect()

    assert linked_service.engine is not None
    linked_service.close()

    mock_engine.dispose.assert_called_once()
    assert linked_service.engine is None


@patch("ds_provider_postgresql_py_lib.linked_service.postgresql.create_engine")
def test_close_is_idempotent(mock_create_engine: MagicMock) -> None:
    """
    It can be called multiple times without error.
    """
    mock_engine = MagicMock(spec=Engine)
    mock_engine.pool = MagicMock(spec=Pool)
    mock_create_engine.return_value = mock_engine

    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    linked_service.connect()
    linked_service.close()
    linked_service.close()
    linked_service.close()

    assert linked_service.engine is None
    mock_engine.dispose.assert_called_once()


def test_close_does_nothing_when_not_connected() -> None:
    """
    It does nothing when close() is called without a connection.
    """
    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )

    assert linked_service.engine is None
    linked_service.close()
    assert linked_service.engine is None
