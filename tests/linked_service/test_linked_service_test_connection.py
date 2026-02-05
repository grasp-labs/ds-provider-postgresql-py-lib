"""
**File:** ``test_linked_service_test_connection.py``
**Region:** ``tests/linked_service/test_linked_service_test_connection``

PostgreSQLLinkedService test_connection() method tests.

Covers:
- Connection testing with success and failure scenarios.
- Auto-connection when test_connection() is called before connect().
- Error handling and exception reporting.
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
def test_test_connection_succeeds_when_connected(mock_create_engine: MagicMock) -> None:
    """
    It returns success when connection test passes.
    """
    mock_engine = MagicMock(spec=Engine)
    mock_engine.pool = MagicMock(spec=Pool)
    mock_connection = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone = MagicMock(return_value=(1,))
    mock_connection.execute = MagicMock(return_value=mock_result)
    mock_context = MagicMock()
    mock_context.__enter__ = MagicMock(return_value=mock_connection)
    mock_context.__exit__ = MagicMock(return_value=None)
    mock_engine.begin = MagicMock(return_value=mock_context)
    mock_create_engine.return_value = mock_engine

    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    linked_service.connect()

    success, message = linked_service.test_connection()
    assert success is True
    assert "successfully tested" in message.lower()


@patch("ds_provider_postgresql_py_lib.linked_service.postgresql.create_engine")
def test_test_connection_auto_connects_if_not_connected(mock_create_engine: MagicMock) -> None:
    """
    It automatically connects if test_connection() is called before connect().
    """
    mock_engine = MagicMock(spec=Engine)
    mock_engine.pool = MagicMock(spec=Pool)
    mock_connection = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone = MagicMock(return_value=(1,))
    mock_connection.execute = MagicMock(return_value=mock_result)
    mock_context = MagicMock()
    mock_context.__enter__ = MagicMock(return_value=mock_connection)
    mock_context.__exit__ = MagicMock(return_value=None)
    mock_engine.begin = MagicMock(return_value=mock_context)
    mock_create_engine.return_value = mock_engine

    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )

    success, message = linked_service.test_connection()
    assert success is True
    assert "connection successfully tested" in message.lower()
    assert linked_service.engine is not None
    mock_create_engine.assert_called_once()


@patch("ds_provider_postgresql_py_lib.linked_service.postgresql.create_engine")
def test_test_connection_fails_on_exception(mock_create_engine: MagicMock) -> None:
    """
    It returns failure when connection test raises an exception.
    """
    mock_engine = MagicMock(spec=Engine)
    mock_engine.pool = MagicMock(spec=Pool)
    mock_context = MagicMock()
    mock_context.__enter__ = MagicMock(side_effect=Exception("Connection failed"))
    mock_context.__exit__ = MagicMock(return_value=None)
    mock_engine.begin = MagicMock(return_value=mock_context)
    mock_create_engine.return_value = mock_engine

    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )
    linked_service.connect()

    success, message = linked_service.test_connection()
    assert success is False
    assert "failed" in message.lower()


@patch("ds_provider_postgresql_py_lib.linked_service.postgresql.create_engine")
def test_test_connection_returns_false_when_engine_creation_fails(mock_create_engine: MagicMock) -> None:
    """
    It returns False when engine creation fails in test_connection().
    """
    mock_create_engine.return_value = None

    props = PostgreSQLLinkedServiceSettings(uri="postgresql://user:pass@localhost/db")
    linked_service = PostgreSQLLinkedService(
        id=uuid.uuid4(),
        name="test-linked-service",
        version="1.0.0",
        settings=props,
    )

    success, message = linked_service.test_connection()
    assert success is False
    assert "failed to create engine" in message.lower()
