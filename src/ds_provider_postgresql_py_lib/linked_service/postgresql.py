"""
**File:** ``postgresql.py``
**Region:** ``ds_provider_postgresql_py_lib/linked_service/postgresql``

PostgreSQL Linked Service

This module implements a linked service for PostgreSQL databases.
"""

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from ds_resource_plugin_py_lib.common.resource.linked_service import (
    LinkedService,
    LinkedServiceSettings,
)
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.pool import Pool

from ..enums import ResourceKind


@dataclass(kw_only=True)
class PostgreSQLLinkedServiceSettings(LinkedServiceSettings):
    """
    The object containing the PostgreSQL linked service settings.
    """

    uri: str
    """
    PostgreSQL connection URI.

    Format: postgresql://[user[:password]@][host][:port][/database][?param1=value1&param2=value2]

    Examples:
        postgresql://user:password@localhost:5432/mydb
        postgresql://user:password@localhost:5432/mydb?sslmode=require
        postgresql://user@localhost/mydb
        postgresql://localhost/mydb
    """

    pool_size: int = 5
    """The size of the connection pool. Defaults to 5."""

    max_overflow: int = 10
    """The maximum overflow connections allowed. Defaults to 10."""

    pool_timeout: int = 30
    """The timeout in seconds for getting a connection from the pool. Defaults to 30."""

    pool_recycle: int = 3600
    """The time in seconds after which a connection is recycled. Defaults to 3600."""


PostgreSQLLinkedServiceSettingsType = TypeVar(
    "PostgreSQLLinkedServiceSettingsType",
    bound=PostgreSQLLinkedServiceSettings,
)


@dataclass(kw_only=True)
class PostgreSQLLinkedService(
    LinkedService[PostgreSQLLinkedServiceSettingsType],
    Generic[PostgreSQLLinkedServiceSettingsType],
):
    """
    The class is used to connect with PostgreSQL database.
    """

    settings: PostgreSQLLinkedServiceSettingsType
    _engine: Engine | None = field(default=None, init=False, repr=False)
    """The SQLAlchemy engine instance with connection pool."""

    @property
    def kind(self) -> ResourceKind:
        """
        Get the kind of the linked service.
        Returns:
            ResourceKind
        """
        return ResourceKind.LINKED_SERVICE

    @property
    def engine(self) -> Engine | None:
        """
        Get the SQLAlchemy engine instance.

        Returns:
            Engine | None: The engine if initialized, None otherwise.
        """
        return self._engine

    @property
    def pool(self) -> Pool | None:
        """
        Get the connection pool from the engine.

        The pool is automatically created by SQLAlchemy when create_engine() is called
        with pool parameters. All connections (via engine.connect(), engine.begin(), etc.)
        automatically use this pool.

        Returns:
            Pool | None: The connection pool if the engine is initialized, None otherwise.
        """
        return self._engine.pool if self._engine else None

    def connect(self) -> None:
        """
        Connect to the PostgreSQL database and create a connection pool.

        Returns:
            None
        """
        if self._engine is not None:
            return

        self._engine = create_engine(
            url=self.settings.uri,
            pool_size=self.settings.pool_size,
            max_overflow=self.settings.max_overflow,
            pool_timeout=self.settings.pool_timeout,
            pool_recycle=self.settings.pool_recycle,
        )
        self.log.info("Connection pool created successfully.")

    def test_connection(self) -> tuple[bool, str]:
        """
        Test the connection to the PostgreSQL database.

        Returns:
            tuple[bool, str]: A tuple containing a boolean indicating success and a string message.
        """
        try:
            if self._engine is None:
                self.connect()

            if self._engine is None:
                return False, "Failed to create engine"

            with self._engine.begin() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, f"Connection test failed: {e!s}"

    def close(self) -> None:
        """
        Close the linked service.
        """
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        self.log.info("Linked service closed successfully.")
