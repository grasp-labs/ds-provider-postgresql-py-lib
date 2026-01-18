"""
**File:** ``__init__.py``
**Region:** ``ds_provider_postgresql_py_lib/linked_service``

PostgreSQL Linked Service

This module implements a linked service for PostgreSQL databases.

Example:
    >>> linked_service = PostgreSQLLinkedService(
    ...     settings=PostgreSQLLinkedServiceSettings(
    ...         uri="postgresql://user:password@localhost:5432/mydb",
    ...     ),
    ... )
    >>> linked_service.connect()
"""

from .postgresql import PostgreSQLLinkedService, PostgreSQLLinkedServiceSettings

__all__ = [
    "PostgreSQLLinkedService",
    "PostgreSQLLinkedServiceSettings",
]
