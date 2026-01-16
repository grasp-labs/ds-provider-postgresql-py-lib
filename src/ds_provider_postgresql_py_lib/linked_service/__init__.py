"""
**File:** ``__init__.py``
**Region:** ``ds_provider_postgresql_py_lib/linked_service``

PostgreSQL Linked Service

This module implements a linked service for PostgreSQL databases.

Example:
    >>> linked_service = PostgreSQLLinkedService(
    ...     typed_properties=PostgreSQLLinkedServiceTypedProperties(
    ...         host="https://api.example.com",
    ...     ),
    ... )
    >>> linked_service.connect()
"""

from .postgresql import PostgreSQLLinkedService, PostgreSQLLinkedServiceTypedProperties

__all__ = [
    "PostgreSQLLinkedService",
    "PostgreSQLLinkedServiceTypedProperties",
]
