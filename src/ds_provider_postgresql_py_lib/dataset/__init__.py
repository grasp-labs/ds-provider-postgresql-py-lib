"""
**File:** ``__init__.py``
**Region:** ``ds_provider_postgresql_py_lib/dataset``

PostgreSQL Dataset

This module implements a dataset for PostgreSQL databases.

Example:
    >>> dataset = PostgreSQLDataset(
    ...     deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
    ...     serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
    ...     typed_properties=PostgreSQLDatasetTypedProperties(
    ...         table="users",
    ...         read=ReadTypedProperties(
    ...             columns=["id", "name"],
    ...             filters={"status": "active"},
    ...             order_by=["created_at"],
    ...         ),
    ...     ),
    ...     linked_service=PostgreSQLLinkedService(
    ...         typed_properties=PostgreSQLLinkedServiceTypedProperties(
    ...             uri="postgresql://user:password@localhost:5432/mydb",
    ...         ),
    ...     ),
    ... )
    >>> dataset.read()
    >>> data = dataset.content
"""

from .postgresql import PostgreSQLDataset, PostgreSQLDatasetTypedProperties

__all__ = [
    "PostgreSQLDataset",
    "PostgreSQLDatasetTypedProperties",
]
