"""
**File:** ``__init__.py``
**Region:** ``ds_provider_postgresql_py_lib/dataset``

PostgreSQL Dataset

This module implements a dataset for PostgreSQL databases.

Example:
    >>> dataset = PostgreSQLDataset(
    ...     deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
    ...     serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
    ...     settings=PostgreSQLDatasetSettings(
    ...         table="users",
    ...         read=ReadSettings(
    ...             columns=["id", "name"],
    ...             filters={"status": "active"},
    ...             order_by=["created_at"],
    ...         ),
    ...     ),
    ...     linked_service=PostgreSQLLinkedService(
    ...         settings=PostgreSQLLinkedServiceSettings(
    ...             uri="postgresql://user:password@localhost:5432/mydb",
    ...         ),
    ...     ),
    ... )
    >>> dataset.read()
    >>> data = dataset.output
"""

from .postgresql import PostgreSQLDataset, PostgreSQLDatasetSettings

__all__ = [
    "PostgreSQLDataset",
    "PostgreSQLDatasetSettings",
]
