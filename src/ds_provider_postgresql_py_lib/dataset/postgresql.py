"""
**File:** ``postgresql.py``
**Region:** ``ds_provider_postgresql_py_lib/dataset/postgresql``

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
    ...             limit=100,
    ...         ),
    ...         create=CreateTypedProperties(
    ...             mode="replace",
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

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, NoReturn, TypeVar, cast

import pandas as pd
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetStorageFormatType,
    DatasetTypedProperties,
    TabularDataset,
)
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError, WriteError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import ConnectionError
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    and_,
    asc,
    desc,
    quoted_name,
    select,
)
from sqlalchemy.sql import Select

from ..enums import ResourceKind
from ..linked_service.postgresql import PostgreSQLLinkedService


@dataclass(kw_only=True)
class CreateTypedProperties:
    """
    Properties specific to the create() operation.

    These properties only apply when writing data to the database
    and do not affect read(), delete(), update(), or rename() operations.
    """

    mode: Literal["append", "replace", "delete_rows"] = "append"
    """
    Write mode for the data.

    Options:
    - "append": Insert new rows (default). Creates table if it doesn't exist.
    - "replace": Drop table if exists, recreate, then insert.
    - "delete_rows": Delete all rows before inserting new values.
    """

    index: bool = False
    """
    Whether to include the index in the output.
    """


@dataclass(kw_only=True)
class ReadTypedProperties:
    """
    Properties specific to the read() operation.

    These properties only apply when reading data from the database
    and do not affect create(), delete(), update(), or rename() operations.
    """

    limit: int | None = None
    """The limit of the data to read."""

    columns: Sequence[str] | None = None
    """
    Specific columns to select. If None, selects all columns (*).

    Example:
        columns=["id", "name", "created_at"]
    """

    filters: dict[str, Any] | None = None
    """
    Dictionary of column filters for WHERE clause. Uses equality comparison.

    Example:
        filters={"status": "active", "amount": 100}

    Multiple filters are combined with AND.
    """

    order_by: Sequence[str | tuple[str, str]] | None = None
    """
    Columns to order by. Can be:
    - List of column names (defaults to ascending)
    - List of (column_name, 'asc'/'desc') tuples

    Example:
        order_by=["created_at"]  # ascending
        order_by=[("created_at", "desc"), "name"]  # created_at desc, name asc
    """


@dataclass(kw_only=True)
class PostgreSQLDatasetTypedProperties(DatasetTypedProperties):
    """
    Properties for PostgreSQL dataset operations.

    The `read` property contains read-specific configuration that only
    applies to the read() method, not to create(), delete(), update(), etc.
    """

    schema: str = "public"
    """The schema to read from."""

    table: str
    """The table to read from."""

    read: ReadTypedProperties | None = None
    """
    Read-specific properties. Only applies to the read() method.

    If None, read() will use default behavior (all columns, no filters, no ordering, no limit).
    """

    create: CreateTypedProperties | None = None
    """
    Create-specific properties. Only applies to the create() method.

    If None, create() will use default behavior (append mode, creates table if needed).
    """


PostgreSQLDatasetTypedPropertiesType = TypeVar(
    "PostgreSQLDatasetTypedPropertiesType",
    bound=PostgreSQLDatasetTypedProperties,
)
PostgreSQLLinkedServiceType = TypeVar(
    "PostgreSQLLinkedServiceType",
    bound=PostgreSQLLinkedService[Any],
)


@dataclass(kw_only=True)
class PostgreSQLDataset(
    TabularDataset[
        PostgreSQLLinkedServiceType,
        PostgreSQLDatasetTypedPropertiesType,
        PandasSerializer,
        PandasDeserializer,
    ],
    Generic[PostgreSQLLinkedServiceType, PostgreSQLDatasetTypedPropertiesType],
):
    linked_service: PostgreSQLLinkedServiceType
    typed_properties: PostgreSQLDatasetTypedPropertiesType

    serializer: PandasSerializer | None = field(
        default_factory=lambda: PandasSerializer(format=DatasetStorageFormatType.JSON),
    )
    deserializer: PandasDeserializer | None = field(
        default_factory=lambda: PandasDeserializer(format=DatasetStorageFormatType.JSON),
    )

    @property
    def kind(self) -> ResourceKind:
        return ResourceKind.DATASET

    def create(self, **_kwargs: Any) -> None:
        """
        Create/write data to the specified table.

        Writes self.content (pandas DataFrame) to the database table using the
        configured create properties (mode, etc.).

        Args:
            _kwargs: Additional keyword arguments to pass to the request.

        Raises:
            ConnectionError: If the connection fails.
            WriteError: If the write operation fails.
        """
        create_props = self.typed_properties.create or CreateTypedProperties()

        if self.linked_service.engine is None:
            raise ConnectionError(message="Connection pool is not initialized.")

        if self.content is None or self.content.empty:
            raise WriteError(
                message="Content is empty or None.",
                status_code=400,
                details={
                    "table": self.typed_properties.table,
                    "schema": self.typed_properties.schema,
                    "settings": self.typed_properties.create,
                },
            )

        try:
            self.content.to_sql(
                name=self.typed_properties.table,
                con=self.linked_service.engine,
                schema=self.typed_properties.schema,
                if_exists=create_props.mode,
                index=create_props.index,
                dtype=cast("Any", self._pandas_dtype_to_sqlalchemy(self.content.dtypes)),
            )
        except Exception as exc:
            raise WriteError(
                message=f"Failed to write data to table: {exc!s}",
                status_code=500,
                details={
                    "table": self.typed_properties.table,
                    "schema": self.typed_properties.schema,
                    "settings": create_props,
                },
            ) from exc

    def read(self, **_kwargs: Any) -> None:
        """
        Read data from the specified endpoint.

        Args:
            _kwargs: Additional keyword arguments to pass to the request.

        Raises:
            ConnectionError: If the connection fails.
            ValueError: If specified columns, filters, or order_by columns don't exist.
            ReadError: If the read operation fails.
        """
        if self.linked_service.engine is None:
            raise ConnectionError(message="Connection pool is not initialized.")

        table = self._get_table()
        read_props = self.typed_properties.read

        stmt = self._build_select_columns(table, read_props)
        stmt = self._build_filters(stmt, table, read_props)
        stmt = self._build_order_by(stmt, table, read_props)

        if read_props and read_props.limit is not None:
            stmt = stmt.limit(read_props.limit)

        self.log.debug(f"Executing query: {stmt}")
        try:
            chunks = pd.read_sql(
                stmt,
                con=self.linked_service.engine,
                chunksize=100_000,
                dtype_backend="pyarrow",
            )
            self.content = pd.concat(list(chunks), ignore_index=True)
            self._set_schema(self.content)
            self.next = False
        except Exception as exc:
            raise ReadError(
                message=f"Failed to read data from table: {exc!s}",
                status_code=500,
                details={
                    "table": self.typed_properties.table,
                    "schema": self.typed_properties.schema,
                    "query": stmt,
                    "settings": read_props,
                },
            ) from exc

    def delete(self, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("Delete operation is not supported for PostgreSQL datasets")

    def update(self, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("Update operation is not supported for PostgreSQL datasets")

    def rename(self, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("Rename operation is not supported for PostgreSQL datasets")

    def _set_schema(self, content: pd.DataFrame) -> None:
        """
        Set the schema from the content.

        Args:
            content: The content to set the schema from.
        """
        converted = content.convert_dtypes(dtype_backend="pyarrow")
        self.schema = {str(col): str(dtype) for col, dtype in converted.dtypes.to_dict().items()}

    def _get_table(self) -> Table:
        """
        Get the SQLAlchemy Table object for the configured schema and table.

        Args:
            None

        Returns:
            Table: The SQLAlchemy Table object.
        """
        schema_name = quoted_name(self.typed_properties.schema, quote=True)
        table_name = quoted_name(self.typed_properties.table, quote=True)

        metadata = MetaData(schema=schema_name)

        return Table(
            table_name,
            metadata,
            schema=schema_name,
            autoload_with=self.linked_service.engine,
        )

    def _pandas_dtype_to_sqlalchemy(self, dtypes: pd.Series) -> dict[str, Any]:
        """
        Convert pandas dtypes Series to a dict mapping column names to SQLAlchemy types.

        Args:
            dtypes: Pandas Series where index is column names and values are dtypes.

        Returns:
            dict[str, Any]: Dictionary mapping column names to SQLAlchemy types.
        """
        dtype_map: dict[str, Any] = {}

        for col_name, dtype in dtypes.items():
            col_name_str = str(col_name)

            if pd.api.types.is_integer_dtype(dtype):
                if hasattr(dtype, "itemsize") and dtype.itemsize <= 2:
                    dtype_map[col_name_str] = Integer()
                else:
                    dtype_map[col_name_str] = BigInteger()
            elif pd.api.types.is_float_dtype(dtype):
                dtype_map[col_name_str] = Float()
            elif pd.api.types.is_bool_dtype(dtype):
                dtype_map[col_name_str] = Boolean()
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                dtype_map[col_name_str] = DateTime()
            elif pd.api.types.is_string_dtype(dtype) or isinstance(dtype, pd.CategoricalDtype):
                dtype_map[col_name_str] = String(length=255)
            else:
                dtype_map[col_name_str] = String(length=255)

        return dtype_map

    def _validate_column(self, table: Table, column_name: str) -> None:
        """
        Validate that a column exists in the table.

        Args:
            table: The SQLAlchemy Table object.
            column_name: The name of the column to validate.

        Raises:
            ValueError: If the column doesn't exist in the table.
        """
        if column_name not in table.c:
            available_columns = list(table.c.keys())
            raise ValueError(
                f"Column '{column_name}' not found in table '{self.typed_properties.table}'. "
                f"Available columns: {available_columns}"
            )

    def _build_select_columns(self, table: Table, read_props: ReadTypedProperties | None) -> Select[Any]:
        """
        Build the SELECT clause of the query.

        Args:
            table: The SQLAlchemy Table object.
            read_props: Read-specific properties.

        Returns:
            Select: The SELECT statement with specified columns or all columns.

        Raises:
            ValueError: If any specified column doesn't exist in the table.
        """
        if read_props and read_props.columns:
            for col_name in read_props.columns:
                self._validate_column(table, col_name)

            selected_columns = [table.c[col_name] for col_name in read_props.columns]
            return select(*selected_columns)

        return select(table)

    def _build_filters(self, stmt: Select[Any], table: Table, read_props: ReadTypedProperties | None) -> Select[Any]:
        """
        Build the WHERE clause of the query from filters.

        Args:
            stmt: The current SELECT statement.
            table: The SQLAlchemy Table object.
            read_props: Read-specific properties.

        Returns:
            Select: The SELECT statement with WHERE clause applied.

        Raises:
            ValueError: If any filter column doesn't exist in the table.
        """
        if not read_props or not read_props.filters:
            return stmt

        for col_name in read_props.filters:
            self._validate_column(table, col_name)

        filter_conditions = [table.c[col_name] == value for col_name, value in read_props.filters.items()]

        return stmt.where(and_(*filter_conditions))

    def _build_order_by(self, stmt: Select[Any], table: Table, read_props: ReadTypedProperties | None) -> Select[Any]:
        """
        Build the ORDER BY clause of the query.

        Args:
            stmt: The current SELECT statement.
            table: The SQLAlchemy Table object.
            read_props: Read-specific properties.

        Returns:
            Select: The SELECT statement with ORDER BY clause applied.

        Raises:
            ValueError: If any order_by column doesn't exist in the table.
        """
        if not read_props or not read_props.order_by:
            return stmt

        order_clauses = []
        for order_spec in read_props.order_by:
            if isinstance(order_spec, tuple):
                col_name, direction = order_spec
                self._validate_column(table, col_name)

                col = table.c[col_name]
                if direction.lower() == "desc":
                    order_clauses.append(desc(col))
                else:
                    order_clauses.append(asc(col))
            else:
                self._validate_column(table, order_spec)
                order_clauses.append(asc(table.c[order_spec]))

        return stmt.order_by(*order_clauses)
