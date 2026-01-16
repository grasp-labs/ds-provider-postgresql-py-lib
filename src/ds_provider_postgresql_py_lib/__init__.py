"""
**File:** ``__init__.py``
**Region:** ``ds-provider-postgresql-py-lib``

Description
-----------
A Python package from the ds-provider-postgresql-py-lib library.

Example
-------
.. code-block:: python

    from ds_provider_postgresql_py_lib import __version__

    print(f"Package version: {__version__}")
"""

from importlib.metadata import version

from .dataset import PostgreSQLDataset, PostgreSQLDatasetTypedProperties
from .linked_service import PostgreSQLLinkedService, PostgreSQLLinkedServiceTypedProperties

PACKAGE_NAME = "ds-provider-postgresql-py-lib"
__version__ = version(PACKAGE_NAME)

__all__ = [
    "PostgreSQLDataset",
    "PostgreSQLDatasetTypedProperties",
    "PostgreSQLLinkedService",
    "PostgreSQLLinkedServiceTypedProperties",
    "__version__",
]
