"""
**File:** ``enums.py``
**Region:** ``ds_provider_postgresql_py_lib/enums``

Constants for PostgreSQL provider.

Example:
    >>> ResourceKind.LINKED_SERVICE
    'DS.RESOURCE.LINKED_SERVICE.POSTGRESQL'
    >>> ResourceKind.DATASET
    'DS.RESOURCE.DATASET.POSTGRESQL'
"""

from enum import StrEnum


class ResourceKind(StrEnum):
    """
    Constants for PostgreSQL provider.
    """

    LINKED_SERVICE = "DS.RESOURCE.LINKED_SERVICE.POSTGRESQL"
    DATASET = "DS.RESOURCE.DATASET.POSTGRESQL"
