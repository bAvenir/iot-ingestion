"""M2 — pyiceberg SqlCatalog on Postgres, warehouse s3://warehouse/.

Connection only, no table knowledge. Swapping to a REST catalog later is
'type': 'sql' -> 'rest' plus a URI; load_table() calls stay untouched.
"""

from functools import lru_cache

from pyiceberg.catalog import load_catalog

from app.config import get_settings


@lru_cache
def get_catalog():
    return load_catalog("warehouse", **get_settings().catalog_properties)
