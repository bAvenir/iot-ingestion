"""M2 — pyiceberg SqlCatalog on Postgres, warehouse s3://warehouse/.

Connection only, no table knowledge. Swapping to a REST catalog later is
'type': 'sql' -> 'rest' plus a URI; load_table() calls stay untouched.
"""
