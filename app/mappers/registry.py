"""M3 — Load and validate every YAML in mappers/defs/ at startup.

Index by id and by fingerprint. An invalid mapper must fail loudly on startup,
not on first use.
"""
