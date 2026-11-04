"""archive.s1d.me — a structured, versioned archive of the Indian republic's public data.

Capture is generic and universal; understanding (parsing, entity resolution) layers on top and never blocks capture.
See PLAN.md. Modules: config, store (content-addressed B2), db (Postgres index), text (extraction), ingest (the one
door for captured content), api (HTTP ingest for pool workers), cli.
"""
