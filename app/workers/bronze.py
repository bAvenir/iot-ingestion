"""M2 — Bronze worker.

XREADGROUP loop, batch to N messages or T seconds. Branch once on payload.mode
(inline = in hand, reference = fetch from landing/); identical after that.
Normalise the envelope (ts/timestamp/time -> event_time, fall back to
received_at), compute schema_fp, leave payload an unparsed JSON string.
One table.append() per batch, then XACK. Retry with attempt++, dead-letter past 3.
"""
