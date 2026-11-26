The executable contract is in `producer/schema.py`.

Export JSON Schema for the Consumer developer:

```powershell
docker compose -f docker/compose.producer.yml run --rm --no-deps app python -m batch.producer schema
```

The output is `data/producer/lastfm-event-v1.schema.json` on the host.

The five business groups follow the 25/10/2026 meeting notes. The envelope fields
`event_id`, `schema_version`, `source`, `trigger_time`, `ingested_at`, and
`metadata_errors`, and nullable missing values are proposed ingestion conventions.
Confirm them with the Consumer developer. Metadata counts are snapshots taken at
fetch time, not historical values at scrobble time.

The event ID hashes source username, timestamp in seconds, artist, track, album.
Last.fm has no distinct scrobble ID in this response: two identical plays with
the same second and names cannot be distinguished by this heuristic.
