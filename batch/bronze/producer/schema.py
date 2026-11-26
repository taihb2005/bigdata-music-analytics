"""Contract: the five groups from the meeting, plus explicitly added ingestion fields."""
import hashlib
import json
from datetime import datetime, timezone

from jsonschema import Draft202012Validator

from .lastfm import as_list


STRING = {"type": ["string", "null"]}
OBJECT = {"type": ["object", "null"]}
ARRAY = {"type": "array", "items": {"type": "object"}}


def shape(fields):
    return {"type": "object", "properties": fields, "required": list(fields), "additionalProperties": False}


SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Last.fm batch event, meeting 2026-10-25, ingestion contract v1",
    **shape({
        "schema_version": {"const": "1.0"},
        "event_id": {"type": "string", "pattern": "^lastfm:[0-9a-f]{64}$"},
        "source": {"enum": ["lastfm", "demo"]},
        "trigger_time": {"type": "integer"},
        "ingested_at": {"type": "string"},
        "metadata_errors": {"type": "array", "items": {"type": "string"}},
        "scrobble_event": shape({
            "name": {"type": "string", "minLength": 1}, "mbid": STRING,
            "loved": {"enum": ["0", "1", None]},
            "date": shape({"uts": {"type": "string", "pattern": "^[0-9]+$"}, "text": STRING}),
            "image": ARRAY,
            "artist": shape({"name": {"type": "string", "minLength": 1}, "mbid": STRING}),
            "album": shape({"name": STRING, "mbid": STRING}),
        }),
        "user_profile": shape({
            "id": STRING, "name": {"type": "string", "minLength": 1}, "realname": STRING,
            "country": STRING, "playcount": STRING, "artist_count": STRING,
            "track_count": STRING, "registered": OBJECT,
        }),
        "track_metadata": shape({
            "duration": STRING, "listeners": STRING, "playcount": STRING,
            "userplaycount": STRING, "toptags": shape({"tag": ARRAY}), "wiki": OBJECT,
        }),
        "artist_metadata": shape({
            "stats": shape({"userplaycount": STRING}), "similar": shape({"artist": ARRAY}),
            "tags": shape({"tag": ARRAY}), "bio": OBJECT,
        }),
        "album_metadata": shape({
            "playcount": STRING, "userplaycount": STRING,
            "tracks": shape({"track": ARRAY}), "wiki": OBJECT,
        }),
    }),
}
VALIDATOR = Draft202012Validator(SCHEMA)


def text(value):
    return None if value is None else str(value)


def object_or_none(value):
    return value if isinstance(value, dict) else None


def identity(value):
    if isinstance(value, dict):
        return {"name": text(value.get("name", value.get("#text"))), "mbid": text(value.get("mbid"))}
    return {"name": text(value), "mbid": None}


def nested_list(value, field):
    return as_list(value.get(field)) if isinstance(value, dict) else []


def event_identity(username, raw):
    if not isinstance(raw, dict):
        raise ValueError("scrobble must be an object")
    artist, album = identity(raw.get("artist")), identity(raw.get("album"))
    if not isinstance(raw.get("name"), str) or not raw["name"].strip() or not artist["name"]:
        raise ValueError("missing track or artist name")
    try:
        timestamp = int(raw["date"]["uts"])
        if timestamp < 0:
            raise ValueError()
    except (KeyError, TypeError, ValueError):
        raise ValueError("missing or invalid scrobble timestamp") from None
    # Source has no stable scrobble ID: this is a documented identity heuristic.
    parts = [username.casefold(), timestamp, artist["name"], raw["name"], album["name"]]
    digest = hashlib.sha256(json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    return "lastfm:" + digest


def normalize(username, raw, profile, track, artist, album, trigger, errors=(), source="lastfm"):
    event_id = event_identity(username, raw)
    profile_fields = ("id", "realname", "country", "playcount", "artist_count", "track_count")
    event = {
        "schema_version": "1.0", "event_id": event_id, "source": source,
        "trigger_time": trigger, "ingested_at": datetime.now(timezone.utc).isoformat(),
        "metadata_errors": list(errors),
        "scrobble_event": {
            "name": raw["name"], "mbid": text(raw.get("mbid")), "loved": text(raw.get("loved")),
            "date": {"uts": str(int(raw["date"]["uts"])), "text": text(raw["date"].get("#text", raw["date"].get("text")))},
            "image": as_list(raw.get("image")), "artist": identity(raw.get("artist")), "album": identity(raw.get("album")),
        },
        "user_profile": {**{k: text(profile.get(k)) for k in profile_fields},
                         "name": username, "registered": object_or_none(profile.get("registered"))},
        "track_metadata": {
            **{k: text(track.get(k)) for k in ("duration", "listeners", "playcount", "userplaycount")},
            "toptags": {"tag": nested_list(track.get("toptags"), "tag")}, "wiki": object_or_none(track.get("wiki")),
        },
        "artist_metadata": {
            "stats": {"userplaycount": text((artist.get("stats") or {}).get("userplaycount"))},
            "similar": {"artist": nested_list(artist.get("similar"), "artist")},
            "tags": {"tag": nested_list(artist.get("tags"), "tag")}, "bio": object_or_none(artist.get("bio")),
        },
        "album_metadata": {
            "playcount": text(album.get("playcount")), "userplaycount": text(album.get("userplaycount")),
            "tracks": {"track": nested_list(album.get("tracks"), "track")}, "wiki": object_or_none(album.get("wiki")),
        },
    }
    VALIDATOR.validate(event)
    return event
