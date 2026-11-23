"""Synthetic, explicitly marked fixtures. Never sent to the live topic."""
from .window import parse_trigger

DEMO_TRIGGER = parse_trigger("2026-10-25T08:00:00+07:00")
DEMO_USER = "demo_listener"


class DemoClient:
    def get(self, method, **params):
        return {"user": {"name": DEMO_USER, "country": "Demo", "playcount": "2",
                         "artist_count": "1", "track_count": "2", "registered": {"unixtime": "1700000000"}}}

    def recent(self, username, start, end):
        for index, title in enumerate(("Demo Track A", "Demo Track B")):
            yield {"name": title, "mbid": "", "loved": "0",
                   "artist": {"#text": "Demo Artist", "mbid": ""},
                   "album": {"#text": "Demo Album", "mbid": ""},
                   "date": {"uts": str(start + 60 + index * 180), "#text": "synthetic fixture"}, "image": []}

    def metadata(self, method, root, **params):
        values = {
            "track": {"duration": "180000", "listeners": "1", "playcount": "1", "userplaycount": "1", "toptags": {"tag": []}},
            "artist": {"stats": {"userplaycount": "2"}, "similar": {"artist": []}, "tags": {"tag": []}},
            "album": {"playcount": "2", "userplaycount": "2", "tracks": {"track": []}},
        }
        return values[root], None

    def close(self):
        pass
