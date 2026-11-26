import time
from collections import OrderedDict

import requests


class TemporaryAPIError(RuntimeError):
    pass


class PermanentAPIError(RuntimeError):
    pass


class MetadataNotFound(PermanentAPIError):
    pass


class PrivateProfileError(PermanentAPIError):
    """User bật chế độ ẩn lịch sử nghe nhạc (Last.fm API error 17)."""


class UserNotFoundError(PermanentAPIError):
    """User không tồn tại trên Last.fm (Last.fm API error 6)."""


def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


class LastFMClient:
    def __init__(self, api_key, interval=0.4, attempts=5, session=None, persistent_cache=None, shared_metadata=False):
        if not api_key:
            raise PermanentAPIError("Set LASTFM_API_KEY in .env first")
        self.api_key = api_key
        self.interval = max(0.0, interval)
        self.attempts = attempts
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "bigdata-lastfm-producer/1.0"})
        self.last_call = 0.0
        self.cache = OrderedDict()
        self.persistent_cache = persistent_cache
        self.shared_metadata = shared_metadata

    def close(self):
        self.session.close()
        if self.persistent_cache is not None:
            try:
                self.persistent_cache.close()
            except Exception:
                pass

    def get(self, method, **params):
        for attempt in range(self.attempts):
            time.sleep(max(0, self.interval - (time.monotonic() - self.last_call)))
            self.last_call = time.monotonic()
            transient = False
            try:
                response = self.session.get(
                    "https://ws.audioscrobbler.com/2.0/",
                    params={"method": method, "api_key": self.api_key, "format": "json", **params},
                    timeout=(5, 30),
                )
                if response.status_code in (408, 429) or response.status_code >= 500:
                    transient = True
                else:
                    payload = response.json()
                    if not isinstance(payload, dict):
                        transient = True
                    else:
                        code = str(payload.get("error", ""))
                        if response.status_code == 200 and not code:
                            return payload
                        if code in {"8", "11", "16", "29"}:
                            transient = True
                        elif code == "17":
                            target_user = params.get("user") or params.get("username") or "unknown"
                            raise PrivateProfileError(
                                f"{method}: user '{target_user}' has private recent tracks (API error 17)"
                            )
                        elif code == "6" and method in {"track.getInfo", "artist.getInfo", "album.getInfo"}:
                            raise MetadataNotFound(f"{method}: metadata unavailable (code 6)")
                        elif code == "6" and method.startswith("user."):
                            target_user = params.get("user") or params.get("username") or "unknown"
                            raise UserNotFoundError(f"{method}: user '{target_user}' not found (code 6)")
                        else:
                            raise PermanentAPIError(f"{method}: API error {code or response.status_code}")
            except (requests.RequestException, ValueError):
                # Requests exceptions may include the URL/API key. Never log or chain them.
                transient = True
            if transient and attempt + 1 < self.attempts:
                time.sleep(min(2 ** attempt, 15))
        raise TemporaryAPIError(f"{method}: temporary failure after {self.attempts} attempts") from None

    def user_profile(self, username):
        if self.persistent_cache is not None:
            cached = self.persistent_cache.get("user.getInfo", {"user": username}, allow_shared=True)
            if cached is not None:
                val, err = cached
                if not err and isinstance(val, dict) and val:
                    return val
        payload = self.get("user.getInfo", user=username)
        profile = payload.get("user")
        if not isinstance(profile, dict):
            raise TemporaryAPIError("user.getInfo: missing user object")
        if self.persistent_cache is not None:
            self.persistent_cache.put("user.getInfo", {"user": username}, profile, None)
        return profile

    def metadata(self, method, root, **params):
        # Lifetime = one job; bounded cache key includes username for user-specific counts.
        key = (method, tuple(sorted(params.items())))
        if key not in self.cache:
            if self.persistent_cache is not None:
                cached = self.persistent_cache.get(method, params, allow_shared=self.shared_metadata)
                if cached is not None:
                    self.cache[key] = cached
                    if len(self.cache) > 512:
                        self.cache.popitem(last=False)
                    self.cache.move_to_end(key)
                    return self.cache[key]
            try:
                payload = self.get(method, **params)
                value = payload.get(root)
                if not isinstance(value, dict):
                    raise TemporaryAPIError(f"{method}: missing {root} object")
                result = (value, None)
            except MetadataNotFound:
                result = ({}, f"{method}:not_found")
            if self.persistent_cache is not None:
                self.persistent_cache.put(method, params, result[0], result[1])
            self.cache[key] = result
            if len(self.cache) > 512:
                self.cache.popitem(last=False)
        self.cache.move_to_end(key)
        return self.cache[key]

    def recent_bulk(self, username, max_pages=3, limit=200, from_ts=None, to_ts=None, max_events=None):
        """Lấy nhiều trang scrobble liên tiếp của một user trong các giờ đã hoàn tất (< to_ts)."""
        page, total_pages = 1, max(1, int(max_pages))
        per_page = max(1, min(200, int(limit)))
        collected = 0
        query_extra = {}
        if from_ts is not None:
            query_extra["from"] = max(0, int(from_ts))
        if to_ts is not None:
            query_extra["to"] = int(to_ts)

        while page <= total_pages:
            payload = self.get(
                "user.getRecentTracks",
                user=username,
                limit=per_page,
                page=page,
                extended=1,
                **query_extra,
            )
            block = payload.get("recenttracks")
            if not isinstance(block, dict) or not isinstance(block.get("@attr"), dict):
                raise TemporaryAPIError("user.getRecentTracks: missing pagination data")
            try:
                api_total_pages = int(block["@attr"].get("totalPages", 1) or 1)
                total_pages = min(int(max_pages), max(1, api_total_pages))
            except (TypeError, ValueError):
                raise TemporaryAPIError("user.getRecentTracks: invalid totalPages") from None
            if "track" not in block and str(block["@attr"].get("total")) != "0":
                raise TemporaryAPIError("user.getRecentTracks: missing track data")

            for record in as_list(block.get("track")):
                if isinstance(record, dict):
                    attrs = record.get("@attr", {})
                    if isinstance(attrs, dict) and str(attrs.get("nowplaying", "")).lower() == "true":
                        continue
                    try:
                        timestamp = int(record["date"]["uts"])
                    except (KeyError, TypeError, ValueError):
                        yield record
                        collected += 1
                        if max_events is not None and collected >= max_events:
                            return
                        continue
                    if from_ts is not None and timestamp < int(from_ts):
                        continue
                    if to_ts is not None and timestamp >= int(to_ts):
                        continue
                yield record
                collected += 1
                if max_events is not None and collected >= max_events:
                    return
            page += 1

    def recent(self, username, start, end):
        page, total_pages = 1, 1
        while page <= total_pages:
            payload = self.get(
                "user.getRecentTracks",
                user=username,
                limit=200,
                page=page,
                extended=1,
                **{"from": max(0, start - 1), "to": end},
            )
            block = payload.get("recenttracks")
            if not isinstance(block, dict) or not isinstance(block.get("@attr"), dict):
                raise TemporaryAPIError("user.getRecentTracks: missing pagination data")
            try:
                total_pages = max(total_pages, int(block["@attr"].get("totalPages", 1)))
            except (TypeError, ValueError):
                raise TemporaryAPIError("user.getRecentTracks: invalid totalPages") from None
            if "track" not in block and str(block["@attr"].get("total")) != "0":
                raise TemporaryAPIError("user.getRecentTracks: missing track data")
            for record in as_list(block.get("track")):
                if isinstance(record, dict):
                    attrs = record.get("@attr", {})
                    if isinstance(attrs, dict) and str(attrs.get("nowplaying", "")).lower() == "true":
                        continue
                    try:
                        timestamp = int(record["date"]["uts"])
                    except (KeyError, TypeError, ValueError):
                        yield record  # Pipeline preserves malformed records in DLQ.
                        continue
                    if not start <= timestamp < end:
                        continue
                yield record
            page += 1

    def latest_completed_trigger(self, username, current_trigger):
        # Select a completed hour containing real scrobbles for a meaningful smoke test.
        for page in range(1, 11):
            payload = self.get(
                "user.getRecentTracks",
                user=username,
                page=page,
                limit=200,
                **{"to": current_trigger},
            )
            block = payload.get("recenttracks")
            if not isinstance(block, dict):
                raise TemporaryAPIError("user.getRecentTracks: missing recenttracks")
            for record in as_list(block.get("track")):
                try:
                    timestamp = int(record["date"]["uts"])
                except (KeyError, TypeError, ValueError):
                    continue
                if 0 < timestamp < current_trigger:
                    return (timestamp // 3600 + 1) * 3600
            if page >= int(block.get("@attr", {}).get("totalPages", 1) or 1):
                break
        raise PermanentAPIError("No scrobbles in a completed hour found; choose another user or --trigger")