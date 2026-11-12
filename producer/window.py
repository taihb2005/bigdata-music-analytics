from datetime import datetime, timezone


def current_hour():
    """Called in the CLI or Beat process, NEVER to choose a window inside a worker."""
    return int(datetime.now(timezone.utc).timestamp()) // 3600 * 3600


def parse_trigger(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("trigger must include timezone, e.g. 2026-10-25T08:00:00+07:00")
    if dt.minute or dt.second or dt.microsecond:
        raise ValueError("trigger must be an exact hour")
    timestamp = int(dt.timestamp())
    window(timestamp)
    return timestamp


def window(trigger_time):
    if isinstance(trigger_time, bool) or not isinstance(trigger_time, int):
        raise ValueError("trigger_time must be integer Unix seconds")
    if trigger_time < 3600 or trigger_time % 3600:
        raise ValueError("trigger_time must be an exact UTC hour after 1970-01-01")
    return trigger_time - 3600, trigger_time
