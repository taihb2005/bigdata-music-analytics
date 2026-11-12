import argparse
import json
import sys
from pathlib import Path

from .config import Settings
from .demo import DEMO_TRIGGER, DEMO_USER
from .lastfm import LastFMClient
from .pipeline import publish_run, run_window
from .schema import SCHEMA
from .window import current_hour, parse_trigger


def print_json(value):
    print(json.dumps(value, ensure_ascii=False, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description="Last.fm Producer: manual run, Celery task, and isolated synthetic demo")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-topics")
    sub.add_parser("schema")
    sub.add_parser("check-config", help="Check required settings without printing the API key")
    demo_parser = sub.add_parser("demo", help="2 synthetic events; no API key required")
    demo_parser.add_argument("--publish", action="store_true")
    for name in ("run", "enqueue"):
        command = sub.add_parser(name)
        command.add_argument("--user", help="Defaults to LASTFM_USERS from .env")
        group = command.add_mutually_exclusive_group()
        group.add_argument("--trigger", help="ISO timestamp with timezone at an exact hour")
        group.add_argument("--latest", action="store_true", help="Pick a completed hour with this user's recent scrobbles")
        if name == "run":
            command.add_argument("--dry-run", action="store_true", help="Save local JSONL, do not send to Kafka")
        else:
            command.add_argument("--demo", action="store_true")
            command.add_argument("--wait", action="store_true", help="Wait up to 60 seconds for the task result")
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--demo", action="store_true")
    inspect.add_argument("--count", type=int, default=2)
    inspect.add_argument("--timeout", type=int, default=20)
    replay = sub.add_parser("publish", help="Replay an extracted run from its saved JSONL files")
    replay.add_argument("--run-dir", required=True)
    args = parser.parse_args()
    settings = Settings()
    if args.command == "check-config":
        print_json({"api_key_present": bool(settings.api_key.strip()), "users": list(settings.users),
                    "kafka_bootstrap_servers": settings.bootstrap, "topic": settings.topic,
                    "schedule": "minute 0 of every hour; UTC; worker uses serialized trigger_time"})
        if not settings.api_key.strip() or not settings.users:
            raise ValueError("Fill LASTFM_API_KEY and LASTFM_USERS in .env, then rerun this command")
    elif args.command == "schema":
        settings.output.mkdir(parents=True, exist_ok=True)
        target = settings.output / "lastfm-event-v1.schema.json"
        target.write_text(json.dumps(SCHEMA, indent=2), encoding="utf-8")
        print(f"Schema: {target}")
    elif args.command == "init-topics":
        from .kafka import init_topics
        init_topics(settings)
    elif args.command == "inspect":
        from .kafka import inspect_events
        if args.count < 1 or args.timeout < 1:
            parser.error("count and timeout must be positive")
        inspect_events(settings, args.demo, args.count, args.timeout)
    elif args.command == "demo":
        print_json(run_window(DEMO_USER, DEMO_TRIGGER, args.publish, demo=True, settings=settings))
    elif args.command == "publish":
        print_json(publish_run(Path(args.run_dir), settings))
    else:
        is_demo = getattr(args, "demo", False)
        users = [DEMO_USER] if is_demo else ([args.user] if args.user else settings.users)
        if not users:
            parser.error("Set LASTFM_USERS in .env or pass --user YOUR_USERNAME")
        trigger = DEMO_TRIGGER if is_demo else (parse_trigger(args.trigger) if args.trigger else current_hour())
        for username in users:
            user_trigger = trigger
            if args.latest and not is_demo:
                client = LastFMClient(settings.api_key, settings.interval)
                try:
                    user_trigger = client.latest_completed_trigger(username, current_hour())
                finally:
                    client.close()
            if args.command == "run":
                print_json(run_window(username, user_trigger, not args.dry_run, settings=settings))
            else:
                from .tasks import ingest_user_window
                result = ingest_user_window.delay(username=username, trigger_time=user_trigger, demo=is_demo)
                print_json({"task_id": result.id, "username": username, "trigger_time": user_trigger})
                if args.wait:
                    print_json(result.get(timeout=60))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # LastFMClient sanitizes all network exceptions before they reach this boundary.
        print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
