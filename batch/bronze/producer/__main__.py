import argparse
import json
import sys
from pathlib import Path

from batch.common.config import _parse_user_statuses

from .config import Settings, load_users_from_file
from .demo import DEMO_TRIGGER, DEMO_USER
from .lastfm import LastFMClient
from .pipeline import publish_run, run_batch_window, run_bulk_backfill, run_window, sync_runs_to_bronze
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
        command.add_argument("--user", help="Defaults to all users from LASTFM_USERS_FILE (or LASTFM_USERS)")
        command.add_argument("--users-file", help="Path to JSON/text users file (overrides LASTFM_USERS_FILE)")
        command.add_argument("--statuses", help="Comma-separated statuses to filter or 'all'")
        group = command.add_mutually_exclusive_group()
        group.add_argument("--trigger", help="ISO timestamp with timezone at an exact hour")
        group.add_argument("--latest", action="store_true", help="Pick a completed hour with this user's recent scrobbles")
        if name == "run":
            command.add_argument("--dry-run", action="store_true", help="Save local JSONL, do not send to Kafka")
        else:
            command.add_argument("--demo", action="store_true")
            command.add_argument("--wait", action="store_true", help="Wait up to 60 seconds for the task result")
            command.add_argument(
                "--dispatch-all",
                action="store_true",
                help="Dispatch all users via batch.bronze.producer.enqueue_hourly_users",
            )
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--demo", action="store_true")
    inspect.add_argument("--count", type=int, default=2)
    inspect.add_argument("--timeout", type=int, default=20)
    replay = sub.add_parser("publish", help="Replay an extracted run from its saved JSONL files")
    replay.add_argument("--run-dir", required=True)

    bulk = sub.add_parser("bulk", help="Bulk fetch multi-page recent scrobbles across multiple users into Bronze")
    bulk.add_argument("--users", help="Comma-separated usernames (overrides --users-file)")
    bulk.add_argument("--users-file", default="dataset/users/showoffs_all_users.json")
    bulk.add_argument("--statuses", default="A", help="Comma-separated user statuses (e.g. A) or 'all'")
    bulk.add_argument("--max-users", type=int, default=10)
    bulk.add_argument("--pages-per-user", type=int, default=2)
    bulk.add_argument("--max-events-per-user", type=int, default=120)
    bulk.add_argument("--no-shared-metadata", action="store_true", help="Disable cross-user metadata cache sharing")
    bulk.add_argument("--publish", action="store_true", help="Publish extracted runs to Kafka")
    bulk.add_argument("--no-sync-minio", action="store_true", help="Do not pack runs into .jsonl.gz Bronze storage")
    bulk.add_argument("--force", action="store_true", help="Re-fetch even if (user, trigger_time) already exists")

    sync_cmd = sub.add_parser("sync-bronze", help="Pack extracted producer runs into .jsonl.gz files in Bronze storage")
    sync_cmd.add_argument("--runs-dir", help="Path to producer runs directory")
    sync_cmd.add_argument("--bucket", default="bronze")
    sync_cmd.add_argument("--prefix", default="events")
    sync_cmd.add_argument("--force", action="store_true")

    args = parser.parse_args()
    settings = Settings()
    if args.command == "check-config":
        resolved_users = settings.resolve_users()
        users_file_display = (
            "" if str(settings.users_file).strip() in ("", ".") else str(settings.users_file)
        )
        print_json({
            "api_key_present": bool(settings.api_key.strip()),
            "users_file": users_file_display,
            "user_statuses": list(settings.user_statuses) if settings.user_statuses else ["all"],
            "user_count": len(resolved_users),
            "users_sample": list(resolved_users[:10]),
            "kafka_bootstrap_servers": settings.bootstrap,
            "topic": settings.topic,
            "schedule": "minute 0 of every hour (single batch across up to 4 workers with immediate per-user Kafka push); minute 30 bronze->silver (previous hour); UTC",
        })
        if not settings.api_key.strip() or not resolved_users:
            raise ValueError("Fill LASTFM_API_KEY and LASTFM_USERS_FILE (or LASTFM_USERS) in .env, then rerun this command")
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
    elif args.command == "sync-bronze":
        print_json(
            sync_runs_to_bronze(
                runs_dir=args.runs_dir,
                bucket=args.bucket,
                prefix=args.prefix,
                force=args.force,
            )
        )
    elif args.command == "bulk":
        raw_statuses = (args.statuses or "").strip()
        statuses = (
            None
            if raw_statuses.lower() == "all"
            else {s.strip() for s in raw_statuses.split(",") if s.strip()}
        )
        explicit_users = (
            [u.strip() for u in args.users.split(",") if u.strip()]
            if args.users
            else None
        )
        print_json(
            run_bulk_backfill(
                usernames=explicit_users,
                users_file=args.users_file,
                statuses=statuses,
                max_users=args.max_users,
                pages_per_user=args.pages_per_user,
                max_events_per_user=args.max_events_per_user,
                shared_metadata=not args.no_shared_metadata,
                publish=args.publish,
                sync_minio=not args.no_sync_minio,
                force=args.force,
                settings=settings,
            )
        )
    else:
        is_demo = getattr(args, "demo", False)
        trigger = DEMO_TRIGGER if is_demo else (parse_trigger(args.trigger) if args.trigger else current_hour())
        if (
            args.command == "enqueue"
            and not is_demo
            and not args.user
            and not args.latest
        ):
            from batch.scheduler.tasks import enqueue_hourly_users
            result = enqueue_hourly_users.delay(
                trigger_time=trigger,
                demo=False,
                users_file=args.users_file,
                statuses=args.statuses,
            )
            print_json({"task_id": result.id, "trigger_time": trigger, "batch": True})
            if args.wait:
                print_json(result.get(timeout=600))
            return

        if is_demo:
            users = [DEMO_USER]
        elif args.user:
            users = [args.user]
        elif args.users_file or args.statuses is not None:
            target_file = args.users_file or settings.users_file
            target_statuses = (
                _parse_user_statuses(args.statuses)
                if args.statuses is not None
                else settings.user_statuses
            )
            users = load_users_from_file(target_file, target_statuses)
        else:
            users = settings.resolve_users()

        if not users:
            parser.error("No users found; set LASTFM_USERS_FILE / LASTFM_USERS or pass --user YOUR_USERNAME")
        if args.command == "run" and len(users) > 1 and not args.latest and not is_demo:
            print_json(run_batch_window(users, trigger, publish=not args.dry_run, demo=False, settings=settings))
            return
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
                from batch.scheduler.tasks import ingest_user_window
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
