#!/usr/bin/env python3
"""
BRAD'S SIGNALS GUARDIAN
G1.0 - Observer-only safety foundation

Purpose:
- Run as a completely separate Railway service.
- Observe shared PostgreSQL and Redis health.
- Never place trades.
- Never send signals.
- Never modify Bot 2.0.
- Never write to PostgreSQL or Redis in G1.0.

Later Guardian stages can add:
- scanner freshness monitoring
- database/Redis outage detection
- duplicate/stuck-process detection
- AI behaviour anomaly detection
- confidence/distribution drift detection
- Telegram alerts
- protected emergency pause

Those later actions are NOT enabled here.
"""

import os
import time
from datetime import datetime, timezone

import psycopg2
import redis

GUARDIAN_VERSION = "G1.0-OBSERVER-ONLY-FOUNDATION"

DATABASE_WRITES = False
REDIS_WRITES = False
TELEGRAM_SENDING = False
TRADE_EXECUTION = False
PRODUCTION_MODIFICATION = False
EMERGENCY_PAUSE = False

CHECK_INTERVAL_SECONDS = int(os.environ.get("GUARDIAN_CHECK_INTERVAL_SECONDS", "60"))

SEP = "=" * 100


def section(title):
    print("\n" + SEP, flush=True)
    print(title, flush=True)
    print(SEP, flush=True)


def utc_now():
    return datetime.now(timezone.utc)


def database_health():
    url = os.environ.get("SIGNALS2_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        return False, "DATABASE URL NOT CONFIGURED"

    conn = None
    try:
        conn = psycopg2.connect(url, connect_timeout=10)
        conn.set_session(readonly=True, autocommit=False)

        with conn.cursor() as cur:
            cur.execute("SELECT NOW(), COUNT(*) FROM public.signals2_opportunities")
            db_time, opportunity_count = cur.fetchone()

        conn.rollback()
        return True, f"READY | READ ONLY | opportunities={opportunity_count} | db_time={db_time}"

    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def redis_health():
    url = os.environ.get("SIGNALS2_REDIS_URL") or os.environ.get("REDIS_URL")
    if not url:
        return False, "REDIS URL NOT CONFIGURED"

    try:
        client = redis.Redis.from_url(
            url,
            socket_connect_timeout=10,
            socket_timeout=10,
            decode_responses=True,
        )
        ok = client.ping()
        if ok:
            return True, "READY | PING OK | NO GUARDIAN WRITES"
        return False, "PING RETURNED FALSE"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def safety_check():
    unsafe = any(
        (
            DATABASE_WRITES,
            REDIS_WRITES,
            TELEGRAM_SENDING,
            TRADE_EXECUTION,
            PRODUCTION_MODIFICATION,
            EMERGENCY_PAUSE,
        )
    )
    if unsafe:
        raise RuntimeError("GUARDIAN SAFETY FLAGS INVALID")


def print_banner():
    section("BRAD'S SIGNALS GUARDIAN")
    print(f"GUARDIAN VERSION: {GUARDIAN_VERSION}", flush=True)
    print(f"UTC START: {utc_now().isoformat()}", flush=True)
    print(f"DATABASE WRITES: {DATABASE_WRITES}", flush=True)
    print(f"REDIS WRITES: {REDIS_WRITES}", flush=True)
    print(f"TELEGRAM SENDING: {TELEGRAM_SENDING}", flush=True)
    print(f"TRADE EXECUTION: {TRADE_EXECUTION}", flush=True)
    print(f"PRODUCTION MODIFICATION: {PRODUCTION_MODIFICATION}", flush=True)
    print(f"EMERGENCY PAUSE: {EMERGENCY_PAUSE}", flush=True)
    print(f"CHECK INTERVAL: {CHECK_INTERVAL_SECONDS}s", flush=True)
    print("SAFETY CHECK: PASS", flush=True)


def run_cycle(cycle):
    started = time.monotonic()

    db_ok, db_message = database_health()
    redis_ok, redis_message = redis_health()

    overall = "HEALTHY" if db_ok and redis_ok else "WARNING"

    section(f"GUARDIAN CYCLE {cycle}")
    print(f"UTC: {utc_now().isoformat()}", flush=True)
    print(f"POSTGRES: {'HEALTHY' if db_ok else 'WARNING'} | {db_message}", flush=True)
    print(f"REDIS: {'HEALTHY' if redis_ok else 'WARNING'} | {redis_message}", flush=True)
    print(f"GUARDIAN STATUS: {overall}", flush=True)
    print("ACTION: OBSERVE ONLY", flush=True)
    print("NO SENDS; NO TRADES; NO PRODUCTION CHANGES", flush=True)
    print(f"CYCLE DURATION: {time.monotonic() - started:.2f}s", flush=True)


def main():
    safety_check()
    print_banner()

    cycle = 0

    while True:
        cycle += 1

        try:
            run_cycle(cycle)
        except Exception as exc:
            section(f"GUARDIAN CYCLE {cycle} ERROR")
            print(f"ERROR TYPE: {type(exc).__name__}", flush=True)
            print(f"ERROR: {exc}", flush=True)
            print("GUARDIAN REMAINS OBSERVER ONLY", flush=True)
            print("NO SENDS; NO TRADES; NO PRODUCTION CHANGES", flush=True)

        print(f"GUARDIAN: WAIT {CHECK_INTERVAL_SECONDS}s", flush=True)
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
