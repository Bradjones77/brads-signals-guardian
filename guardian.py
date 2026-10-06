#!/usr/bin/env python3
"""
BRAD'S SIGNALS GUARDIAN
G1.1 - Bot activity monitor

Observer-only:
- PostgreSQL SELECTs only, session forced read-only.
- Redis PING only.
- No Telegram.
- No trades.
- No production modification.
- No emergency pause.

G1.1 adds:
- latest opportunity timestamp monitoring
- database activity/freshness monitoring
- stale-data detection
- opportunity flow monitoring between Guardian cycles
"""

import os
import time
from datetime import datetime, timezone

import psycopg2
import redis

GUARDIAN_VERSION = "G1.1-BOT-ACTIVITY-MONITOR"

DATABASE_WRITES = False
REDIS_WRITES = False
TELEGRAM_SENDING = False
TRADE_EXECUTION = False
PRODUCTION_MODIFICATION = False
EMERGENCY_PAUSE = False

CHECK_INTERVAL_SECONDS = int(
    os.environ.get("GUARDIAN_CHECK_INTERVAL_SECONDS", "60")
)

# Bot 2.0 normally works on a 5-minute scanner cycle.
# 15 minutes gives three expected cycles before Guardian calls activity stale.
OPPORTUNITY_STALE_SECONDS = int(
    os.environ.get("GUARDIAN_OPPORTUNITY_STALE_SECONDS", "900")
)

SEP = "=" * 100

_previous_opportunity_count = None


def section(title):
    print("\n" + SEP, flush=True)
    print(title, flush=True)
    print(SEP, flush=True)


def utc_now():
    return datetime.now(timezone.utc)


def seconds_old(value, now):
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return max(0.0, (now - value).total_seconds())


def database_snapshot():
    url = os.environ.get("SIGNALS2_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        return {
            "ok": False,
            "message": "DATABASE URL NOT CONFIGURED",
        }

    conn = None
    try:
        conn = psycopg2.connect(url, connect_timeout=10)
        conn.set_session(readonly=True, autocommit=False)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    NOW(),
                    COUNT(*),
                    MAX(created_at)
                FROM public.signals2_opportunities
                """
            )
            db_time, opportunity_count, latest_opportunity_time = cur.fetchone()

        conn.rollback()

        return {
            "ok": True,
            "message": "READY | READ ONLY",
            "db_time": db_time,
            "opportunity_count": int(opportunity_count),
            "latest_opportunity_time": latest_opportunity_time,
        }

    except Exception as exc:
        return {
            "ok": False,
            "message": f"{type(exc).__name__}: {exc}",
        }

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
    print(f"OPPORTUNITY STALE LIMIT: {OPPORTUNITY_STALE_SECONDS}s", flush=True)
    print("SAFETY CHECK: PASS", flush=True)


def run_cycle(cycle):
    global _previous_opportunity_count

    started = time.monotonic()
    now = utc_now()

    db = database_snapshot()
    redis_ok, redis_message = redis_health()

    warnings = []

    section(f"GUARDIAN CYCLE {cycle}")
    print(f"UTC: {now.isoformat()}", flush=True)

    if db["ok"]:
        count = db["opportunity_count"]
        latest = db["latest_opportunity_time"]
        age = seconds_old(latest, now)

        print(
            f"POSTGRES: HEALTHY | {db['message']} | opportunities={count}",
            flush=True,
        )

        if latest is None:
            warnings.append("NO OPPORTUNITIES FOUND")
            print("BOT ACTIVITY: WARNING | no opportunity timestamp found", flush=True)
        else:
            freshness = (
                "HEALTHY"
                if age <= OPPORTUNITY_STALE_SECONDS
                else "STALE"
            )

            print(
                "LATEST OPPORTUNITY: "
                f"{latest} | age={age:.0f}s | status={freshness}",
                flush=True,
            )

            if freshness == "STALE":
                warnings.append(
                    f"OPPORTUNITY DATA STALE ({age:.0f}s old)"
                )

        if _previous_opportunity_count is None:
            print(
                "OPPORTUNITY FLOW: BASELINE CAPTURED "
                f"| count={count}",
                flush=True,
            )
        else:
            delta = count - _previous_opportunity_count

            if delta < 0:
                warnings.append("OPPORTUNITY COUNT MOVED BACKWARDS")
                print(
                    f"OPPORTUNITY FLOW: WARNING | delta={delta}",
                    flush=True,
                )
            elif delta == 0:
                print(
                    "OPPORTUNITY FLOW: NO NEW ROWS THIS GUARDIAN CYCLE "
                    "| not automatically an error",
                    flush=True,
                )
            else:
                print(
                    f"OPPORTUNITY FLOW: ACTIVE | +{delta} rows since prior check",
                    flush=True,
                )

        _previous_opportunity_count = count

    else:
        warnings.append("POSTGRES UNAVAILABLE")
        print(f"POSTGRES: WARNING | {db['message']}", flush=True)

    if redis_ok:
        print(f"REDIS: HEALTHY | {redis_message}", flush=True)
    else:
        warnings.append("REDIS UNAVAILABLE")
        print(f"REDIS: WARNING | {redis_message}", flush=True)

    if warnings:
        overall = "WARNING"
        print(f"GUARDIAN STATUS: {overall}", flush=True)
        print("GUARDIAN WARNINGS:", flush=True)
        for warning in warnings:
            print(f" - {warning}", flush=True)
    else:
        overall = "HEALTHY"
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
