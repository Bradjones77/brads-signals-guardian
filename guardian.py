#!/usr/bin/env python3
"""
BRAD'S SIGNALS GUARDIAN
G1.2 - Scanner health monitor

Observer-only:
- PostgreSQL SELECTs only; session forced read-only.
- Redis reads only (PING / HGETALL / TTL / GET).
- No Redis writes.
- No Telegram.
- No trades.
- No production modification.
- No emergency pause.

G1.2 is grounded in Bot 2.0's actual shared Redis telemetry:
- signals2:health
- signals2:watchdog
- signals2:scanner_lock

It also independently checks PostgreSQL opportunity freshness so Guardian
does not rely on the bot's own opinion of its health.
"""

import os
import time
from datetime import datetime, timezone

import psycopg2
import redis


GUARDIAN_VERSION = "G1.2-SCANNER-HEALTH-MONITOR"

DATABASE_WRITES = False
REDIS_WRITES = False
TELEGRAM_SENDING = False
TRADE_EXECUTION = False
PRODUCTION_MODIFICATION = False
EMERGENCY_PAUSE = False
AI_CALLS = False

CHECK_INTERVAL_SECONDS = int(
    os.environ.get("GUARDIAN_CHECK_INTERVAL_SECONDS", "60")
)

# Bot 2.0 normally scans on a 5-minute cadence. Bot 2.0 itself uses
# 720 seconds for scanner staleness, so Guardian independently uses
# the same generous 12-minute limit for the shared scanner heartbeat.
SCANNER_STALE_SECONDS = int(
    os.environ.get("GUARDIAN_SCANNER_STALE_SECONDS", "720")
)

# Independent DB evidence gets a slightly wider window because a cycle
# may legitimately be skipped/locked during deployment overlap.
OPPORTUNITY_STALE_SECONDS = int(
    os.environ.get("GUARDIAN_OPPORTUNITY_STALE_SECONDS", "900")
)

REDIS_HEALTH_KEY = "signals2:health"
REDIS_WATCHDOG_KEY = "signals2:watchdog"
REDIS_SCANNER_LOCK_KEY = "signals2:scanner_lock"

SEP = "=" * 100

_previous_opportunity_count = None


def section(title):
    print("\n" + SEP, flush=True)
    print(title, flush=True)
    print(SEP, flush=True)


def utc_now():
    return datetime.now(timezone.utc)


def parse_timestamp(value):
    if value in (None, "", "None"):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def seconds_old(value, now):
    parsed = value if isinstance(value, datetime) else parse_timestamp(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (now - parsed).total_seconds())


def database_snapshot():
    url = os.environ.get("SIGNALS2_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        return {"ok": False, "message": "DATABASE URL NOT CONFIGURED"}

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


def redis_snapshot():
    url = os.environ.get("SIGNALS2_REDIS_URL") or os.environ.get("REDIS_URL")
    if not url:
        return {"ok": False, "message": "REDIS URL NOT CONFIGURED"}

    try:
        client = redis.Redis.from_url(
            url,
            socket_connect_timeout=10,
            socket_timeout=10,
            decode_responses=True,
        )

        if not client.ping():
            return {"ok": False, "message": "PING RETURNED FALSE"}

        # READS ONLY. Guardian never SETs/HSETs/EXPIREs/DELs production keys.
        health = client.hgetall(REDIS_HEALTH_KEY)
        watchdog = client.hgetall(REDIS_WATCHDOG_KEY)
        health_ttl = client.ttl(REDIS_HEALTH_KEY)
        watchdog_ttl = client.ttl(REDIS_WATCHDOG_KEY)
        scanner_lock_value = client.get(REDIS_SCANNER_LOCK_KEY)
        scanner_lock_ttl = client.ttl(REDIS_SCANNER_LOCK_KEY)

        return {
            "ok": True,
            "message": "READY | READ ONLY",
            "health": health,
            "watchdog": watchdog,
            "health_ttl": health_ttl,
            "watchdog_ttl": watchdog_ttl,
            "scanner_lock_value": scanner_lock_value,
            "scanner_lock_ttl": scanner_lock_ttl,
        }

    except Exception as exc:
        return {
            "ok": False,
            "message": f"{type(exc).__name__}: {exc}",
        }


def safety_check():
    unsafe = any(
        (
            DATABASE_WRITES,
            REDIS_WRITES,
            TELEGRAM_SENDING,
            TRADE_EXECUTION,
            PRODUCTION_MODIFICATION,
            EMERGENCY_PAUSE,
            AI_CALLS,
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
    print(f"AI CALLS: {AI_CALLS}", flush=True)
    print(f"CHECK INTERVAL: {CHECK_INTERVAL_SECONDS}s", flush=True)
    print(f"SCANNER STALE LIMIT: {SCANNER_STALE_SECONDS}s", flush=True)
    print(f"OPPORTUNITY STALE LIMIT: {OPPORTUNITY_STALE_SECONDS}s", flush=True)
    print("SAFETY CHECK: PASS", flush=True)


def run_cycle(cycle):
    global _previous_opportunity_count

    started = time.monotonic()
    now = utc_now()

    db = database_snapshot()
    redis_data = redis_snapshot()

    warnings = []

    section(f"GUARDIAN CYCLE {cycle}")
    print(f"UTC: {now.isoformat()}", flush=True)

    # ------------------------------------------------------------
    # 1. Independent PostgreSQL activity evidence
    # ------------------------------------------------------------
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
            print("DB ACTIVITY: WARNING | no opportunity timestamp found", flush=True)
        else:
            freshness = (
                "HEALTHY"
                if age is not None and age <= OPPORTUNITY_STALE_SECONDS
                else "STALE"
            )
            print(
                f"LATEST OPPORTUNITY: {latest} | age={age:.0f}s | status={freshness}",
                flush=True,
            )
            if freshness == "STALE":
                warnings.append(f"OPPORTUNITY DATA STALE ({age:.0f}s old)")

        if _previous_opportunity_count is None:
            print(
                f"OPPORTUNITY FLOW: BASELINE CAPTURED | count={count}",
                flush=True,
            )
        else:
            delta = count - _previous_opportunity_count
            if delta < 0:
                warnings.append("OPPORTUNITY COUNT MOVED BACKWARDS")
                print(f"OPPORTUNITY FLOW: WARNING | delta={delta}", flush=True)
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

    # ------------------------------------------------------------
    # 2. Bot 2.0's actual shared Redis health/watchdog telemetry
    # ------------------------------------------------------------
    if redis_data["ok"]:
        print(f"REDIS: HEALTHY | {redis_data['message']}", flush=True)

        health = redis_data["health"]
        watchdog = redis_data["watchdog"]

        if not health:
            warnings.append("BOT HEALTH KEY MISSING")
            print(
                f"BOT HEARTBEAT: WARNING | {REDIS_HEALTH_KEY} missing/expired",
                flush=True,
            )
        else:
            last_scan = health.get("last_scan")
            scan_age = seconds_old(last_scan, now)
            source = health.get("source")
            last_error = health.get("last_error")
            scanner_failures = health.get("scanner_failures_consecutive")
            last_scan_duration = health.get("last_scan_duration_seconds")

            if scan_age is None:
                warnings.append("BOT LAST_SCAN INVALID OR MISSING")
                scanner_status = "WARNING"
            elif scan_age > SCANNER_STALE_SECONDS:
                warnings.append(f"BOT SCANNER HEARTBEAT STALE ({scan_age:.0f}s old)")
                scanner_status = "STALE"
            else:
                scanner_status = "HEALTHY"

            print(
                "BOT HEARTBEAT: "
                f"status={scanner_status} | source={source} | "
                f"last_scan={last_scan} | "
                f"age={None if scan_age is None else round(scan_age, 1)}s | "
                f"duration={last_scan_duration}s | "
                f"scanner_failures={scanner_failures} | "
                f"last_error={last_error} | "
                f"redis_ttl={redis_data['health_ttl']}s",
                flush=True,
            )

            try:
                failures = int(scanner_failures or 0)
            except Exception:
                failures = 0

            if failures >= 3:
                warnings.append(
                    f"BOT REPORTS REPEATED SCANNER FAILURES ({failures})"
                )

            if last_error not in (None, "", "None"):
                print(
                    f"BOT LAST ERROR OBSERVED: {last_error}",
                    flush=True,
                )

        if not watchdog:
            warnings.append("BOT WATCHDOG KEY MISSING")
            print(
                f"BOT WATCHDOG: WARNING | {REDIS_WATCHDOG_KEY} missing/expired",
                flush=True,
            )
        else:
            wd_status = watchdog.get("status", "UNKNOWN")
            wd_issues = watchdog.get("issues", "unknown")
            wd_source = watchdog.get("source", "unknown")

            print(
                "BOT WATCHDOG: "
                f"status={wd_status} | issues={wd_issues} | "
                f"source={wd_source} | redis_ttl={redis_data['watchdog_ttl']}s",
                flush=True,
            )

            if wd_status != "HEALTHY":
                warnings.append(
                    f"BOT WATCHDOG REPORTS {wd_status}: {wd_issues}"
                )

        lock_value = redis_data["scanner_lock_value"]
        lock_ttl = redis_data["scanner_lock_ttl"]

        if lock_value is None:
            print(
                "SCANNER LOCK: not currently held | normal between scanner work",
                flush=True,
            )
        else:
            print(
                f"SCANNER LOCK: currently held | token={lock_value} | ttl={lock_ttl}s",
                flush=True,
            )
            # The production lock is intentionally short-lived (240s).
            # Merely seeing it held is NOT an error; Guardian only observes it.

    else:
        warnings.append("REDIS UNAVAILABLE")
        print(f"REDIS: WARNING | {redis_data['message']}", flush=True)

    # ------------------------------------------------------------
    # 3. Independent Guardian verdict
    # ------------------------------------------------------------
    if warnings:
        print("GUARDIAN STATUS: WARNING", flush=True)
        print("GUARDIAN WARNINGS:", flush=True)
        for warning in warnings:
            print(f" - {warning}", flush=True)
    else:
        print("GUARDIAN STATUS: HEALTHY", flush=True)

    print("ACTION: OBSERVE ONLY", flush=True)
    print("NO WRITES; NO SENDS; NO TRADES; NO PRODUCTION CHANGES", flush=True)
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
            print("NO WRITES; NO SENDS; NO TRADES; NO PRODUCTION CHANGES", flush=True)

        print(f"GUARDIAN: WAIT {CHECK_INTERVAL_SECONDS}s", flush=True)
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
