#!/usr/bin/env python3
"""
BRAD'S SIGNALS GUARDIAN
G1.3 - Failure & anomaly detector

Observer-only:
- PostgreSQL SELECTs only; session forced read-only.
- Redis reads only.
- No Redis writes.
- No Telegram.
- No trades.
- No production modification.
- No emergency pause.
- No AI calls.

G1.3 watches:
- PostgreSQL availability and opportunity freshness
- opportunity-flow gaps across Guardian checks
- Bot 2.0 shared scanner heartbeat
- Bot 2.0 watchdog warnings
- consecutive scanner failures
- scanner duration anomalies
- Redis health
- scanner-lock anomalies

IMPORTANT:
Warnings cause observation/logging only. Guardian does not intervene.
"""

import os
import time
from datetime import datetime, timezone

import psycopg2
import redis


GUARDIAN_VERSION = "G1.3-FAILURE-ANOMALY-DETECTOR"

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

SCANNER_STALE_SECONDS = int(
    os.environ.get("GUARDIAN_SCANNER_STALE_SECONDS", "720")
)

OPPORTUNITY_STALE_SECONDS = int(
    os.environ.get("GUARDIAN_OPPORTUNITY_STALE_SECONDS", "900")
)

# Current healthy runtime showed ~101 seconds. This limit is deliberately
# generous so normal variation does not trigger noise.
SCANNER_SLOW_SECONDS = float(
    os.environ.get("GUARDIAN_SCANNER_SLOW_SECONDS", "240")
)

# Production scanner lock TTL is 240 seconds. A live lock at/under that TTL
# is normal. Guardian warns only if Redis reports something inconsistent.
EXPECTED_SCANNER_LOCK_MAX_TTL = 240

REDIS_HEALTH_KEY = "signals2:health"
REDIS_WATCHDOG_KEY = "signals2:watchdog"
REDIS_SCANNER_LOCK_KEY = "signals2:scanner_lock"

SEP = "=" * 100

_previous_opportunity_count = None
_no_growth_checks = 0
_previous_last_scan = None
_unchanged_heartbeat_checks = 0


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


def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


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
            db_time, opportunity_count, latest_created_at = cur.fetchone()

        conn.rollback()

        return {
            "ok": True,
            "message": "READY | READ ONLY",
            "db_time": db_time,
            "opportunity_count": int(opportunity_count),
            "latest_created_at": latest_created_at,
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

        # READS ONLY.
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
    print(f"SCANNER SLOW LIMIT: {SCANNER_SLOW_SECONDS:.0f}s", flush=True)
    print("SAFETY CHECK: PASS", flush=True)


def add_warning(warnings, code, detail):
    warnings.append((code, detail))


def run_cycle(cycle):
    global _previous_opportunity_count
    global _no_growth_checks
    global _previous_last_scan
    global _unchanged_heartbeat_checks

    started = time.monotonic()
    now = utc_now()

    db = database_snapshot()
    redis_data = redis_snapshot()
    warnings = []

    section(f"GUARDIAN CYCLE {cycle}")
    print(f"UTC: {now.isoformat()}", flush=True)

    # ------------------------------------------------------------
    # 1. Independent PostgreSQL evidence
    # ------------------------------------------------------------
    if db["ok"]:
        count = db["opportunity_count"]
        latest = db["latest_created_at"]
        age = seconds_old(latest, now)

        print(
            f"POSTGRES: HEALTHY | {db['message']} | opportunities={count}",
            flush=True,
        )

        if latest is None:
            add_warning(warnings, "DB_NO_OPPORTUNITIES", "no opportunity timestamp found")
            print("LATEST OPPORTUNITY: WARNING | missing", flush=True)
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
                add_warning(
                    warnings,
                    "DB_OPPORTUNITY_STALE",
                    f"latest opportunity is {age:.0f}s old",
                )

        if _previous_opportunity_count is None:
            _no_growth_checks = 0
            print(
                f"OPPORTUNITY FLOW: BASELINE CAPTURED | count={count}",
                flush=True,
            )
        else:
            delta = count - _previous_opportunity_count

            if delta < 0:
                _no_growth_checks = 0
                add_warning(
                    warnings,
                    "DB_COUNT_REVERSED",
                    f"opportunity count moved backwards by {abs(delta)}",
                )
                print(f"OPPORTUNITY FLOW: WARNING | delta={delta}", flush=True)

            elif delta == 0:
                _no_growth_checks += 1
                print(
                    "OPPORTUNITY FLOW: NO NEW ROWS "
                    f"| consecutive Guardian checks={_no_growth_checks}",
                    flush=True,
                )

                # Do not warn merely because one-minute checks see no growth.
                # Freshness is the authoritative stale-data test.
                if age is not None and age > OPPORTUNITY_STALE_SECONDS:
                    add_warning(
                        warnings,
                        "DB_FLOW_STALLED",
                        f"no growth and latest row is {age:.0f}s old",
                    )
            else:
                _no_growth_checks = 0
                print(
                    f"OPPORTUNITY FLOW: ACTIVE | +{delta} rows since prior check",
                    flush=True,
                )

        _previous_opportunity_count = count

    else:
        add_warning(warnings, "POSTGRES_UNAVAILABLE", db["message"])
        print(f"POSTGRES: WARNING | {db['message']}", flush=True)

    # ------------------------------------------------------------
    # 2. Redis + real Bot 2.0 heartbeat/watchdog
    # ------------------------------------------------------------
    if redis_data["ok"]:
        print(f"REDIS: HEALTHY | {redis_data['message']}", flush=True)

        health = redis_data["health"]
        watchdog = redis_data["watchdog"]

        if not health:
            add_warning(
                warnings,
                "BOT_HEALTH_KEY_MISSING",
                f"{REDIS_HEALTH_KEY} missing or expired",
            )
            print("BOT HEARTBEAT: WARNING | shared health key missing", flush=True)

        else:
            last_scan = health.get("last_scan")
            scan_age = seconds_old(last_scan, now)
            source = health.get("source")
            last_error = health.get("last_error")
            failures = safe_int(health.get("scanner_failures_consecutive"), 0)
            duration = safe_float(health.get("last_scan_duration_seconds"))

            if last_scan == _previous_last_scan and last_scan not in (None, "", "None"):
                _unchanged_heartbeat_checks += 1
            else:
                _unchanged_heartbeat_checks = 0
            _previous_last_scan = last_scan

            if scan_age is None:
                scanner_status = "WARNING"
                add_warning(
                    warnings,
                    "SCANNER_HEARTBEAT_INVALID",
                    "last_scan is missing or invalid",
                )
            elif scan_age > SCANNER_STALE_SECONDS:
                scanner_status = "STALE"
                add_warning(
                    warnings,
                    "SCANNER_HEARTBEAT_STALE",
                    f"last scanner heartbeat is {scan_age:.0f}s old",
                )
            else:
                scanner_status = "HEALTHY"

            print(
                "BOT HEARTBEAT: "
                f"status={scanner_status} | source={source} | "
                f"last_scan={last_scan} | "
                f"age={None if scan_age is None else round(scan_age, 1)}s | "
                f"unchanged_checks={_unchanged_heartbeat_checks} | "
                f"duration={duration}s | scanner_failures={failures} | "
                f"last_error={last_error} | redis_ttl={redis_data['health_ttl']}s",
                flush=True,
            )

            if failures >= 3:
                add_warning(
                    warnings,
                    "SCANNER_REPEATED_FAILURES",
                    f"Bot reports {failures} consecutive scanner failures",
                )

            if duration is not None and duration > SCANNER_SLOW_SECONDS:
                add_warning(
                    warnings,
                    "SCANNER_SLOW",
                    f"last scanner duration {duration:.1f}s exceeds "
                    f"{SCANNER_SLOW_SECONDS:.0f}s limit",
                )

            if last_error not in (None, "", "None"):
                print(f"BOT LAST ERROR OBSERVED: {last_error}", flush=True)

        if not watchdog:
            add_warning(
                warnings,
                "BOT_WATCHDOG_KEY_MISSING",
                f"{REDIS_WATCHDOG_KEY} missing or expired",
            )
            print("BOT WATCHDOG: WARNING | shared watchdog key missing", flush=True)

        else:
            wd_status = watchdog.get("status", "UNKNOWN")
            wd_issues = watchdog.get("issues", "unknown")
            wd_source = watchdog.get("source", "unknown")
            wd_scan_age = watchdog.get("scan_age_seconds")
            wd_db_age = watchdog.get("db_age_seconds")
            wd_scanner_failures = watchdog.get("scanner_failures_consecutive")

            print(
                "BOT WATCHDOG: "
                f"status={wd_status} | issues={wd_issues} | "
                f"source={wd_source} | scan_age={wd_scan_age}s | "
                f"db_age={wd_db_age}s | "
                f"scanner_failures={wd_scanner_failures} | "
                f"redis_ttl={redis_data['watchdog_ttl']}s",
                flush=True,
            )

            if wd_status != "HEALTHY":
                add_warning(
                    warnings,
                    "BOT_WATCHDOG_WARNING",
                    f"status={wd_status}; issues={wd_issues}",
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
                f"SCANNER LOCK: held | token={lock_value} | ttl={lock_ttl}s",
                flush=True,
            )

            if lock_ttl is not None and lock_ttl > EXPECTED_SCANNER_LOCK_MAX_TTL:
                add_warning(
                    warnings,
                    "SCANNER_LOCK_TTL_ABNORMAL",
                    f"lock TTL {lock_ttl}s exceeds expected "
                    f"{EXPECTED_SCANNER_LOCK_MAX_TTL}s",
                )

            if lock_ttl == -1:
                add_warning(
                    warnings,
                    "SCANNER_LOCK_NO_EXPIRY",
                    "scanner lock exists without an expiry",
                )

    else:
        add_warning(warnings, "REDIS_UNAVAILABLE", redis_data["message"])
        print(f"REDIS: WARNING | {redis_data['message']}", flush=True)

    # ------------------------------------------------------------
    # 3. Guardian anomaly summary
    # ------------------------------------------------------------
    if warnings:
        print("GUARDIAN STATUS: WARNING", flush=True)
        print(f"ANOMALIES DETECTED: {len(warnings)}", flush=True)
        for code, detail in warnings:
            print(f" - {code}: {detail}", flush=True)
    else:
        print("GUARDIAN STATUS: HEALTHY", flush=True)
        print("ANOMALIES DETECTED: 0", flush=True)

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
