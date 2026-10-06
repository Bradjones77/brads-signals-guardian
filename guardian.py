#!/usr/bin/env python3
"""
BRAD'S SIGNALS GUARDIAN
G1.4 - Telegram safety alerts

Observer/safety-alert service:
- PostgreSQL SELECTs only; session forced read-only.
- Redis reads only.
- Telegram sends ONLY Guardian health alerts/recovery notices.
- No Redis writes.
- No database writes.
- No trades.
- No production modification.
- No emergency pause.
- No AI calls.

Alerts:
- HEALTHY -> WARNING: immediate Telegram alert
- WARNING -> HEALTHY: recovery Telegram message
- Persistent same warning: reminder only after cooldown
- Changed warning set: immediate updated alert

Guardian never sends trading signals and never changes Bot 2.0.
"""

import os
import time
from datetime import datetime, timezone

import psycopg2
import redis
import requests


GUARDIAN_VERSION = "G1.4-TELEGRAM-SAFETY-ALERTS"

DATABASE_WRITES = False
REDIS_WRITES = False
TELEGRAM_SAFETY_ALERTS = True
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
SCANNER_SLOW_SECONDS = float(
    os.environ.get("GUARDIAN_SCANNER_SLOW_SECONDS", "240")
)
ALERT_COOLDOWN_SECONDS = int(
    os.environ.get("GUARDIAN_ALERT_COOLDOWN_SECONDS", "1800")
)

EXPECTED_SCANNER_LOCK_MAX_TTL = 240

REDIS_HEALTH_KEY = "signals2:health"
REDIS_WATCHDOG_KEY = "signals2:watchdog"
REDIS_SCANNER_LOCK_KEY = "signals2:scanner_lock"

SEP = "=" * 100

_previous_opportunity_count = None
_no_growth_checks = 0
_previous_last_scan = None
_unchanged_heartbeat_checks = 0

_previous_guardian_status = None
_previous_warning_signature = None
_last_alert_monotonic = None


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
        return {"ok": False, "message": f"{type(exc).__name__}: {exc}"}
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

        return {
            "ok": True,
            "message": "READY | READ ONLY",
            "health": client.hgetall(REDIS_HEALTH_KEY),
            "watchdog": client.hgetall(REDIS_WATCHDOG_KEY),
            "health_ttl": client.ttl(REDIS_HEALTH_KEY),
            "watchdog_ttl": client.ttl(REDIS_WATCHDOG_KEY),
            "scanner_lock_value": client.get(REDIS_SCANNER_LOCK_KEY),
            "scanner_lock_ttl": client.ttl(REDIS_SCANNER_LOCK_KEY),
        }
    except Exception as exc:
        return {"ok": False, "message": f"{type(exc).__name__}: {exc}"}


def telegram_configured():
    token = os.environ.get("GUARDIAN_TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("GUARDIAN_TELEGRAM_CHAT_ID", "").strip()
    return bool(token and chat_id)


def send_guardian_telegram(message):
    """Send Guardian health text only. Never trading content."""
    token = os.environ.get("GUARDIAN_TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("GUARDIAN_TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        print("GUARDIAN TELEGRAM: NOT CONFIGURED | alert not sent", flush=True)
        return False

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": message,
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if response.ok:
            print("GUARDIAN TELEGRAM: SENT", flush=True)
            return True

        print(
            f"GUARDIAN TELEGRAM: FAIL | HTTP {response.status_code}",
            flush=True,
        )
        return False
    except Exception as exc:
        print(
            f"GUARDIAN TELEGRAM: FAIL | {type(exc).__name__}: {exc}",
            flush=True,
        )
        return False


def warning_signature(warnings):
    return "|".join(sorted(code for code, _ in warnings))


def format_alert(warnings, now):
    lines = [
        "🚨 BRAD'S SIGNALS GUARDIAN ALERT",
        "",
        f"Time: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Problems detected: {len(warnings)}",
        "",
    ]
    for code, detail in warnings:
        lines.append(f"• {code}: {detail}")

    lines.extend(
        [
            "",
            "Guardian action: OBSERVE + ALERT ONLY",
            "No trades. No production changes.",
        ]
    )
    return "\n".join(lines)


def format_recovery(now):
    return "\n".join(
        [
            "✅ BRAD'S SIGNALS GUARDIAN RECOVERY",
            "",
            f"Time: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "Guardian status is HEALTHY again.",
            "",
            "Bot monitoring has returned to normal.",
            "No trades or production changes were made.",
        ]
    )


def maybe_send_status_alert(status, warnings, now):
    global _previous_guardian_status
    global _previous_warning_signature
    global _last_alert_monotonic

    current_signature = warning_signature(warnings)
    current_mono = time.monotonic()

    # Do not send a startup "healthy" message. It creates noise on redeploy.
    if _previous_guardian_status is None:
        _previous_guardian_status = status
        _previous_warning_signature = current_signature
        if status == "WARNING":
            sent = send_guardian_telegram(format_alert(warnings, now))
            if sent:
                _last_alert_monotonic = current_mono
            print("ALERT DECISION: STARTUP WARNING", flush=True)
        else:
            print("ALERT DECISION: STARTUP HEALTHY | no Telegram needed", flush=True)
        return

    if status == "WARNING":
        changed = current_signature != _previous_warning_signature
        transitioned = _previous_guardian_status != "WARNING"

        cooldown_elapsed = (
            _last_alert_monotonic is None
            or (current_mono - _last_alert_monotonic) >= ALERT_COOLDOWN_SECONDS
        )

        if transitioned:
            reason = "HEALTHY -> WARNING"
        elif changed:
            reason = "WARNING SET CHANGED"
        elif cooldown_elapsed:
            reason = "PERSISTENT WARNING COOLDOWN ELAPSED"
        else:
            reason = None

        if reason:
            sent = send_guardian_telegram(format_alert(warnings, now))
            if sent:
                _last_alert_monotonic = current_mono
            print(f"ALERT DECISION: SEND | {reason}", flush=True)
        else:
            remaining = ALERT_COOLDOWN_SECONDS
            if _last_alert_monotonic is not None:
                remaining = max(
                    0,
                    int(
                        ALERT_COOLDOWN_SECONDS
                        - (current_mono - _last_alert_monotonic)
                    ),
                )
            print(
                f"ALERT DECISION: SUPPRESS DUPLICATE | cooldown_remaining={remaining}s",
                flush=True,
            )

    elif status == "HEALTHY" and _previous_guardian_status == "WARNING":
        send_guardian_telegram(format_recovery(now))
        print("ALERT DECISION: SEND | WARNING -> HEALTHY RECOVERY", flush=True)

    else:
        print("ALERT DECISION: HEALTHY | no Telegram needed", flush=True)

    _previous_guardian_status = status
    _previous_warning_signature = current_signature


def safety_check():
    if any(
        (
            DATABASE_WRITES,
            REDIS_WRITES,
            TRADE_EXECUTION,
            PRODUCTION_MODIFICATION,
            EMERGENCY_PAUSE,
            AI_CALLS,
        )
    ):
        raise RuntimeError("GUARDIAN SAFETY FLAGS INVALID")


def print_banner():
    section("BRAD'S SIGNALS GUARDIAN")
    print(f"GUARDIAN VERSION: {GUARDIAN_VERSION}", flush=True)
    print(f"UTC START: {utc_now().isoformat()}", flush=True)
    print(f"DATABASE WRITES: {DATABASE_WRITES}", flush=True)
    print(f"REDIS WRITES: {REDIS_WRITES}", flush=True)
    print(f"TELEGRAM SAFETY ALERTS: {TELEGRAM_SAFETY_ALERTS}", flush=True)
    print(f"TELEGRAM CONFIGURED: {telegram_configured()}", flush=True)
    print(f"TRADE EXECUTION: {TRADE_EXECUTION}", flush=True)
    print(f"PRODUCTION MODIFICATION: {PRODUCTION_MODIFICATION}", flush=True)
    print(f"EMERGENCY PAUSE: {EMERGENCY_PAUSE}", flush=True)
    print(f"AI CALLS: {AI_CALLS}", flush=True)
    print(f"CHECK INTERVAL: {CHECK_INTERVAL_SECONDS}s", flush=True)
    print(f"SCANNER STALE LIMIT: {SCANNER_STALE_SECONDS}s", flush=True)
    print(f"OPPORTUNITY STALE LIMIT: {OPPORTUNITY_STALE_SECONDS}s", flush=True)
    print(f"SCANNER SLOW LIMIT: {SCANNER_SLOW_SECONDS:.0f}s", flush=True)
    print(f"ALERT COOLDOWN: {ALERT_COOLDOWN_SECONDS}s", flush=True)
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
            print(f"OPPORTUNITY FLOW: BASELINE CAPTURED | count={count}", flush=True)
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
                f"db_age={wd_db_age}s | scanner_failures={wd_scanner_failures} | "
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

    status = "WARNING" if warnings else "HEALTHY"

    if warnings:
        print("GUARDIAN STATUS: WARNING", flush=True)
        print(f"ANOMALIES DETECTED: {len(warnings)}", flush=True)
        for code, detail in warnings:
            print(f" - {code}: {detail}", flush=True)
    else:
        print("GUARDIAN STATUS: HEALTHY", flush=True)
        print("ANOMALIES DETECTED: 0", flush=True)

    maybe_send_status_alert(status, warnings, now)

    print("ACTION: OBSERVE + SAFETY ALERT ONLY", flush=True)
    print("NO DB/REDIS WRITES; NO TRADES; NO PRODUCTION CHANGES", flush=True)
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
            print("GUARDIAN REMAINS SAFETY-ALERT ONLY", flush=True)
            print("NO DB/REDIS WRITES; NO TRADES; NO PRODUCTION CHANGES", flush=True)

        print(f"GUARDIAN: WAIT {CHECK_INTERVAL_SECONDS}s", flush=True)
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
