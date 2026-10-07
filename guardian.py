#!/usr/bin/env python3
"""
BRAD'S SIGNALS GUARDIAN
G1.9 - Data and model drift monitoring

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

G1.7 preserves all G1.6 service-health checks and adds conservative,
read-only behavioural observation over recent Bot 2.0 opportunities:
- LONG/SHORT direction balance
- final-confidence distribution
- genuine AI participation rate
- recent opportunity production volume

Behaviour warnings require a minimum sample and use broad safety bounds.
They are anomaly indicators, not trading decisions and not profitability claims.

Guardian never sends trading signals and never changes Bot 2.0.
"""

import os
import time
from datetime import datetime, timezone

import psycopg2
import redis
import requests


GUARDIAN_VERSION = "G1.9-DATA-MODEL-DRIFT-MONITOR"

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
MEMORY_STALE_SECONDS = int(
    os.environ.get("GUARDIAN_MEMORY_STALE_SECONDS", "5400")
)
OUTCOME_WORKER_START_GRACE_SECONDS = int(
    os.environ.get("GUARDIAN_OUTCOME_WORKER_START_GRACE_SECONDS", "5400")
)
OUTCOME_WORKER_STALE_SECONDS = int(
    os.environ.get("GUARDIAN_OUTCOME_WORKER_STALE_SECONDS", "5400")
)
OUTCOME_WORKER_FAILURE_LIMIT = int(
    os.environ.get("GUARDIAN_OUTCOME_WORKER_FAILURE_LIMIT", "2")
)
OUTCOME_WORKER_SLOW_SECONDS = float(
    os.environ.get("GUARDIAN_OUTCOME_WORKER_SLOW_SECONDS", "900")
)
FLOW_CONTRADICTION_CHECKS = int(
    os.environ.get("GUARDIAN_FLOW_CONTRADICTION_CHECKS", "3")
)
BEHAVIOUR_WINDOW_MINUTES = int(os.environ.get("GUARDIAN_BEHAVIOUR_WINDOW_MINUTES", "60"))
BEHAVIOUR_MIN_SAMPLE = int(os.environ.get("GUARDIAN_BEHAVIOUR_MIN_SAMPLE", "100"))
DIRECTION_IMBALANCE_LIMIT = float(os.environ.get("GUARDIAN_DIRECTION_IMBALANCE_LIMIT", "0.90"))
CONFIDENCE_MEAN_LOW = float(os.environ.get("GUARDIAN_CONFIDENCE_MEAN_LOW", "20"))
CONFIDENCE_MEAN_HIGH = float(os.environ.get("GUARDIAN_CONFIDENCE_MEAN_HIGH", "95"))
AI_PARTICIPATION_HIGH = float(os.environ.get("GUARDIAN_AI_PARTICIPATION_HIGH", "0.60"))
DRIFT_RECENT_HOURS = int(os.environ.get("GUARDIAN_DRIFT_RECENT_HOURS", "6"))
DRIFT_BASELINE_HOURS = int(os.environ.get("GUARDIAN_DRIFT_BASELINE_HOURS", "48"))
DRIFT_MIN_RECENT_SAMPLE = int(os.environ.get("GUARDIAN_DRIFT_MIN_RECENT_SAMPLE", "500"))
DRIFT_CONFIDENCE_MEAN_DELTA = float(os.environ.get("GUARDIAN_DRIFT_CONFIDENCE_MEAN_DELTA", "12"))
DRIFT_DIRECTION_SHARE_DELTA = float(os.environ.get("GUARDIAN_DRIFT_DIRECTION_SHARE_DELTA", "0.20"))
DRIFT_AI_SHARE_DELTA = float(os.environ.get("GUARDIAN_DRIFT_AI_SHARE_DELTA", "0.25"))
DRIFT_VOLUME_RATIO_LOW = float(os.environ.get("GUARDIAN_DRIFT_VOLUME_RATIO_LOW", "0.40"))
DRIFT_VOLUME_RATIO_HIGH = float(os.environ.get("GUARDIAN_DRIFT_VOLUME_RATIO_HIGH", "2.50"))

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

_previous_integrated_complete = None
_previous_integrated_h24 = None


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

            cur.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE direction = 'LONG'),
                    COUNT(*) FILTER (WHERE direction = 'SHORT'),
                    AVG(final_confidence) FILTER (WHERE final_confidence IS NOT NULL),
                    MIN(final_confidence) FILTER (WHERE final_confidence IS NOT NULL),
                    MAX(final_confidence) FILTER (WHERE final_confidence IS NOT NULL),
                    COUNT(*) FILTER (
                        WHERE ai_confidence IS NOT NULL
                          AND COALESCE((ai_analysis->>'available')::boolean, FALSE) = TRUE
                          AND ai_analysis ? 'ai_score'
                    ),
                    COUNT(*) FILTER (WHERE signal_sent = TRUE)
                FROM public.signals2_opportunities
                WHERE created_at >= NOW() - (%s * INTERVAL '1 minute')
                  AND symbol NOT LIKE 'SIGNALS2%%'
                """,
                (BEHAVIOUR_WINDOW_MINUTES,),
            )
            b = cur.fetchone()

            # G1.8 outcome/learning integrity snapshot. SELECT only.
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                    ) AS integrated_total,
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                          AND r.outcome_complete = TRUE
                    ) AS integrated_complete,
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                          AND r.direction_correct_30s IS NOT NULL
                    ) AS h30s,
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                          AND r.direction_correct_1m IS NOT NULL
                    ) AS h1m,
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                          AND r.direction_correct_5m IS NOT NULL
                    ) AS h5m,
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                          AND r.direction_correct_10m IS NOT NULL
                    ) AS h10m,
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                          AND r.direction_correct_30m IS NOT NULL
                    ) AS h30m,
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                          AND r.direction_correct_1h IS NOT NULL
                    ) AS h1h,
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                          AND r.direction_correct_4h IS NOT NULL
                    ) AS h4h,
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                          AND r.direction_correct_12h IS NOT NULL
                    ) AS h12h,
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                          AND r.direction_correct_24h IS NOT NULL
                    ) AS h24h,
                    COUNT(*) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                          AND r.outcome_complete = FALSE
                          AND r.opportunity_time <= NOW() - INTERVAL '24 hours 5 minutes'
                    ) AS mature_pending,
                    MAX(r.last_updated) FILTER (
                        WHERE o.model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                          AND o.symbol NOT LIKE 'SIGNALS2%%'
                    ) AS latest_outcome_update
                FROM public.signals2_opportunities o
                JOIN public.signals2_outcomes r
                  ON r.opportunity_id = o.opportunity_id
                """
            )
            oi = cur.fetchone()

            # G1.9 drift snapshot: recent window versus an older historical baseline.
            # Restrict to the current integrated model and real symbols.
            cur.execute(
                """
                WITH recent AS (
                    SELECT
                        COUNT(*)::bigint AS n,
                        AVG(final_confidence) AS avg_conf,
                        AVG(CASE WHEN direction = 'LONG' THEN 1.0 ELSE 0.0 END) AS long_share,
                        AVG(
                            CASE WHEN ai_confidence IS NOT NULL
                                  AND COALESCE((ai_analysis->>'available')::boolean, FALSE) = TRUE
                                  AND ai_analysis ? 'ai_score'
                                 THEN 1.0 ELSE 0.0 END
                        ) AS ai_share,
                        COUNT(DISTINCT model_version) AS model_versions,
                        COUNT(DISTINCT strategy_version) AS strategy_versions,
                        MAX(model_version) AS model_version,
                        MAX(strategy_version) AS strategy_version
                    FROM public.signals2_opportunities
                    WHERE created_at >= NOW() - (%s * INTERVAL '1 hour')
                      AND symbol NOT LIKE 'SIGNALS2%%'
                      AND model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                ),
                baseline AS (
                    SELECT
                        COUNT(*)::bigint AS n,
                        AVG(final_confidence) AS avg_conf,
                        AVG(CASE WHEN direction = 'LONG' THEN 1.0 ELSE 0.0 END) AS long_share,
                        AVG(
                            CASE WHEN ai_confidence IS NOT NULL
                                  AND COALESCE((ai_analysis->>'available')::boolean, FALSE) = TRUE
                                  AND ai_analysis ? 'ai_score'
                                 THEN 1.0 ELSE 0.0 END
                        ) AS ai_share
                    FROM public.signals2_opportunities
                    WHERE created_at < NOW() - (%s * INTERVAL '1 hour')
                      AND created_at >= NOW() - (%s * INTERVAL '1 hour')
                      AND symbol NOT LIKE 'SIGNALS2%%'
                      AND model_version = 'SIGNALS2_AI_INTEGRATED_V1'
                )
                SELECT
                    recent.n, recent.avg_conf, recent.long_share, recent.ai_share,
                    recent.model_versions, recent.strategy_versions,
                    recent.model_version, recent.strategy_version,
                    baseline.n, baseline.avg_conf, baseline.long_share, baseline.ai_share
                FROM recent CROSS JOIN baseline
                """,
                (DRIFT_RECENT_HOURS, DRIFT_RECENT_HOURS, DRIFT_BASELINE_HOURS),
            )
            dr = cur.fetchone()

        conn.rollback()
        return {
            "ok": True,
            "message": "READY | READ ONLY",
            "db_time": db_time,
            "opportunity_count": int(opportunity_count),
            "latest_created_at": latest_created_at,
            "behaviour": {
                "sample_count": int(b[0] or 0),
                "long_count": int(b[1] or 0),
                "short_count": int(b[2] or 0),
                "avg_final_confidence": float(b[3]) if b[3] is not None else None,
                "min_final_confidence": float(b[4]) if b[4] is not None else None,
                "max_final_confidence": float(b[5]) if b[5] is not None else None,
                "genuine_ai_count": int(b[6] or 0),
                "signal_sent_count": int(b[7] or 0),
            },
            "outcome_integrity": {
                "integrated_total": int(oi[0] or 0),
                "integrated_complete": int(oi[1] or 0),
                "h30s": int(oi[2] or 0),
                "h1m": int(oi[3] or 0),
                "h5m": int(oi[4] or 0),
                "h10m": int(oi[5] or 0),
                "h30m": int(oi[6] or 0),
                "h1h": int(oi[7] or 0),
                "h4h": int(oi[8] or 0),
                "h12h": int(oi[9] or 0),
                "h24h": int(oi[10] or 0),
                "mature_pending": int(oi[11] or 0),
                "latest_outcome_update": oi[12],
            },
            "drift": {
                "recent_n": int(dr[0] or 0),
                "recent_avg_conf": float(dr[1]) if dr[1] is not None else None,
                "recent_long_share": float(dr[2]) if dr[2] is not None else None,
                "recent_ai_share": float(dr[3]) if dr[3] is not None else None,
                "recent_model_versions": int(dr[4] or 0),
                "recent_strategy_versions": int(dr[5] or 0),
                "recent_model_version": dr[6],
                "recent_strategy_version": dr[7],
                "baseline_n": int(dr[8] or 0),
                "baseline_avg_conf": float(dr[9]) if dr[9] is not None else None,
                "baseline_long_share": float(dr[10]) if dr[10] is not None else None,
                "baseline_ai_share": float(dr[11]) if dr[11] is not None else None,
            },
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
    print(f"MEMORY STALE LIMIT: {MEMORY_STALE_SECONDS}s", flush=True)
    print(
        f"OUTCOME WORKER START GRACE: {OUTCOME_WORKER_START_GRACE_SECONDS}s",
        flush=True,
    )
    print(
        f"OUTCOME WORKER STALE LIMIT: {OUTCOME_WORKER_STALE_SECONDS}s",
        flush=True,
    )
    print(
        f"OUTCOME WORKER FAILURE LIMIT: {OUTCOME_WORKER_FAILURE_LIMIT}",
        flush=True,
    )
    print(f"OUTCOME WORKER SLOW LIMIT: {OUTCOME_WORKER_SLOW_SECONDS:.0f}s", flush=True)
    print(f"FLOW CONTRADICTION CHECKS: {FLOW_CONTRADICTION_CHECKS}", flush=True)
    print(f"BEHAVIOUR WINDOW: {BEHAVIOUR_WINDOW_MINUTES}m", flush=True)
    print(f"BEHAVIOUR MIN SAMPLE: {BEHAVIOUR_MIN_SAMPLE}", flush=True)
    print(f"DIRECTION IMBALANCE LIMIT: {DIRECTION_IMBALANCE_LIMIT:.0%}", flush=True)
    print(f"CONFIDENCE MEAN BOUNDS: {CONFIDENCE_MEAN_LOW:.1f}-{CONFIDENCE_MEAN_HIGH:.1f}", flush=True)
    print(f"AI PARTICIPATION HIGH LIMIT: {AI_PARTICIPATION_HIGH:.0%}", flush=True)
    print(f"DRIFT RECENT WINDOW: {DRIFT_RECENT_HOURS}h", flush=True)
    print(f"DRIFT BASELINE WINDOW: {DRIFT_BASELINE_HOURS}h", flush=True)
    print(f"DRIFT MIN RECENT SAMPLE: {DRIFT_MIN_RECENT_SAMPLE}", flush=True)
    print(f"DRIFT CONFIDENCE MEAN DELTA: {DRIFT_CONFIDENCE_MEAN_DELTA:.1f}", flush=True)
    print(f"DRIFT DIRECTION SHARE DELTA: {DRIFT_DIRECTION_SHARE_DELTA:.0%}", flush=True)
    print(f"DRIFT AI SHARE DELTA: {DRIFT_AI_SHARE_DELTA:.0%}", flush=True)
    print(f"DRIFT VOLUME RATIO BOUNDS: {DRIFT_VOLUME_RATIO_LOW:.2f}-{DRIFT_VOLUME_RATIO_HIGH:.2f}", flush=True)
    print(f"ALERT COOLDOWN: {ALERT_COOLDOWN_SECONDS}s", flush=True)
    print("SAFETY CHECK: PASS", flush=True)


def add_warning(warnings, code, detail):
    warnings.append((code, detail))


def run_cycle(cycle):
    global _previous_opportunity_count
    global _no_growth_checks
    global _previous_last_scan
    global _unchanged_heartbeat_checks
    global _previous_integrated_complete
    global _previous_integrated_h24

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

        behaviour = db.get("behaviour") or {}
        sample_count = safe_int(behaviour.get("sample_count"), 0)
        long_count = safe_int(behaviour.get("long_count"), 0)
        short_count = safe_int(behaviour.get("short_count"), 0)
        avg_conf = safe_float(behaviour.get("avg_final_confidence"))
        min_conf = safe_float(behaviour.get("min_final_confidence"))
        max_conf = safe_float(behaviour.get("max_final_confidence"))
        genuine_ai_count = safe_int(behaviour.get("genuine_ai_count"), 0)
        signal_sent_count = safe_int(behaviour.get("signal_sent_count"), 0)

        directional_total = long_count + short_count
        dominant_share = max(long_count, short_count) / directional_total if directional_total else None
        ai_share = genuine_ai_count / sample_count if sample_count else None

        print(
            "BEHAVIOUR SNAPSHOT: "
            f"window={BEHAVIOUR_WINDOW_MINUTES}m | sample={sample_count} | "
            f"LONG={long_count} | SHORT={short_count} | "
            f"dominant_share={None if dominant_share is None else round(dominant_share, 3)} | "
            f"avg_conf={None if avg_conf is None else round(avg_conf, 2)} | "
            f"min_conf={None if min_conf is None else round(min_conf, 2)} | "
            f"max_conf={None if max_conf is None else round(max_conf, 2)} | "
            f"genuine_ai={genuine_ai_count} | "
            f"ai_share={None if ai_share is None else round(ai_share, 3)} | "
            f"signals_sent={signal_sent_count}",
            flush=True,
        )

        if sample_count < BEHAVIOUR_MIN_SAMPLE:
            print(
                f"BEHAVIOUR STATUS: OBSERVE ONLY | sample {sample_count} below minimum {BEHAVIOUR_MIN_SAMPLE}",
                flush=True,
            )
        else:
            flags = 0
            if dominant_share is not None and dominant_share >= DIRECTION_IMBALANCE_LIMIT:
                dominant = "LONG" if long_count >= short_count else "SHORT"
                add_warning(
                    warnings, "DIRECTION_EXTREME_IMBALANCE",
                    f"{dominant} is {dominant_share:.1%} of {directional_total} recent directional opportunities",
                )
                flags += 1
            if avg_conf is not None and (avg_conf < CONFIDENCE_MEAN_LOW or avg_conf > CONFIDENCE_MEAN_HIGH):
                add_warning(
                    warnings, "CONFIDENCE_DISTRIBUTION_ABNORMAL",
                    f"recent mean final confidence {avg_conf:.2f} is outside {CONFIDENCE_MEAN_LOW:.1f}-{CONFIDENCE_MEAN_HIGH:.1f}",
                )
                flags += 1
            # Low AI participation is not a warning because ranked AI is selective.
            if ai_share is not None and ai_share > AI_PARTICIPATION_HIGH:
                add_warning(
                    warnings, "AI_PARTICIPATION_HIGH",
                    f"genuine AI participation {ai_share:.1%} exceeds {AI_PARTICIPATION_HIGH:.0%} of recent opportunities",
                )
                flags += 1
            print(
                "BEHAVIOUR STATUS: HEALTHY | broad anomaly bounds not breached"
                if flags == 0 else f"BEHAVIOUR STATUS: WARNING | flags={flags}",
                flush=True,
            )

        integrity = db.get("outcome_integrity") or {}
        integrated_total = safe_int(integrity.get("integrated_total"), 0)
        integrated_complete = safe_int(integrity.get("integrated_complete"), 0)
        h30s = safe_int(integrity.get("h30s"), 0)
        h1m = safe_int(integrity.get("h1m"), 0)
        h5m = safe_int(integrity.get("h5m"), 0)
        h10m = safe_int(integrity.get("h10m"), 0)
        h30m = safe_int(integrity.get("h30m"), 0)
        h1h = safe_int(integrity.get("h1h"), 0)
        h4h = safe_int(integrity.get("h4h"), 0)
        h12h = safe_int(integrity.get("h12h"), 0)
        h24h = safe_int(integrity.get("h24h"), 0)
        mature_pending = safe_int(integrity.get("mature_pending"), 0)
        latest_outcome_update = integrity.get("latest_outcome_update")
        latest_update_age = seconds_old(latest_outcome_update, now)

        complete_delta = (
            None if _previous_integrated_complete is None
            else integrated_complete - _previous_integrated_complete
        )
        h24_delta = (
            None if _previous_integrated_h24 is None
            else h24h - _previous_integrated_h24
        )

        print(
            "OUTCOME INTEGRITY: "
            f"integrated_total={integrated_total} | complete={integrated_complete} | "
            f"h30s={h30s} (optional) | h1m={h1m} | h5m={h5m} | h10m={h10m} | "
            f"h30m={h30m} | h1h={h1h} | h4h={h4h} | h12h={h12h} | h24h={h24h} | "
            f"mature_pending={mature_pending} | latest_update={latest_outcome_update} | "
            f"update_age={None if latest_update_age is None else round(latest_update_age, 1)}s | "
            f"complete_delta={complete_delta} | h24_delta={h24_delta}",
            flush=True,
        )

        # Required completion contract begins at 1m. The 30s checkpoint is
        # optional/informational and must not make an otherwise complete record fail.
        if not (
            integrated_complete <= h24h <= h12h <= h4h <= h1h
            <= h30m <= h10m <= h5m <= h1m <= integrated_total
        ):
            add_warning(
                warnings,
                "OUTCOME_HORIZON_INTEGRITY",
                "required integrated outcome horizon counts violate expected monotonic ordering",
            )

        if complete_delta is not None and complete_delta < 0:
            add_warning(
                warnings,
                "OUTCOME_COMPLETE_COUNT_REVERSED",
                f"integrated COMPLETE count moved backwards by {abs(complete_delta)}",
            )
        if h24_delta is not None and h24_delta < 0:
            add_warning(
                warnings,
                "OUTCOME_24H_COUNT_REVERSED",
                f"integrated 24h population moved backwards by {abs(h24_delta)}",
            )

        if integrated_total > 0 and h1m == 0:
            add_warning(
                warnings,
                "OUTCOME_INTEGRATED_NOT_POPULATING",
                f"{integrated_total} integrated rows exist but no required 1m outcomes are populated",
            )

        _previous_integrated_complete = integrated_complete
        _previous_integrated_h24 = h24h

        print(
            "OUTCOME INTEGRITY STATUS: "
            + ("POPULATED" if h1m > 0 else "EMPTY")
            + " | read-only verification",
            flush=True,
        )

        drift = db.get("drift") or {}
        rn = safe_int(drift.get("recent_n"), 0)
        bn = safe_int(drift.get("baseline_n"), 0)
        rc = safe_float(drift.get("recent_avg_conf"))
        bc = safe_float(drift.get("baseline_avg_conf"))
        rl = safe_float(drift.get("recent_long_share"))
        bl = safe_float(drift.get("baseline_long_share"))
        ra = safe_float(drift.get("recent_ai_share"))
        ba = safe_float(drift.get("baseline_ai_share"))
        rmv = safe_int(drift.get("recent_model_versions"), 0)
        rsv = safe_int(drift.get("recent_strategy_versions"), 0)

        baseline_hours_effective = max(1, DRIFT_BASELINE_HOURS - DRIFT_RECENT_HOURS)
        recent_rate = rn / max(1, DRIFT_RECENT_HOURS)
        baseline_rate = bn / baseline_hours_effective
        volume_ratio = recent_rate / baseline_rate if baseline_rate > 0 else None

        print(
            "DRIFT SNAPSHOT: "
            f"recent_n={rn} | baseline_n={bn} | "
            f"recent_avg_conf={None if rc is None else round(rc, 2)} | "
            f"baseline_avg_conf={None if bc is None else round(bc, 2)} | "
            f"recent_LONG_share={None if rl is None else round(rl, 3)} | "
            f"baseline_LONG_share={None if bl is None else round(bl, 3)} | "
            f"recent_AI_share={None if ra is None else round(ra, 3)} | "
            f"baseline_AI_share={None if ba is None else round(ba, 3)} | "
            f"volume_ratio={None if volume_ratio is None else round(volume_ratio, 3)} | "
            f"model={drift.get('recent_model_version')} | strategy={drift.get('recent_strategy_version')}",
            flush=True,
        )

        if rn < DRIFT_MIN_RECENT_SAMPLE or bn < DRIFT_MIN_RECENT_SAMPLE:
            print(
                "DRIFT STATUS: OBSERVE ONLY | insufficient recent/baseline sample",
                flush=True,
            )
        else:
            drift_flags = 0
            if rmv > 1 or rsv > 1:
                add_warning(
                    warnings, "MODEL_STRATEGY_VERSION_MIX",
                    f"recent window contains model_versions={rmv}, strategy_versions={rsv}",
                )
                drift_flags += 1
            if rc is not None and bc is not None and abs(rc - bc) > DRIFT_CONFIDENCE_MEAN_DELTA:
                add_warning(
                    warnings, "CONFIDENCE_MEAN_DRIFT",
                    f"recent mean {rc:.2f} differs from baseline {bc:.2f} by {abs(rc-bc):.2f}",
                )
                drift_flags += 1
            if rl is not None and bl is not None and abs(rl - bl) > DRIFT_DIRECTION_SHARE_DELTA:
                add_warning(
                    warnings, "DIRECTION_SHARE_DRIFT",
                    f"recent LONG share {rl:.1%} differs from baseline {bl:.1%}",
                )
                drift_flags += 1
            if ra is not None and ba is not None and abs(ra - ba) > DRIFT_AI_SHARE_DELTA:
                add_warning(
                    warnings, "AI_PARTICIPATION_DRIFT",
                    f"recent AI share {ra:.1%} differs from baseline {ba:.1%}",
                )
                drift_flags += 1
            if volume_ratio is not None and (
                volume_ratio < DRIFT_VOLUME_RATIO_LOW or volume_ratio > DRIFT_VOLUME_RATIO_HIGH
            ):
                add_warning(
                    warnings, "OPPORTUNITY_VOLUME_DRIFT",
                    f"recent hourly opportunity rate is {volume_ratio:.2f}x historical baseline",
                )
                drift_flags += 1

            print(
                "DRIFT STATUS: HEALTHY | broad historical-baseline bounds not breached"
                if drift_flags == 0 else f"DRIFT STATUS: WARNING | flags={drift_flags}",
                flush=True,
            )
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

            # G1.5 worker/service health. These fields are written by Bot 2.0
            # into the existing signals2:health hash. Guardian remains read-only.
            memory_time = health.get("last_memory_cycle")
            memory_age = seconds_old(memory_time, now)
            memory_failures = safe_int(
                health.get("memory_failures_consecutive"), 0
            )
            memory_duration = safe_float(
                health.get("last_memory_duration_seconds")
            )

            if memory_time in (None, "", "None"):
                print(
                    "MEMORY WORKER: STARTUP/WAITING | "
                    "no completed memory cycle recorded yet",
                    flush=True,
                )
            else:
                memory_status = (
                    "HEALTHY"
                    if memory_age is not None and memory_age <= MEMORY_STALE_SECONDS
                    else "STALE"
                )
                print(
                    "MEMORY WORKER: "
                    f"status={memory_status} | last_cycle={memory_time} | "
                    f"age={None if memory_age is None else round(memory_age, 1)}s | "
                    f"duration={memory_duration}s | failures={memory_failures}",
                    flush=True,
                )
                if memory_age is None:
                    add_warning(
                        warnings,
                        "MEMORY_HEARTBEAT_INVALID",
                        "last_memory_cycle is invalid",
                    )
                elif memory_age > MEMORY_STALE_SECONDS:
                    add_warning(
                        warnings,
                        "MEMORY_HEARTBEAT_STALE",
                        f"last memory cycle is {memory_age:.0f}s old",
                    )

            if memory_failures >= 2:
                add_warning(
                    warnings,
                    "MEMORY_REPEATED_FAILURES",
                    f"Bot reports {memory_failures} consecutive memory failures",
                )

            outcome_started = health.get("outcome_worker_started")
            outcome_started_age = seconds_old(outcome_started, now)
            outcome_cycle = health.get("last_outcome_cycle")
            outcome_cycle_age = seconds_old(outcome_cycle, now)
            outcome_success = health.get("last_outcome_success")
            outcome_success_age = seconds_old(outcome_success, now)
            outcome_duration = safe_float(
                health.get("last_outcome_duration_seconds")
            )
            outcome_error = health.get("last_outcome_error")
            outcome_failures = safe_int(
                health.get("outcome_failures_consecutive"), 0
            )

            if outcome_started in (None, "", "None"):
                add_warning(
                    warnings,
                    "OUTCOME_WORKER_NOT_STARTED",
                    "Bot health has no outcome_worker_started timestamp",
                )
                print(
                    "OUTCOME WORKER: WARNING | start heartbeat missing",
                    flush=True,
                )
            elif outcome_cycle in (None, "", "None"):
                if (
                    outcome_started_age is not None
                    and outcome_started_age > OUTCOME_WORKER_START_GRACE_SECONDS
                ):
                    add_warning(
                        warnings,
                        "OUTCOME_WORKER_FIRST_CYCLE_OVERDUE",
                        "worker started "
                        f"{outcome_started_age:.0f}s ago but no cycle has started",
                    )
                    outcome_status = "OVERDUE"
                else:
                    outcome_status = "STARTUP/WAITING"
                print(
                    "OUTCOME WORKER: "
                    f"status={outcome_status} | started={outcome_started} | "
                    f"started_age={None if outcome_started_age is None else round(outcome_started_age, 1)}s | "
                    "last_cycle=None | last_success=None | "
                    f"failures={outcome_failures} | last_error={outcome_error}",
                    flush=True,
                )
            else:
                outcome_status = "HEALTHY"
                if outcome_cycle_age is None:
                    outcome_status = "INVALID"
                    add_warning(
                        warnings,
                        "OUTCOME_WORKER_HEARTBEAT_INVALID",
                        "last_outcome_cycle is invalid",
                    )
                elif outcome_cycle_age > OUTCOME_WORKER_STALE_SECONDS:
                    outcome_status = "STALE"
                    add_warning(
                        warnings,
                        "OUTCOME_WORKER_HEARTBEAT_STALE",
                        f"last outcome cycle is {outcome_cycle_age:.0f}s old",
                    )

                print(
                    "OUTCOME WORKER: "
                    f"status={outcome_status} | started={outcome_started} | "
                    f"last_cycle={outcome_cycle} | "
                    f"cycle_age={None if outcome_cycle_age is None else round(outcome_cycle_age, 1)}s | "
                    f"last_success={outcome_success} | "
                    f"success_age={None if outcome_success_age is None else round(outcome_success_age, 1)}s | "
                    f"duration={outcome_duration}s | failures={outcome_failures} | "
                    f"last_error={outcome_error}",
                    flush=True,
                )

            if outcome_failures >= OUTCOME_WORKER_FAILURE_LIMIT:
                add_warning(
                    warnings,
                    "OUTCOME_WORKER_REPEATED_FAILURES",
                    f"Bot reports {outcome_failures} consecutive outcome-worker failures",
                )

            # G1.6 conservative duration anomaly. First proven production cycle
            # was ~498s, so the default warning limit is deliberately 900s.
            if outcome_duration is not None and outcome_duration > OUTCOME_WORKER_SLOW_SECONDS:
                add_warning(
                    warnings,
                    "OUTCOME_WORKER_SLOW",
                    f"last outcome-worker duration {outcome_duration:.1f}s exceeds "
                    f"{OUTCOME_WORKER_SLOW_SECONDS:.0f}s limit",
                )

            if outcome_error not in (None, "", "None"):
                print(
                    f"OUTCOME WORKER LAST ERROR OBSERVED: {outcome_error}",
                    flush=True,
                )

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

        # G1.6 cross-service contradiction check. A single quiet minute is
        # normal; warn only when DB freshness is genuinely stale while the
        # scanner heartbeat itself remains fresh.
        if db["ok"] and health:
            g16_scan_age = seconds_old(health.get("last_scan"), now)
            latest_age = seconds_old(db.get("latest_created_at"), now)
            if (
                _no_growth_checks >= FLOW_CONTRADICTION_CHECKS
                and g16_scan_age is not None
                and g16_scan_age <= SCANNER_STALE_SECONDS
                and latest_age is not None
                and latest_age > OPPORTUNITY_STALE_SECONDS
            ):
                add_warning(
                    warnings,
                    "SCANNER_DB_FLOW_CONTRADICTION",
                    "scanner heartbeat is fresh but opportunity flow is stale "
                    f"for {latest_age:.0f}s",
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
