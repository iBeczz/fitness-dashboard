import json
import datetime

def handler(event, context):
    cors = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }

    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": cors, "body": ""}

    try:
        body = json.loads(event.get("body") or "{}")
    except Exception:
        body = {}

    email = body.get("email", "").strip()
    password = body.get("password", "")
    mfa_code = body.get("mfa_code", "").strip() or None

    if not email or not password:
        return {
            "statusCode": 400,
            "headers": cors,
            "body": json.dumps({"error": "Garmin email and password are required"}),
        }

    try:
        from garminconnect import (
            Garmin,
            GarminConnectAuthenticationError,
            GarminConnectConnectionError,
            GarminConnectTooManyRequestsError,
        )
    except ImportError as e:
        return {
            "statusCode": 500,
            "headers": cors,
            "body": json.dumps({"error": f"garminconnect library not available: {e}"}),
        }

    try:
        if mfa_code:
            client = Garmin(email=email, password=password, prompt_mfa=lambda: mfa_code)
        else:
            client = Garmin(email=email, password=password)
        client.login()
    except GarminConnectAuthenticationError as e:
        return {
            "statusCode": 401,
            "headers": cors,
            "body": json.dumps({"error": f"Garmin authentication failed: {e}. If you have 2FA enabled, provide your MFA code."}),
        }
    except GarminConnectTooManyRequestsError:
        return {
            "statusCode": 429,
            "headers": cors,
            "body": json.dumps({"error": "Garmin rate limit hit — try again in a few minutes"}),
        }
    except GarminConnectConnectionError as e:
        return {
            "statusCode": 502,
            "headers": cors,
            "body": json.dumps({"error": f"Garmin connection error: {e}"}),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": cors,
            "body": json.dumps({"error": f"Garmin login error: {e}"}),
        }

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    today_str = today.isoformat()
    yesterday_str = yesterday.isoformat()

    data = {}

    def safe(key, fn, fallback_fn=None):
        try:
            data[key] = fn()
        except Exception as e1:
            if fallback_fn:
                try:
                    data[key] = fallback_fn()
                    return
                except Exception:
                    pass
            data[f"{key}_error"] = str(e1)

    # Sleep — try today, fall back to yesterday
    safe(
        "sleep",
        lambda: client.get_sleep_data(today_str),
        lambda: client.get_sleep_data(yesterday_str),
    )

    # HRV
    safe(
        "hrv",
        lambda: client.get_hrv_data(today_str),
        lambda: client.get_hrv_data(yesterday_str),
    )

    # Resting HR
    safe(
        "rhr",
        lambda: client.get_rhr_day(today_str),
        lambda: client.get_rhr_day(yesterday_str),
    )

    # Body Battery
    safe(
        "body_battery",
        lambda: client.get_body_battery(today_str),
        lambda: client.get_body_battery(yesterday_str),
    )

    # Stress
    safe(
        "stress",
        lambda: client.get_stress_data(today_str),
        lambda: client.get_stress_data(yesterday_str),
    )

    # VO2max / max metrics
    safe(
        "max_metrics",
        lambda: client.get_max_metrics(today_str),
        lambda: client.get_max_metrics(yesterday_str),
    )

    # Training status
    safe(
        "training_status",
        lambda: client.get_training_status(today_str),
        lambda: client.get_training_status(yesterday_str),
    )

    # Training readiness (includes recovery time)
    safe(
        "training_readiness",
        lambda: client.get_training_readiness(today_str),
        lambda: client.get_training_readiness(yesterday_str),
    )

    # Race predictions
    safe("race_predictions", lambda: client.get_race_predictions())

    # Lactate threshold — try named method, fall back to direct API endpoint
    def get_lactate():
        return client.connectapi("/biometric-service/lactateThreshold")

    def get_lactate_alt():
        # Some versions expose it differently
        return client.connectapi(
            f"/wellness-service/wellness/dailyPerformanceMetrics?startDate={yesterday_str}&endDate={today_str}"
        )

    safe("lactate_threshold", get_lactate, get_lactate_alt)

    return {
        "statusCode": 200,
        "headers": cors,
        "body": json.dumps(data, default=str),
    }
