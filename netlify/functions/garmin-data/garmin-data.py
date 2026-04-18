import json, datetime, os, tempfile, logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.getLogger("garminconnect").setLevel(logging.CRITICAL)
logging.getLogger("garth").setLevel(logging.CRITICAL)

# ── Format helpers ──────────────────────────────────────────────

def hm(seconds):
    if not seconds: return "--"
    m = int(seconds) // 60
    h, mn = divmod(m, 60)
    return f"{h}h {mn:02d}m" if h else f"{mn}m"

def pace_fmt(sec_per_km):
    if not sec_per_km or sec_per_km <= 0: return "--"
    m, s = divmod(int(sec_per_km), 60)
    return f"{m}:{s:02d} /km"

def race_fmt(seconds):
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

def pct(part, total):
    if not total or not part: return "--%"
    return f"{round(100 * part / total)}%"

def ttl(s):
    return str(s).replace("_", " ").title()

def extract_rhr_value(rhr_data):
    if not rhr_data: return None
    try:
        return rhr_data["allMetrics"]["metricsMap"]["WELLNESS_RESTING_HEART_RATE"][0]["value"]
    except (KeyError, IndexError, TypeError):
        return rhr_data.get("restingHeartRate") or rhr_data.get("value")

def ts_to_dt(ts):
    if ts is None: return None
    ts = int(ts)
    if ts > 1e12: ts = ts // 1000
    return datetime.datetime.utcfromtimestamp(ts)

def find_bb_at(bb_array, target_dt, window_minutes=20):
    if not bb_array or not target_dt: return None
    target_ms = int(target_dt.timestamp() * 1000)
    best_val, best_diff = None, float("inf")
    for entry in bb_array:
        if isinstance(entry, list) and len(entry) >= 2:
            ts, val = entry[0], entry[1]
        elif isinstance(entry, dict):
            ts  = entry.get("startTimestampGMT") or entry.get("timestamp")
            val = entry.get("bodyBatteryLevel") or entry.get("value")
        else:
            continue
        if ts is None or val is None: continue
        diff = abs(int(ts) - target_ms)
        if diff < best_diff:
            best_diff, best_val = diff, val
    return best_val if best_diff <= window_minutes * 60 * 1000 else None


# ── Section formatters ──────────────────────────────────────────

def section_sleep(sleep_raw, hrv_raw):
    lines = ["SLEEP", "-" * 46]

    sl = (sleep_raw or {}).get("dailySleepDTO") or sleep_raw or {}
    total_s = sl.get("sleepTimeSeconds") or 0
    deep_s  = sl.get("deepSleepSeconds") or 0
    light_s = sl.get("lightSleepSeconds") or 0
    rem_s   = sl.get("remSleepSeconds") or 0
    awake_s = sl.get("awakeSleepSeconds") or 0

    score_obj = (sl.get("sleepScores") or {}).get("overall") or {}
    score     = score_obj.get("value") or sl.get("sleepScore") or "--"
    qual      = score_obj.get("qualifierKey", "")
    score_str = f"{score}/100  ({ttl(qual)})" if qual else f"{score}/100"

    lines.append(f"Score:           {score_str}")
    lines.append(f"Total Duration:  {hm(total_s)}")

    def ts_to_local_time(ts_local, ts_gmt):
        raw = ts_local or ts_gmt
        if not raw: return None
        try:
            dt = ts_to_dt(raw)
            return dt.strftime("%H:%M")
        except Exception:
            return None

    bedtime  = ts_to_local_time(sl.get("sleepStartTimestampLocal"), sl.get("sleepStartTimestampGMT"))
    waketime = ts_to_local_time(sl.get("sleepEndTimestampLocal"),   sl.get("sleepEndTimestampGMT"))
    if bedtime:  lines.append(f"Bedtime:         {bedtime}")
    if waketime: lines.append(f"Wake Time:       {waketime}")

    lines.append("")
    lines.append("Sleep Stages:")
    lines.append(f"  Deep:    {hm(deep_s):>8}   {pct(deep_s, total_s):>4}")
    lines.append(f"  Light:   {hm(light_s):>8}   {pct(light_s, total_s):>4}")
    lines.append(f"  REM:     {hm(rem_s):>8}   {pct(rem_s, total_s):>4}")
    lines.append(f"  Awake:   {hm(awake_s):>8}   {pct(awake_s, total_s):>4}")

    latency    = sl.get("sleepStartTimestampGMT")
    auto_sleep = sl.get("autoSleepStartTimestampGMT")
    if latency and auto_sleep:
        latency_min = abs(int(auto_sleep) - int(latency)) // 60000
        if latency_min < 120:
            lines.append(f"\nSleep Onset Latency: {latency_min} min")

    spo2_avg = sl.get("averageSpO2Value")
    spo2_low = sl.get("lowestSpO2Value")
    if spo2_avg or spo2_low:
        lines.append("")
        lines.append("SpO2 Overnight:")
        if spo2_avg: lines.append(f"  Average: {spo2_avg:.1f}%")
        if spo2_low: lines.append(f"  Lowest:  {spo2_low:.1f}%")

    resp_avg = sl.get("averageRespirationValue")
    resp_low = sl.get("lowestRespirationValue")
    resp_hi  = sl.get("highestRespirationValue")
    if resp_avg:
        lines.append("")
        lines.append("Respiration Overnight:")
        lines.append(f"  Average: {resp_avg:.1f} brpm")
        if resp_low and resp_hi:
            lines.append(f"  Range:   {resp_low:.1f} - {resp_hi:.1f} brpm")

    restless = sl.get("restlessMomentsCount") or sl.get("restlessScore")
    if restless is not None:
        lines.append(f"\nRestlessness Events: {restless}")

    hrv_readings = (hrv_raw or {}).get("hrvReadings") or []
    hrv_summary  = (hrv_raw or {}).get("hrvSummary") or hrv_raw or {}

    if hrv_readings:
        parsed = []
        for r in hrv_readings:
            v  = r.get("hrvValue")
            rt = r.get("readingTime") or r.get("startTimeGMT")
            if v is None or not rt: continue
            try:
                if isinstance(rt, str):
                    dt = datetime.datetime.fromisoformat(rt.replace(" ", "T").split(".")[0])
                else:
                    dt = ts_to_dt(rt)
                parsed.append((dt, int(v)))
            except Exception:
                continue
        parsed.sort(key=lambda x: x[0])

        if parsed:
            buckets = {}
            for dt, val in parsed:
                slot = dt.replace(minute=(dt.minute // 30) * 30, second=0, microsecond=0)
                buckets.setdefault(slot, []).append(val)

            all_vals = [v for _, v in parsed]
            avg_hrv  = round(sum(all_vals) / len(all_vals))
            min_hrv  = min(all_vals)
            min_time = min(parsed, key=lambda x: x[1])[0].strftime("%H:%M")

            lines.append("")
            lines.append("HRV Overnight - RMSSD at 30-min intervals:")
            for slot in sorted(buckets):
                slot_avg = round(sum(buckets[slot]) / len(buckets[slot]))
                lines.append(f"  {slot.strftime('%H:%M')}   {slot_avg} ms")
            lines.append(f"\n  Overnight Average:  {avg_hrv} ms")
            lines.append(f"  Lowest Point:       {min_hrv} ms  (at {min_time})")

    elif hrv_summary:
        status   = hrv_summary.get("status") or hrv_summary.get("lastNightStatus") or "--"
        last_n   = hrv_summary.get("lastNight")
        week_avg = hrv_summary.get("weeklyAvg")
        lines.append("")
        lines.append("HRV Overnight:")
        lines.append(f"  Status:          {ttl(status)}")
        if last_n:   lines.append(f"  Last Night Avg:  {last_n} ms")
        if week_avg: lines.append(f"  5-Night Avg:     {week_avg} ms")

    return lines


def section_readiness(bb_raw, stress_raw, rhr_today_raw, rhr_week_vals, sleep_raw, cutoff_ms=None):
    lines = ["", "READINESS", "-" * 46]

    bb_item = (bb_raw[0] if isinstance(bb_raw, list) and bb_raw else bb_raw) or {}
    bb_array = bb_item.get("bodyBatteryValuesArray") or []

    sleep_dto      = (sleep_raw or {}).get("dailySleepDTO") or sleep_raw or {}
    sleep_start_ts = sleep_dto.get("sleepStartTimestampGMT")
    sleep_end_ts   = sleep_dto.get("sleepEndTimestampGMT")
    sleep_start_dt = ts_to_dt(sleep_start_ts) if sleep_start_ts else None
    sleep_end_dt   = ts_to_dt(sleep_end_ts)   if sleep_end_ts   else None

    lines.append("Body Battery:")
    if bb_array:
        entries = []
        for e in bb_array:
            if isinstance(e, list) and len(e) >= 2:
                entries.append((e[0], e[1]))
            elif isinstance(e, dict):
                ts  = e.get("startTimestampGMT") or e.get("timestamp")
                val = e.get("bodyBatteryLevel") or e.get("value")
                if ts and val is not None:
                    entries.append((int(ts), val))
        if entries:
            entries.sort()
            if cutoff_ms:
                filtered = [(ts, v) for ts, v in entries if ts <= cutoff_ms]
                entries = filtered if filtered else entries
            all_vals = [v for _, v in entries]

            at_sleep_start  = find_bb_at(bb_array, sleep_start_dt, 30) if sleep_start_dt else None
            at_wake         = find_bb_at(bb_array, sleep_end_dt,   30) if sleep_end_dt   else None
            current         = all_vals[-1]

            peak_during_sleep = None
            if sleep_start_dt and sleep_end_dt:
                s_ms = int(sleep_start_dt.timestamp() * 1000)
                e_ms = int(sleep_end_dt.timestamp()   * 1000)
                sleep_vals = [v for ts, v in entries if s_ms <= ts <= e_ms]
                if sleep_vals:
                    peak_during_sleep = max(sleep_vals)

            if at_sleep_start is not None: lines.append(f"  At Sleep Start:    {at_sleep_start}")
            if peak_during_sleep is not None: lines.append(f"  Peak Overnight:    {peak_during_sleep}")
            if at_wake is not None: lines.append(f"  At Wake-Up:        {at_wake}")
            lines.append(f"  Current:           {current}")
        else:
            charged = bb_item.get("charged")
            drained = bb_item.get("drained")
            if charged: lines.append(f"  Charged to:  {charged}")
            if drained: lines.append(f"  Drained:     {drained}")
    else:
        lines.append("  No data available")

    lines.append("")
    lines.append("Stress:")
    if stress_raw:
        overall = stress_raw.get("overallStressLevel") or stress_raw.get("avgStressLevel")
        rest_s  = stress_raw.get("restStressDuration") or 0
        low_s   = stress_raw.get("lowStressDuration") or 0
        med_s   = stress_raw.get("mediumStressDuration") or 0
        high_s  = stress_raw.get("highStressDuration") or 0
        act_s   = stress_raw.get("activityStressDuration") or 0

        def stress_label(v):
            if v is None: return "--"
            if v < 26:    return f"{v}  (Low)"
            if v < 51:    return f"{v}  (Medium)"
            if v < 76:    return f"{v}  (High)"
            return        f"{v}  (Very High)"

        stress_arr = stress_raw.get("stressValuesArray") or []
        if cutoff_ms and stress_arr:
            cutoff_vals = [e[1] for e in stress_arr
                           if isinstance(e, list) and len(e) >= 2
                           and e[1] is not None and e[1] >= 0
                           and int(e[0]) <= cutoff_ms]
            if cutoff_vals:
                overall = round(sum(cutoff_vals) / len(cutoff_vals))

        if overall is not None:
            lines.append(f"  Overall (24hr avg):  {stress_label(overall)}")

        if stress_arr and sleep_start_dt and sleep_end_dt:
            s_ms = int(sleep_start_dt.timestamp() * 1000)
            e_ms = int(sleep_end_dt.timestamp()   * 1000)
            overnight_vals = [
                e[1] for e in stress_arr
                if isinstance(e, list) and len(e) >= 2
                and e[1] is not None and e[1] >= 0
                and s_ms <= int(e[0]) <= e_ms
            ]
            if overnight_vals:
                on_avg = round(sum(overnight_vals) / len(overnight_vals))
                lines.append(f"  Overnight Average:   {stress_label(on_avg)}")

        total_tracked = rest_s + low_s + med_s + high_s + act_s
        if total_tracked > 0:
            lines.append("  Last 24hrs Breakdown:")
            if rest_s:  lines.append(f"    Rest:         {hm(rest_s)}")
            if low_s:   lines.append(f"    Low Stress:   {hm(low_s)}")
            if med_s:   lines.append(f"    Med Stress:   {hm(med_s)}")
            if high_s:  lines.append(f"    High Stress:  {hm(high_s)}")
            if act_s:   lines.append(f"    Activity:     {hm(act_s)}")
    else:
        lines.append("  No data available")

    lines.append("")
    lines.append("Resting HR:")
    rhr_val = extract_rhr_value(rhr_today_raw)
    if rhr_val:
        lines.append(f"  Last Night:    {rhr_val} bpm")
    else:
        lines.append("  Last Night:    No data")

    if rhr_week_vals:
        week_avg = round(sum(rhr_week_vals) / len(rhr_week_vals))
        lines.append(f"  7-Day Average: {week_avg} bpm  ({len(rhr_week_vals)} days)")
        if rhr_val:
            diff = rhr_val - week_avg
            sign = "+" if diff > 0 else ""
            lines.append(f"  vs Avg:        {sign}{diff} bpm")

    return lines


def section_fitness(mm_now, mm_4w, ts_raw, lactate_raw, race_raw, readiness_raw):
    lines = ["", "FITNESS", "-" * 46]

    def extract_vo2(mm):
        if not mm: return None, None
        items = mm if isinstance(mm, list) else [mm]
        for item in items:
            if not isinstance(item, dict): continue
            generic = item.get("generic") or {}
            vo2 = (generic.get("vo2MaxPreciseValue") or
                   generic.get("vo2MaxValue") or
                   generic.get("VO2MaxValue"))
            if vo2:
                return float(vo2), generic.get("fitnessAgeDescription") or generic.get("fitnessLevel")
        return None, None

    vo2_now, lvl_now = extract_vo2(mm_now)
    vo2_4w,  _       = extract_vo2(mm_4w)

    lines.append("VO2max:")
    if vo2_now:
        lvl_str = f"  ({ttl(lvl_now)})" if lvl_now else ""
        lines.append(f"  Current:     {vo2_now:.1f} mL/kg/min{lvl_str}")
    else:
        lines.append("  Current:     No data")
    if vo2_4w:
        diff     = round(vo2_now - vo2_4w, 1) if vo2_now else None
        sign     = "+" if diff and diff > 0 else ""
        diff_str = f"  ({sign}{diff} over 4 wks)" if diff is not None else ""
        lines.append(f"  4 Weeks Ago: {vo2_4w:.1f} mL/kg/min{diff_str}")
    else:
        lines.append("  4 Weeks Ago: No data")

    lines.append("")
    if isinstance(ts_raw, list):
        ts_raw = ts_raw[0] if ts_raw else None
    ts_obj    = (ts_raw or {}).get("mostRecentTrainingStatus") or {}
    ts_type   = ts_obj.get("trainingStatusType") or ts_obj.get("status")
    lines.append(f"Training Status: {ttl(ts_type) if ts_type else 'No data'}")

    ts_feedback = ts_obj.get("trainingStatusFeedbackPhrase") or ts_obj.get("feedback")
    if ts_feedback:
        lines.append(f"  ({ttl(ts_feedback)})")

    readiness_list = readiness_raw if isinstance(readiness_raw, list) else ([readiness_raw] if readiness_raw else [])
    tr = next((r for r in readiness_list if isinstance(r, dict) and r.get("primaryActivityTracker")), None) \
         or (readiness_list[0] if readiness_list else {})

    acute_load = tr.get("acuteLoad") if tr else None
    acwr_pct   = tr.get("acwrFactorPercent") if tr else None

    tlb_map = ((ts_raw or {}).get("mostRecentTrainingLoadBalance") or {}) \
                              .get("metricsTrainingLoadBalanceDTOMap") or {}
    tlb = next(iter(tlb_map.values()), {}) if tlb_map else {}
    aero_low  = tlb.get("monthlyLoadAerobicLow")
    aero_high = tlb.get("monthlyLoadAerobicHigh")
    anaerobic = tlb.get("monthlyLoadAnaerobic")
    tlb_phrase = tlb.get("trainingBalanceFeedbackPhrase")

    if acute_load is not None:
        lines.append("")
        lines.append("Training Load:")
        lines.append(f"  Acute (7-day):    {round(acute_load)}")
        if acwr_pct is not None:
            ratio = acwr_pct / 100
            if ratio < 0.8:     rlabel = "Low -- possible detraining"
            elif ratio <= 1.0:  rlabel = "Maintenance"
            elif ratio <= 1.3:  rlabel = "Optimal"
            elif ratio <= 1.5:  rlabel = "High"
            else:               rlabel = "Very High -- overreaching risk"
            lines.append(f"  Acute/Chronic:    {ratio:.2f}  ({rlabel})")

    if any(v is not None for v in [aero_low, aero_high, anaerobic]):
        total = (aero_low or 0) + (aero_high or 0) + (anaerobic or 0)
        lines.append("")
        lines.append("Load Focus (last 4 weeks):")
        if aero_low  is not None: lines.append(f"  Aerobic Base:  {round(aero_low / total * 100) if total else '--'}%  ({round(aero_low)} AU)")
        if aero_high is not None: lines.append(f"  Aerobic Peak:  {round(aero_high / total * 100) if total else '--'}%  ({round(aero_high)} AU)")
        if anaerobic is not None: lines.append(f"  Anaerobic:     {round(anaerobic / total * 100) if total else '--'}%  ({round(anaerobic)} AU)")
        if tlb_phrase:            lines.append(f"  Balance Note:  {ttl(tlb_phrase)}")

    lines.append("")
    lines.append("Lactate Threshold:")
    if lactate_raw:
        lt_hr  = (lactate_raw.get("lactateThresholdHeartRate") or
                  lactate_raw.get("heartRate") or lactate_raw.get("hrBpm"))
        lt_p   = lactate_raw.get("pacePer1000mInSeconds") or lactate_raw.get("paceInSeconds")
        lt_spd = lactate_raw.get("maxSpeedInMetersPerSecond")
        lines.append(f"  Heart Rate:  {lt_hr} bpm" if lt_hr else "  Heart Rate:  No data")
        if lt_p:
            lines.append(f"  Pace:        {pace_fmt(lt_p)}")
        elif lt_spd and lt_spd > 0:
            lines.append(f"  Pace:        {pace_fmt(1000 / lt_spd)}")
        else:
            lines.append("  Pace:        No data")
    else:
        lines.append("  Not available via API for this device")

    lines.append("")
    lines.append("Race Predictions:")
    if isinstance(race_raw, dict) and race_raw.get("time5K"):
        lines.append(f"  {'5K:':<16} {race_fmt(race_raw['time5K'])}")
        if race_raw.get("time10K"):
            lines.append(f"  {'10K:':<16} {race_fmt(race_raw['time10K'])}")
        if race_raw.get("timeHalfMarathon"):
            lines.append(f"  {'Half Marathon:':<16} {race_fmt(race_raw['timeHalfMarathon'])}")
        if race_raw.get("timeMarathon"):
            lines.append(f"  {'Marathon:':<16} {race_fmt(race_raw['timeMarathon'])}")
    else:
        lines.append("  No data available")

    return lines


def section_recovery(readiness_raw):
    lines = ["", "RECOVERY", "-" * 46]

    readiness_list = readiness_raw if isinstance(readiness_raw, list) else ([readiness_raw] if readiness_raw else [])

    post_exercise = [r for r in readiness_list if isinstance(r, dict)
                     and r.get("inputContext") == "AFTER_POST_EXERCISE_RESET"]
    wakeup        = [r for r in readiness_list if isinstance(r, dict)
                     and r.get("inputContext") == "AFTER_WAKEUP_RESET"]

    tr_score = (wakeup[0]       if wakeup        else readiness_list[0] if readiness_list else {})
    tr_rec   = (post_exercise[0] if post_exercise else readiness_list[0] if readiness_list else {})

    score    = tr_score.get("score")
    level    = tr_score.get("level")
    feedback = tr_score.get("feedbackShort") or tr_score.get("feedbackLong")

    if score is not None:
        lvl_str = f"  ({ttl(level)})" if level else ""
        lines.append(f"Readiness Score:  {score}/100{lvl_str}")
    if feedback:
        lines.append(f"Readiness Status: {ttl(feedback)}")

    rec_min = tr_rec.get("recoveryTime")
    rec_ts  = tr_rec.get("timestamp") or tr_rec.get("timestampLocal")
    if rec_min is not None and rec_min > 1:
        initial_hours = rec_min / 60
        elapsed_hours = 0
        if rec_ts:
            try:
                rec_dt = datetime.datetime.fromisoformat(str(rec_ts).replace(" ", "T").split(".")[0])
                elapsed_hours = (datetime.datetime.utcnow() - rec_dt).total_seconds() / 3600
            except Exception:
                pass
        remaining = max(0, round(initial_hours - elapsed_hours))
        lines.append(f"Recovery Time:    {remaining}h remaining  (started at {round(initial_hours)}h)")
    else:
        lines.append("Recovery Time:    Fully recovered")

    hrv_pct = tr_score.get("hrvFactorPercent")
    hrv_wk  = tr_score.get("hrvWeeklyAverage")
    if hrv_pct is not None: lines.append(f"HRV Factor:       {hrv_pct}%  (vs 7-day baseline)")
    if hrv_wk  is not None: lines.append(f"HRV Weekly Avg:   {hrv_wk} ms")

    return lines


# ── Parallel data fetch ─────────────────────────────────────────

def fetch_all_parallel(client, today, yesterday):
    t  = today.isoformat()
    y  = yesterday.isoformat()
    fw = (today - datetime.timedelta(weeks=4)).isoformat()

    def safe(*fns):
        for fn in fns:
            try:
                return fn()
            except Exception:
                continue
        return None

    # Capture string values in default args to avoid closure issues
    task_map = {
        "sleep":     lambda t=t, y=y: safe(lambda: client.get_sleep_data(t), lambda: client.get_sleep_data(y)),
        "hrv":       lambda t=t, y=y: safe(lambda: client.get_hrv_data(t), lambda: client.get_hrv_data(y)),
        "bb":        lambda t=t, y=y: safe(lambda: client.get_body_battery(t), lambda: client.get_body_battery(y)),
        "stress":    lambda t=t, y=y: safe(lambda: client.get_stress_data(t), lambda: client.get_stress_data(y)),
        "rhr_today": lambda t=t, y=y: safe(lambda: client.get_rhr_day(t), lambda: client.get_rhr_day(y)),
        "mm_now":    lambda t=t, y=y: safe(lambda: client.get_max_metrics(t), lambda: client.get_max_metrics(y)),
        "mm_4w":     lambda fw=fw:    safe(lambda: client.get_max_metrics(fw)),
        "ts":        lambda t=t, y=y: safe(lambda: client.get_training_status(t), lambda: client.get_training_status(y)),
        "readiness": lambda t=t, y=y: safe(lambda: client.get_training_readiness(t), lambda: client.get_training_readiness(y)),
        "race":      lambda: safe(lambda: client.get_race_predictions()),
        "lactate":   lambda: safe(
            lambda: client.connectapi("/biometric-service/lactateThreshold"),
            lambda: client.connectapi("/userprofile-service/userprofile/personal-information/lactateThreshold"),
        ),
    }

    def get_rhr_day(d):
        try:
            return extract_rhr_value(client.get_rhr_day(d))
        except Exception:
            return None

    rhr_days = [(today - datetime.timedelta(days=i)).isoformat() for i in range(7)]

    results = {}
    rhr_vals = []

    with ThreadPoolExecutor(max_workers=14) as executor:
        task_futures = {executor.submit(fn): key for key, fn in task_map.items()}
        rhr_futures  = [executor.submit(get_rhr_day, d) for d in rhr_days]

        for future in as_completed(task_futures):
            key = task_futures[future]
            try:
                results[key] = future.result()
            except Exception:
                results[key] = None

        for future in as_completed(rhr_futures):
            try:
                v = future.result()
                if v: rhr_vals.append(v)
            except Exception:
                pass

    results["rhr_week"] = rhr_vals
    return results


# ── Handler ─────────────────────────────────────────────────────

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

    email        = body.get("email", "").strip()
    password     = body.get("password", "")
    token_data   = body.get("token_data")       # {filename: json_content}
    report_date_str  = body.get("report_date")  # "YYYY-MM-DD"
    cutoff_ms_raw    = body.get("cutoff_ms")    # epoch ms integer (UTC)

    # Parse report date
    try:
        report_date = datetime.date.fromisoformat(report_date_str) if report_date_str else datetime.date.today()
    except (ValueError, TypeError):
        report_date = datetime.date.today()

    # cutoff_ms for timestamp filtering; cutoff_dt for display
    cutoff_ms = int(cutoff_ms_raw) if cutoff_ms_raw else None
    if cutoff_ms:
        cutoff_dt = datetime.datetime.utcfromtimestamp(cutoff_ms / 1000)
    else:
        cutoff_dt = datetime.datetime.utcnow()

    yesterday = report_date - datetime.timedelta(days=1)

    if not token_data and (not email or not password):
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

    tmp_dir = tempfile.mkdtemp()
    new_token_data = {}

    try:
        client = Garmin(email=email or "", password=password or "")

        if token_data:
            for fname, content in token_data.items():
                fpath = os.path.join(tmp_dir, fname)
                with open(fpath, "w") as f:
                    if isinstance(content, str):
                        f.write(content)
                    else:
                        json.dump(content, f)
            try:
                client.login(tmp_dir)
            except Exception:
                if not email or not password:
                    return {
                        "statusCode": 401,
                        "headers": cors,
                        "body": json.dumps({"error": "Garmin session expired. Please reconnect with your credentials."}),
                    }
                client = Garmin(email=email, password=password)
                client.login()
        else:
            client.login()

        try:
            client.garth.dump(tmp_dir)
            for fname in os.listdir(tmp_dir):
                fpath = os.path.join(tmp_dir, fname)
                if os.path.isfile(fpath):
                    with open(fpath) as f:
                        try:
                            new_token_data[fname] = json.load(f)
                        except json.JSONDecodeError:
                            new_token_data[fname] = f.read()
        except Exception:
            new_token_data = token_data or {}

    except GarminConnectAuthenticationError as e:
        return {"statusCode": 401, "headers": cors,
                "body": json.dumps({"error": f"Garmin authentication failed: {e}"})}
    except GarminConnectTooManyRequestsError:
        return {"statusCode": 429, "headers": cors,
                "body": json.dumps({"error": "Garmin rate limit — try again in a few minutes"})}
    except GarminConnectConnectionError as e:
        return {"statusCode": 502, "headers": cors,
                "body": json.dumps({"error": f"Garmin connection error: {e}"})}
    except Exception as e:
        return {"statusCode": 500, "headers": cors,
                "body": json.dumps({"error": f"Garmin login error: {e}"})}

    try:
        data = fetch_all_parallel(client, report_date, yesterday)
    except Exception as e:
        return {"statusCode": 500, "headers": cors,
                "body": json.dumps({"error": f"Data fetch error: {e}"})}

    # Build report
    is_now     = cutoff_ms is None or abs((cutoff_dt - datetime.datetime.utcnow()).total_seconds()) < 120
    date_str   = report_date.strftime("%A, %d %B %Y")
    period_str = f"as of {cutoff_dt.strftime('%H:%M')} UTC" if not is_now else f"Generated {datetime.datetime.utcnow().strftime('%H:%M')} UTC"

    header = [
        "",
        "=" * 46,
        "  GARMIN CONNECT - DAILY HEALTH REPORT",
        f"  {date_str} | {period_str}",
        "=" * 46,
        "",
    ]

    all_sections = (
        section_sleep(data.get("sleep"), data.get("hrv")) +
        section_readiness(data.get("bb"), data.get("stress"), data.get("rhr_today"),
                          data.get("rhr_week", []), data.get("sleep"), cutoff_ms) +
        section_fitness(data.get("mm_now"), data.get("mm_4w"), data.get("ts"),
                        data.get("lactate"), data.get("race"), data.get("readiness")) +
        section_recovery(data.get("readiness"))
    )

    report_text = "\n".join(header + all_sections + ["", "=" * 46, ""])

    return {
        "statusCode": 200,
        "headers": cors,
        "body": json.dumps({"report_text": report_text, "token_data": new_token_data}, default=str),
    }
