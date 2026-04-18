'use strict';
const { GarminConnect } = require('garmin-connect');

// ── Format helpers ──────────────────────────────────────────────

function hm(seconds) {
  if (!seconds) return '--';
  const m  = Math.floor(Number(seconds) / 60);
  const h  = Math.floor(m / 60);
  const mn = m % 60;
  return h ? `${h}h ${String(mn).padStart(2, '0')}m` : `${mn}m`;
}

function paceFmt(secPerKm) {
  if (!secPerKm || secPerKm <= 0) return '--';
  const m = Math.floor(secPerKm / 60);
  const s = Math.round(secPerKm % 60);
  return `${m}:${String(s).padStart(2, '0')} /km`;
}

function raceFmt(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.round(seconds % 60);
  return h
    ? `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`
    : `${m}:${String(s).padStart(2, '0')}`;
}

function pct(part, total) {
  if (!total || !part) return '--%';
  return `${Math.round(100 * part / total)}%`;
}

function ttl(s) {
  if (!s) return '';
  return String(s).replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function extractRhrValue(rhrData) {
  if (!rhrData) return null;
  try {
    return rhrData.allMetrics.metricsMap.WELLNESS_RESTING_HEART_RATE[0].value;
  } catch {
    return rhrData.restingHeartRate || rhrData.value || null;
  }
}

function tsToDt(ts) {
  if (ts == null) return null;
  ts = Number(ts);
  if (ts > 1e12) ts = Math.floor(ts / 1000);
  return new Date(ts * 1000);
}

function findBbAt(bbArray, targetMs, windowMinutes = 20) {
  if (!bbArray || !targetMs) return null;
  let bestVal = null, bestDiff = Infinity;
  for (const entry of bbArray) {
    let ts, val;
    if (Array.isArray(entry) && entry.length >= 2) {
      [ts, val] = entry;
    } else if (entry && typeof entry === 'object') {
      ts  = entry.startTimestampGMT || entry.timestamp;
      val = entry.bodyBatteryLevel ?? entry.value;
    } else continue;
    if (ts == null || val == null) continue;
    const diff = Math.abs(Number(ts) - targetMs);
    if (diff < bestDiff) { bestDiff = diff; bestVal = val; }
  }
  return bestVal !== null && bestDiff <= windowMinutes * 60 * 1000 ? bestVal : null;
}

// ── Section formatters ──────────────────────────────────────────

function sectionSleep(sleepRaw, hrvRaw) {
  const lines = ['SLEEP', '-'.repeat(46)];

  const sl     = (sleepRaw || {}).dailySleepDTO || sleepRaw || {};
  const totalS = sl.sleepTimeSeconds || 0;
  const deepS  = sl.deepSleepSeconds || 0;
  const lightS = sl.lightSleepSeconds || 0;
  const remS   = sl.remSleepSeconds || 0;
  const awakeS = sl.awakeSleepSeconds || 0;

  const scoreObj = ((sl.sleepScores || {}).overall) || {};
  const score    = scoreObj.value || sl.sleepScore || '--';
  const qual     = scoreObj.qualifierKey || '';
  const scoreStr = qual ? `${score}/100  (${ttl(qual)})` : `${score}/100`;

  lines.push(`Score:           ${scoreStr}`);
  lines.push(`Total Duration:  ${hm(totalS)}`);

  function tsToUtcTime(ts) {
    if (!ts) return null;
    try {
      const dt = tsToDt(ts);
      return dt ? dt.toISOString().slice(11, 16) : null;
    } catch { return null; }
  }

  const bedtime  = tsToUtcTime(sl.sleepStartTimestampLocal || sl.sleepStartTimestampGMT);
  const waketime = tsToUtcTime(sl.sleepEndTimestampLocal   || sl.sleepEndTimestampGMT);
  if (bedtime)  lines.push(`Bedtime:         ${bedtime}`);
  if (waketime) lines.push(`Wake Time:       ${waketime}`);

  lines.push('');
  lines.push('Sleep Stages:');
  lines.push(`  Deep:    ${hm(deepS).padStart(8)}   ${pct(deepS, totalS).padStart(4)}`);
  lines.push(`  Light:   ${hm(lightS).padStart(8)}   ${pct(lightS, totalS).padStart(4)}`);
  lines.push(`  REM:     ${hm(remS).padStart(8)}   ${pct(remS, totalS).padStart(4)}`);
  lines.push(`  Awake:   ${hm(awakeS).padStart(8)}   ${pct(awakeS, totalS).padStart(4)}`);

  const latency   = sl.sleepStartTimestampGMT;
  const autoSleep = sl.autoSleepStartTimestampGMT;
  if (latency && autoSleep) {
    const latencyMin = Math.abs(Number(autoSleep) - Number(latency)) / 60000;
    if (latencyMin < 120) lines.push(`\nSleep Onset Latency: ${Math.round(latencyMin)} min`);
  }

  const spo2Avg = sl.averageSpO2Value;
  const spo2Low = sl.lowestSpO2Value;
  if (spo2Avg || spo2Low) {
    lines.push('');
    lines.push('SpO2 Overnight:');
    if (spo2Avg != null) lines.push(`  Average: ${Number(spo2Avg).toFixed(1)}%`);
    if (spo2Low != null) lines.push(`  Lowest:  ${Number(spo2Low).toFixed(1)}%`);
  }

  const respAvg = sl.averageRespirationValue;
  const respLow = sl.lowestRespirationValue;
  const respHi  = sl.highestRespirationValue;
  if (respAvg) {
    lines.push('');
    lines.push('Respiration Overnight:');
    lines.push(`  Average: ${Number(respAvg).toFixed(1)} brpm`);
    if (respLow && respHi) lines.push(`  Range:   ${Number(respLow).toFixed(1)} - ${Number(respHi).toFixed(1)} brpm`);
  }

  const restless = sl.restlessMomentsCount ?? sl.restlessScore;
  if (restless != null) lines.push(`\nRestlessness Events: ${restless}`);

  const hrvReadings = (hrvRaw || {}).hrvReadings || [];
  const hrvSummary  = (hrvRaw || {}).hrvSummary || {};

  if (hrvReadings.length) {
    const parsed = [];
    for (const r of hrvReadings) {
      const v  = r.hrvValue;
      const rt = r.readingTime || r.startTimeGMT;
      if (v == null || !rt) continue;
      try {
        const dt = typeof rt === 'string'
          ? new Date(rt.replace(' ', 'T').split('.')[0] + 'Z')
          : tsToDt(rt);
        if (dt) parsed.push([dt, Math.round(Number(v))]);
      } catch {}
    }
    parsed.sort((a, b) => a[0] - b[0]);

    if (parsed.length) {
      const buckets = new Map();
      for (const [dt, val] of parsed) {
        const slot = new Date(dt);
        slot.setUTCMinutes(Math.floor(slot.getUTCMinutes() / 30) * 30, 0, 0);
        const key = slot.toISOString();
        if (!buckets.has(key)) buckets.set(key, { slot, vals: [] });
        buckets.get(key).vals.push(val);
      }

      const allVals = parsed.map(([, v]) => v);
      const avgHrv  = Math.round(allVals.reduce((a, b) => a + b, 0) / allVals.length);
      const minHrv  = Math.min(...allVals);
      const minTime = parsed.find(([, v]) => v === minHrv)[0].toISOString().slice(11, 16);

      lines.push('');
      lines.push('HRV Overnight - RMSSD at 30-min intervals:');
      for (const [key, { slot, vals }] of [...buckets].sort(([a], [b]) => a.localeCompare(b))) {
        const slotAvg = Math.round(vals.reduce((a, b) => a + b, 0) / vals.length);
        lines.push(`  ${slot.toISOString().slice(11, 16)}   ${slotAvg} ms`);
      }
      lines.push(`\n  Overnight Average:  ${avgHrv} ms`);
      lines.push(`  Lowest Point:       ${minHrv} ms  (at ${minTime})`);
    }
  } else if (Object.keys(hrvSummary).length) {
    const status  = hrvSummary.status || hrvSummary.lastNightStatus || '--';
    const lastN   = hrvSummary.lastNight;
    const weekAvg = hrvSummary.weeklyAvg;
    lines.push('');
    lines.push('HRV Overnight:');
    lines.push(`  Status:          ${ttl(status)}`);
    if (lastN)   lines.push(`  Last Night Avg:  ${lastN} ms`);
    if (weekAvg) lines.push(`  5-Night Avg:     ${weekAvg} ms`);
  }

  return lines;
}

function sectionReadiness(bbRaw, stressRaw, rhrTodayRaw, rhrWeekVals, sleepRaw, cutoffMs) {
  const lines = ['', 'READINESS', '-'.repeat(46)];

  const bbItem  = (Array.isArray(bbRaw) && bbRaw.length) ? bbRaw[0] : (bbRaw || {});
  const bbArray = bbItem.bodyBatteryValuesArray || [];

  const sleepDto    = (sleepRaw || {}).dailySleepDTO || sleepRaw || {};
  const sleepStartDt = sleepDto.sleepStartTimestampGMT ? tsToDt(sleepDto.sleepStartTimestampGMT) : null;
  const sleepEndDt   = sleepDto.sleepEndTimestampGMT   ? tsToDt(sleepDto.sleepEndTimestampGMT)   : null;
  const sleepStartMs = sleepStartDt ? sleepStartDt.getTime() : null;
  const sleepEndMs   = sleepEndDt   ? sleepEndDt.getTime()   : null;

  lines.push('Body Battery:');
  if (bbArray.length) {
    const entries = [];
    for (const e of bbArray) {
      if (Array.isArray(e) && e.length >= 2) {
        entries.push([Number(e[0]), e[1]]);
      } else if (e && typeof e === 'object') {
        const ts  = e.startTimestampGMT || e.timestamp;
        const val = e.bodyBatteryLevel ?? e.value;
        if (ts != null && val != null) entries.push([Number(ts), val]);
      }
    }
    if (entries.length) {
      entries.sort((a, b) => a[0] - b[0]);
      let filtered = entries;
      if (cutoffMs) {
        const f = entries.filter(([ts]) => ts <= cutoffMs);
        if (f.length) filtered = f;
      }
      const allVals = filtered.map(([, v]) => v);

      const atSleepStart  = sleepStartDt ? findBbAt(bbArray, sleepStartDt.getTime(), 30) : null;
      const atWake        = sleepEndDt   ? findBbAt(bbArray, sleepEndDt.getTime(), 30)   : null;
      const current       = allVals[allVals.length - 1];

      let peakDuringSleep = null;
      if (sleepStartMs && sleepEndMs) {
        const sleepVals = entries.filter(([ts]) => ts >= sleepStartMs && ts <= sleepEndMs).map(([, v]) => v);
        if (sleepVals.length) peakDuringSleep = Math.max(...sleepVals);
      }

      if (atSleepStart != null)  lines.push(`  At Sleep Start:    ${atSleepStart}`);
      if (peakDuringSleep != null) lines.push(`  Peak Overnight:    ${peakDuringSleep}`);
      if (atWake != null)        lines.push(`  At Wake-Up:        ${atWake}`);
      lines.push(`  Current:           ${current}`);
    } else {
      if (bbItem.charged) lines.push(`  Charged to:  ${bbItem.charged}`);
      if (bbItem.drained) lines.push(`  Drained:     ${bbItem.drained}`);
    }
  } else {
    lines.push('  No data available');
  }

  lines.push('');
  lines.push('Stress:');
  if (stressRaw) {
    let overall  = stressRaw.overallStressLevel ?? stressRaw.avgStressLevel;
    const restS  = stressRaw.restStressDuration     || 0;
    const lowS   = stressRaw.lowStressDuration      || 0;
    const medS   = stressRaw.mediumStressDuration   || 0;
    const highS  = stressRaw.highStressDuration     || 0;
    const actS   = stressRaw.activityStressDuration || 0;

    function stressLabel(v) {
      if (v == null) return '--';
      if (v < 26)   return `${v}  (Low)`;
      if (v < 51)   return `${v}  (Medium)`;
      if (v < 76)   return `${v}  (High)`;
      return             `${v}  (Very High)`;
    }

    const stressArr = stressRaw.stressValuesArray || [];
    if (cutoffMs && stressArr.length) {
      const cutoffVals = stressArr
        .filter(e => Array.isArray(e) && e.length >= 2 && e[1] != null && e[1] >= 0 && Number(e[0]) <= cutoffMs)
        .map(e => e[1]);
      if (cutoffVals.length)
        overall = Math.round(cutoffVals.reduce((a, b) => a + b, 0) / cutoffVals.length);
    }

    if (overall != null) lines.push(`  Overall (24hr avg):  ${stressLabel(overall)}`);

    if (stressArr.length && sleepStartMs && sleepEndMs) {
      const overnightVals = stressArr
        .filter(e => Array.isArray(e) && e.length >= 2 && e[1] != null && e[1] >= 0
                  && Number(e[0]) >= sleepStartMs && Number(e[0]) <= sleepEndMs)
        .map(e => e[1]);
      if (overnightVals.length) {
        const onAvg = Math.round(overnightVals.reduce((a, b) => a + b, 0) / overnightVals.length);
        lines.push(`  Overnight Average:   ${stressLabel(onAvg)}`);
      }
    }

    const totalTracked = restS + lowS + medS + highS + actS;
    if (totalTracked > 0) {
      lines.push('  Last 24hrs Breakdown:');
      if (restS)  lines.push(`    Rest:         ${hm(restS)}`);
      if (lowS)   lines.push(`    Low Stress:   ${hm(lowS)}`);
      if (medS)   lines.push(`    Med Stress:   ${hm(medS)}`);
      if (highS)  lines.push(`    High Stress:  ${hm(highS)}`);
      if (actS)   lines.push(`    Activity:     ${hm(actS)}`);
    }
  } else {
    lines.push('  No data available');
  }

  lines.push('');
  lines.push('Resting HR:');
  const rhrVal = extractRhrValue(rhrTodayRaw);
  lines.push(rhrVal ? `  Last Night:    ${rhrVal} bpm` : '  Last Night:    No data');

  if (rhrWeekVals && rhrWeekVals.length) {
    const weekAvg = Math.round(rhrWeekVals.reduce((a, b) => a + b, 0) / rhrWeekVals.length);
    lines.push(`  7-Day Average: ${weekAvg} bpm  (${rhrWeekVals.length} days)`);
    if (rhrVal) {
      const diff = rhrVal - weekAvg;
      lines.push(`  vs Avg:        ${diff > 0 ? '+' : ''}${diff} bpm`);
    }
  }

  return lines;
}

function sectionFitness(mmNow, mm4w, tsRaw, lactateRaw, raceRaw, readinessRaw) {
  const lines = ['', 'FITNESS', '-'.repeat(46)];

  function extractVo2(mm) {
    if (!mm) return [null, null];
    const items = Array.isArray(mm) ? mm : [mm];
    for (const item of items) {
      if (!item || typeof item !== 'object') continue;
      const generic = item.generic || {};
      const vo2 = generic.vo2MaxPreciseValue || generic.vo2MaxValue || generic.VO2MaxValue;
      if (vo2) return [parseFloat(vo2), generic.fitnessAgeDescription || generic.fitnessLevel || null];
    }
    return [null, null];
  }

  const [vo2Now, lvlNow] = extractVo2(mmNow);
  const [vo24w]          = extractVo2(mm4w);

  lines.push('VO2max:');
  if (vo2Now) {
    const lvlStr = lvlNow ? `  (${ttl(lvlNow)})` : '';
    lines.push(`  Current:     ${vo2Now.toFixed(1)} mL/kg/min${lvlStr}`);
  } else {
    lines.push('  Current:     No data');
  }
  if (vo24w) {
    const diff    = vo2Now ? Math.round((vo2Now - vo24w) * 10) / 10 : null;
    const diffStr = diff != null ? `  (${diff > 0 ? '+' : ''}${diff} over 4 wks)` : '';
    lines.push(`  4 Weeks Ago: ${vo24w.toFixed(1)} mL/kg/min${diffStr}`);
  } else {
    lines.push('  4 Weeks Ago: No data');
  }

  lines.push('');
  const tsObj  = (Array.isArray(tsRaw) ? tsRaw[0] : tsRaw) || {};
  const tsData = tsObj.mostRecentTrainingStatus || tsObj;
  const tsType = tsData.trainingStatusType || tsData.status;
  lines.push(`Training Status: ${tsType ? ttl(tsType) : 'No data'}`);

  const tsFeedback = tsData.trainingStatusFeedbackPhrase || tsData.feedback;
  if (tsFeedback) lines.push(`  (${ttl(tsFeedback)})`);

  const readinessList = Array.isArray(readinessRaw) ? readinessRaw : (readinessRaw ? [readinessRaw] : []);
  const tr = readinessList.find(r => r && r.primaryActivityTracker) || readinessList[0] || {};

  const acuteLoad = tr.acuteLoad ?? null;
  const acwrPct   = tr.acwrFactorPercent ?? null;

  const tlbMap   = ((tsObj.mostRecentTrainingLoadBalance || {}).metricsTrainingLoadBalanceDTOMap) || {};
  const tlb      = Object.values(tlbMap)[0] || {};
  const aeroLow  = tlb.monthlyLoadAerobicLow  ?? null;
  const aeroHigh = tlb.monthlyLoadAerobicHigh ?? null;
  const anaerobic = tlb.monthlyLoadAnaerobic  ?? null;
  const tlbPhrase = tlb.trainingBalanceFeedbackPhrase;

  if (acuteLoad != null) {
    lines.push('');
    lines.push('Training Load:');
    lines.push(`  Acute (7-day):    ${Math.round(acuteLoad)}`);
    if (acwrPct != null) {
      const ratio = acwrPct / 100;
      let rlabel;
      if (ratio < 0.8)       rlabel = 'Low -- possible detraining';
      else if (ratio <= 1.0) rlabel = 'Maintenance';
      else if (ratio <= 1.3) rlabel = 'Optimal';
      else if (ratio <= 1.5) rlabel = 'High';
      else                   rlabel = 'Very High -- overreaching risk';
      lines.push(`  Acute/Chronic:    ${ratio.toFixed(2)}  (${rlabel})`);
    }
  }

  if ([aeroLow, aeroHigh, anaerobic].some(v => v != null)) {
    const total = (aeroLow || 0) + (aeroHigh || 0) + (anaerobic || 0);
    lines.push('');
    lines.push('Load Focus (last 4 weeks):');
    if (aeroLow  != null) lines.push(`  Aerobic Base:  ${total ? Math.round(aeroLow / total * 100) : '--'}%  (${Math.round(aeroLow)} AU)`);
    if (aeroHigh != null) lines.push(`  Aerobic Peak:  ${total ? Math.round(aeroHigh / total * 100) : '--'}%  (${Math.round(aeroHigh)} AU)`);
    if (anaerobic != null) lines.push(`  Anaerobic:     ${total ? Math.round(anaerobic / total * 100) : '--'}%  (${Math.round(anaerobic)} AU)`);
    if (tlbPhrase) lines.push(`  Balance Note:  ${ttl(tlbPhrase)}`);
  }

  lines.push('');
  lines.push('Lactate Threshold:');
  if (lactateRaw) {
    const ltHr  = lactateRaw.lactateThresholdHeartRate || lactateRaw.heartRate || lactateRaw.hrBpm;
    const ltP   = lactateRaw.pacePer1000mInSeconds || lactateRaw.paceInSeconds;
    const ltSpd = lactateRaw.maxSpeedInMetersPerSecond;
    lines.push(ltHr ? `  Heart Rate:  ${ltHr} bpm` : '  Heart Rate:  No data');
    if (ltP)             lines.push(`  Pace:        ${paceFmt(ltP)}`);
    else if (ltSpd > 0)  lines.push(`  Pace:        ${paceFmt(1000 / ltSpd)}`);
    else                 lines.push('  Pace:        No data');
  } else {
    lines.push('  Not available via API for this device');
  }

  lines.push('');
  lines.push('Race Predictions:');
  if (raceRaw && typeof raceRaw === 'object' && raceRaw.time5K) {
    lines.push(`  ${'5K:'.padEnd(16)} ${raceFmt(raceRaw.time5K)}`);
    if (raceRaw.time10K)          lines.push(`  ${'10K:'.padEnd(16)} ${raceFmt(raceRaw.time10K)}`);
    if (raceRaw.timeHalfMarathon) lines.push(`  ${'Half Marathon:'.padEnd(16)} ${raceFmt(raceRaw.timeHalfMarathon)}`);
    if (raceRaw.timeMarathon)     lines.push(`  ${'Marathon:'.padEnd(16)} ${raceFmt(raceRaw.timeMarathon)}`);
  } else {
    lines.push('  No data available');
  }

  return lines;
}

function sectionRecovery(readinessRaw) {
  const lines = ['', 'RECOVERY', '-'.repeat(46)];

  const readinessList = Array.isArray(readinessRaw) ? readinessRaw : (readinessRaw ? [readinessRaw] : []);
  const postExercise  = readinessList.filter(r => r && r.inputContext === 'AFTER_POST_EXERCISE_RESET');
  const wakeup        = readinessList.filter(r => r && r.inputContext === 'AFTER_WAKEUP_RESET');

  const trScore = wakeup[0]        || readinessList[0] || {};
  const trRec   = postExercise[0]  || readinessList[0] || {};

  const score    = trScore.score ?? null;
  const level    = trScore.level;
  const feedback = trScore.feedbackShort || trScore.feedbackLong;

  if (score != null) {
    const lvlStr = level ? `  (${ttl(level)})` : '';
    lines.push(`Readiness Score:  ${score}/100${lvlStr}`);
  }
  if (feedback) lines.push(`Readiness Status: ${ttl(feedback)}`);

  const recMin = trRec.recoveryTime ?? null;
  const recTs  = trRec.timestamp || trRec.timestampLocal;
  if (recMin != null && recMin > 1) {
    const initialHours = recMin / 60;
    let elapsedHours = 0;
    if (recTs) {
      try {
        const recDt = new Date(String(recTs).replace(' ', 'T').split('.')[0]);
        elapsedHours = (Date.now() - recDt.getTime()) / 3600000;
      } catch {}
    }
    const remaining = Math.max(0, Math.round(initialHours - elapsedHours));
    lines.push(`Recovery Time:    ${remaining}h remaining  (started at ${Math.round(initialHours)}h)`);
  } else {
    lines.push('Recovery Time:    Fully recovered');
  }

  const hrvPct = trScore.hrvFactorPercent ?? null;
  const hrvWk  = trScore.hrvWeeklyAverage ?? null;
  if (hrvPct != null) lines.push(`HRV Factor:       ${hrvPct}%  (vs 7-day baseline)`);
  if (hrvWk  != null) lines.push(`HRV Weekly Avg:   ${hrvWk} ms`);

  return lines;
}

// ── Handler ─────────────────────────────────────────────────────

exports.handler = async (event) => {
  const cors = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json',
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: cors, body: '' };
  }


  let body = {};
  try { body = JSON.parse(event.body || '{}'); } catch {}

  const { email = '', password = '', report_date, cutoff_ms } = body;

  if (!email || !password) {
    return {
      statusCode: 400,
      headers: cors,
      body: JSON.stringify({ error: 'Garmin email and password are required' }),
    };
  }

  // Parse report date
  let reportDate;
  try {
    reportDate = report_date ? new Date(report_date + 'T00:00:00Z') : new Date();
    if (isNaN(reportDate)) reportDate = new Date();
  } catch { reportDate = new Date(); }

  const cutoffMs = cutoff_ms ? Number(cutoff_ms) : null;

  // Date strings (UTC)
  const dateStr = reportDate.toISOString().split('T')[0];

  const yesterday = new Date(reportDate);
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);
  const yesterdayStr = yesterday.toISOString().split('T')[0];

  const fourWeeksAgo = new Date(reportDate);
  fourWeeksAgo.setUTCDate(fourWeeksAgo.getUTCDate() - 28);
  const fourWeeksAgoStr = fourWeeksAgo.toISOString().split('T')[0];

  // Auth
  let GCClient;
  try {
    GCClient = new GarminConnect({ username: email, password });
    await GCClient.login();
  } catch (e) {
    const msg = String(e.message || e);
    const isAuth = msg.toLowerCase().includes('auth') ||
                   msg.toLowerCase().includes('credential') ||
                   msg.toLowerCase().includes('invalid') ||
                   msg.includes('401') || msg.includes('403');
    return {
      statusCode: isAuth ? 401 : 500,
      headers: cors,
      body: JSON.stringify({ error: `Garmin login failed: ${msg}` }),
    };
  }

  async function safeGet(fn) {
    try { return await fn(); } catch { return null; }
  }

  async function withFallback(todayFn, yesterdayFn) {
    const result = await safeGet(todayFn);
    if (result != null) return result;
    return safeGet(yesterdayFn);
  }

  async function rawGet(path) {
    try {
      // Try the GarminConnect client's get method with the full URL
      return await GCClient.get('https://connect.garmin.com' + path);
    } catch {
      return null;
    }
  }

  // Weekly RHR: one RHR number per day for the past 7 days
  async function getRhrDay(dateString) {
    try {
      const raw = await GCClient.getHeartRateData(dateString);
      if (!raw) return null;
      return raw.restingHeartRate || extractRhrValue(raw) || null;
    } catch { return null; }
  }

  const rhrDays = Array.from({ length: 7 }, (_, i) => {
    const d = new Date(reportDate);
    d.setUTCDate(d.getUTCDate() - i);
    return d.toISOString().split('T')[0];
  });

  // Parallel fetch — all 11 main + 7 RHR days
  const [
    sleepR, hrvR, bbR, stressR, rhrTodayR,
    mmNowR, mm4wR, tsR, readinessR, raceR, lactateR,
    ...rhrWeekR
  ] = await Promise.allSettled([
    withFallback(
      () => GCClient.getSleepData(dateStr),
      () => GCClient.getSleepData(yesterdayStr)
    ),
    withFallback(
      () => rawGet(`/proxy/hrv-service/hrv/${dateStr}`),
      () => rawGet(`/proxy/hrv-service/hrv/${yesterdayStr}`)
    ),
    withFallback(
      () => GCClient.getBodyBattery(dateStr, dateStr),
      () => GCClient.getBodyBattery(yesterdayStr, yesterdayStr)
    ),
    withFallback(
      () => GCClient.getStressData(dateStr),
      () => GCClient.getStressData(yesterdayStr)
    ),
    withFallback(
      () => GCClient.getHeartRateData(dateStr),
      () => GCClient.getHeartRateData(yesterdayStr)
    ),
    withFallback(
      () => rawGet(`/proxy/metrics-service/metrics/maxmet/weekly/${dateStr}`),
      () => rawGet(`/proxy/metrics-service/metrics/maxmet/weekly/${yesterdayStr}`)
    ),
    safeGet(() => rawGet(`/proxy/metrics-service/metrics/maxmet/weekly/${fourWeeksAgoStr}`)),
    withFallback(
      () => rawGet(`/proxy/metrics-service/metrics/trainingstatus/aggregated/${dateStr}`),
      () => rawGet(`/proxy/metrics-service/metrics/trainingstatus/aggregated/${yesterdayStr}`)
    ),
    withFallback(
      () => rawGet(`/proxy/metrics-service/metrics/trainingreadiness/${dateStr}`),
      () => rawGet(`/proxy/metrics-service/metrics/trainingreadiness/${yesterdayStr}`)
    ),
    safeGet(() => rawGet('/proxy/metrics-service/metrics/racepredictions')),
    safeGet(() => rawGet('/proxy/biometric-service/lactateThreshold')),
    ...rhrDays.map(d => getRhrDay(d)),
  ]);

  function unwrap(r) { return r && r.status === 'fulfilled' ? r.value : null; }

  const data = {
    sleep:     unwrap(sleepR),
    hrv:       unwrap(hrvR),
    bb:        unwrap(bbR),
    stress:    unwrap(stressR),
    rhr_today: unwrap(rhrTodayR),
    mm_now:    unwrap(mmNowR),
    mm_4w:     unwrap(mm4wR),
    ts:        unwrap(tsR),
    readiness: unwrap(readinessR),
    race:      unwrap(raceR),
    lactate:   unwrap(lactateR),
    rhr_week:  rhrWeekR.map(unwrap).filter(v => v != null),
  };

  // Build report header
  const now      = new Date();
  const cutoffDt = cutoffMs ? new Date(cutoffMs) : now;
  const isNow    = !cutoffMs || Math.abs(now.getTime() - cutoffMs) < 120000;

  const dateLabel = reportDate.toLocaleDateString('en-GB', {
    weekday: 'long', day: '2-digit', month: 'long', year: 'numeric', timeZone: 'UTC',
  });
  const periodStr = isNow
    ? `Generated ${now.toISOString().slice(11, 16)} UTC`
    : `as of ${cutoffDt.toISOString().slice(11, 16)} UTC`;

  const header = [
    '',
    '='.repeat(46),
    '  GARMIN CONNECT - DAILY HEALTH REPORT',
    `  ${dateLabel} | ${periodStr}`,
    '='.repeat(46),
    '',
  ];

  const allSections = [
    ...sectionSleep(data.sleep, data.hrv),
    ...sectionReadiness(data.bb, data.stress, data.rhr_today, data.rhr_week, data.sleep, cutoffMs),
    ...sectionFitness(data.mm_now, data.mm_4w, data.ts, data.lactate, data.race, data.readiness),
    ...sectionRecovery(data.readiness),
  ];

  const reportText = [...header, ...allSections, '', '='.repeat(46), ''].join('\n');

  return {
    statusCode: 200,
    headers: cors,
    body: JSON.stringify({ report_text: reportText }),
  };
};
