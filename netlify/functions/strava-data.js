const API = 'https://www.strava.com/api/v3';

exports.handler = async (event) => {
  const cors = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: cors, body: '' };
  }

  let body;
  try { body = JSON.parse(event.body || '{}'); } catch { body = {}; }

  let { access_token, refresh_token, expires_at } = body;

  if (!access_token) {
    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Missing Strava token' }) };
  }

  // Refresh if within 60 seconds of expiry
  if (Number(expires_at) < Date.now() / 1000 + 60) {
    try {
      const r = await fetch('https://www.strava.com/oauth/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          client_id: process.env.STRAVA_CLIENT_ID,
          client_secret: process.env.STRAVA_CLIENT_SECRET,
          refresh_token,
          grant_type: 'refresh_token'
        })
      });
      const rd = await r.json();
      if (rd.access_token) {
        access_token = rd.access_token;
        refresh_token = rd.refresh_token;
        expires_at = rd.expires_at;
      }
    } catch (e) {
      return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Token refresh failed: ' + e.message }) };
    }
  }

  const authHeader = { Authorization: `Bearer ${access_token}` };

  // Fetch recent activities to find the latest run
  let activities;
  try {
    const r = await fetch(`${API}/athlete/activities?per_page=20&page=1`, { headers: authHeader });
    activities = await r.json();
  } catch (e) {
    return { statusCode: 502, headers: cors, body: JSON.stringify({ error: 'Failed to fetch activities: ' + e.message }) };
  }

  if (!Array.isArray(activities)) {
    return { statusCode: 502, headers: cors, body: JSON.stringify({ error: activities?.message || 'Unexpected activities response' }) };
  }

  const run = activities.find(a => a.type === 'Run' || a.sport_type === 'Run');
  if (!run) {
    return { statusCode: 404, headers: cors, body: JSON.stringify({ error: 'No recent run found in last 20 activities' }) };
  }

  // Fetch full activity detail
  let activity;
  try {
    const r = await fetch(`${API}/activities/${run.id}`, { headers: authHeader });
    activity = await r.json();
  } catch (e) {
    return { statusCode: 502, headers: cors, body: JSON.stringify({ error: 'Failed to fetch activity detail: ' + e.message }) };
  }

  // Fetch streams at full resolution
  const keys = 'time,distance,altitude,heartrate,cadence,watts,grade_smooth,velocity_smooth';
  let streams;
  try {
    const r = await fetch(
      `${API}/activities/${run.id}/streams?keys=${keys}&key_by_type=true&resolution=high`,
      { headers: authHeader }
    );
    streams = await r.json();
  } catch (e) {
    return { statusCode: 502, headers: cors, body: JSON.stringify({ error: 'Failed to fetch streams: ' + e.message }) };
  }

  const intervals = resampleAt100m(streams);

  return {
    statusCode: 200,
    headers: cors,
    body: JSON.stringify({
      activity,
      intervals,
      new_token: { access_token, refresh_token, expires_at }
    })
  };
};

function resampleAt100m(streams) {
  if (!streams?.distance?.data?.length) return [];

  const dist = streams.distance.data;
  const get = (key, i) => streams[key]?.data?.[i] ?? null;

  const intervals = [];
  let target = 100;

  for (let i = 0; i < dist.length; i++) {
    if (dist[i] < target) continue;

    // Check if all columns present to decide header once
    const velRaw = get('velocity_smooth', i);
    const cadRaw = get('cadence', i);

    let pace = null;
    let paceStr = null;
    if (velRaw != null && velRaw > 0) {
      const secPerKm = 1000 / velRaw;
      const m = Math.floor(secPerKm / 60);
      const s = Math.round(secPerKm % 60);
      paceStr = `${m}:${String(s).padStart(2, '0')} /km`;
      pace = secPerKm;
    }

    let stride = null;
    if (velRaw != null && velRaw > 0 && cadRaw != null && cadRaw > 0) {
      // Strava cadence = steps/min (one leg). Stride = distance per step.
      stride = +(velRaw / (cadRaw / 60)).toFixed(2);
    }

    intervals.push({
      distance: target,
      time: get('time', i),
      paceStr,
      pace,
      hr: get('heartrate', i),
      altitude: get('altitude', i) != null ? +get('altitude', i).toFixed(1) : null,
      grade: get('grade_smooth', i) != null ? +get('grade_smooth', i).toFixed(1) : null,
      cadence: cadRaw,
      stride,
      power: get('watts', i)
    });

    target += 100;
    if (target > dist[dist.length - 1] + 100) break;
  }

  return intervals;
}
