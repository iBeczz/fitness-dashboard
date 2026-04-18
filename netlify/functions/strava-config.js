exports.handler = async () => {
  const clientId = process.env.STRAVA_CLIENT_ID;
  if (!clientId) {
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'STRAVA_CLIENT_ID not configured' })
    };
  }

  const siteUrl = process.env.URL || 'http://localhost:8888';
  const redirectUri = `${siteUrl}/.netlify/functions/strava-callback`;
  const scope = 'activity:read_all';
  const authUrl =
    `https://www.strava.com/oauth/authorize` +
    `?client_id=${clientId}` +
    `&redirect_uri=${encodeURIComponent(redirectUri)}` +
    `&response_type=code` +
    `&scope=${scope}` +
    `&approval_prompt=auto`;

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    body: JSON.stringify({ authUrl })
  };
};
