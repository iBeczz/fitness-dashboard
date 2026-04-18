exports.handler = async (event) => {
  const { code, error, error_description } = event.queryStringParameters || {};
  const proto = event.headers['x-forwarded-proto'] || 'https';
  const host = event.headers['x-forwarded-host'] || event.headers['host'];
  const siteUrl = `${proto}://${host}`;

  if (error) {
    return {
      statusCode: 302,
      headers: { Location: `${siteUrl}/?strava_error=${encodeURIComponent(error_description || error)}` }
    };
  }

  if (!code) {
    return {
      statusCode: 302,
      headers: { Location: `${siteUrl}/?strava_error=${encodeURIComponent('No authorization code received')}` }
    };
  }

  try {
    const tokenRes = await fetch('https://www.strava.com/oauth/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        client_id: process.env.STRAVA_CLIENT_ID,
        client_secret: process.env.STRAVA_CLIENT_SECRET,
        code,
        grant_type: 'authorization_code'
      })
    });

    const data = await tokenRes.json();

    if (data.errors || !data.access_token) {
      const msg = (data.message || 'Token exchange failed');
      return {
        statusCode: 302,
        headers: { Location: `${siteUrl}/?strava_error=${encodeURIComponent(msg)}` }
      };
    }

    const params = new URLSearchParams({
      strava_access_token: data.access_token,
      strava_refresh_token: data.refresh_token,
      strava_expires_at: String(data.expires_at),
      strava_athlete: `${data.athlete?.firstname ?? ''} ${data.athlete?.lastname ?? ''}`.trim()
    });

    return {
      statusCode: 302,
      headers: { Location: `${siteUrl}/?${params.toString()}` }
    };
  } catch (err) {
    return {
      statusCode: 302,
      headers: { Location: `${siteUrl}/?strava_error=${encodeURIComponent(String(err.message))}` }
    };
  }
};
