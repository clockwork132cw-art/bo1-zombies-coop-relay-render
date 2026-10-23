# BO1 Zombies co-op relay (Node.js, Render Singapore)

A plain Node.js version of the BO1 Zombies co-op relay. It speaks the same protocol (v3) as the Cloudflare Worker
relay, so the extension can't tell them apart: put this server's `https://` address where the `workers.dev` one was.

## Deploy on Render

1. Put these files at the top level of a GitHub repository (no `node_modules` folder).
2. In Render: **New → Blueprint**, pick the repository, then **Apply**.
   It creates a free web service in Singapore from `render.yaml`.
3. When it's live, open `https://<service>.onrender.com/health`. You should see `{"ok":true,"protocol":3}`.
4. Everyone in the game: set the co-op relay to `https://<service>.onrender.com` and reload vel.gg.

## Things to know

- **It sleeps.** On the free plan, Render stops the service after about 15 idle minutes. Open `/health` a minute before
  playing to wake it up. Waking up (or a redeploy) drops any game in progress.
- **No daily cap.** `/usage` always reports 0%.
- **Allowed pages** are set by the `ALLOWED_ORIGINS` environment variable (default `https://vel.gg`, comma-separated).

## Check it

```
npm install
npm test                                              # starts a local relay and plays host + guests against it
RELAY_URL=https://<service>.onrender.com npm test     # same checks against the deployed relay, with round-trip times
```

On Windows PowerShell, the second one is: `$env:RELAY_URL="https://<service>.onrender.com"; npm test`
