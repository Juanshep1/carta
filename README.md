# CARTA — A Personal Codex

Your own scholarly wiki, kept on a Raspberry Pi. Tell CARTA (the embedded
llama) "I just learned about octopus cognition", and she finds the Wikipedia
article, mirrors the text and images locally, files it under the right
*province*, and leaves room in the margins for your own notes.

Runs entirely on your Pi. Wikipedia is the only outbound fetch.

## Architecture

```
   ┌─────────────┐        HTTPS           ┌──────────────────┐
   │   Netlify   │ ─────────────────────▶ │  Your Pi         │
   │  (frontend) │      Bearer token      │  FastAPI + llama │
   └─────────────┘                        └──────────────────┘
        ▲                                        ▲
        │                                        │
    phone / laptop                         Tailscale (backup)
```

- **Frontend** — single HTML file served by Netlify's CDN (fast anywhere).
  On first visit it asks for your Pi's public URL and an API key, saves both
  to `localStorage`, and talks to the Pi for everything.
- **Backend** — FastAPI + SQLite + BeautifulSoup running on the Pi,
  fronted by Tailscale Funnel or Cloudflare Tunnel for public access.
- **iOS app** (future) — connects to the same Pi API.

## Repo layout

```
carta/
├── backend/              # runs on the Pi
│   ├── server.py         # FastAPI app
│   ├── schema.sql        # SQLite schema
│   ├── requirements.txt
│   ├── start.sh
│   ├── carta.db          # created at runtime (gitignored)
│   └── static/images/    # Wikipedia mirrors (gitignored)
├── frontend/             # deployed to Netlify
│   ├── index.html
│   ├── netlify.toml
│   └── _redirects
├── start.sh              # convenience wrapper → backend/start.sh
└── README.md
```

## Backend (Pi)

```bash
cd backend
CARTA_API_KEY=$(openssl rand -base64 32) ./start.sh
```

Write that `CARTA_API_KEY` down — you'll paste it into the Netlify frontend.

Env vars:

| Variable          | Default                         | Purpose                                   |
| ----------------- | ------------------------------- | ----------------------------------------- |
| `CARTA_API_KEY`   | *(empty)*                       | Bearer token. Empty disables auth (dev).  |
| `OLLAMA_URL`      | `http://127.0.0.1:11434`        | Ollama endpoint                           |
| `OLLAMA_MODEL`    | `llama3.2:1b`                   | Model CARTA uses                          |
| `CARTA_HOST`      | `0.0.0.0`                       | Bind address                              |
| `CARTA_PORT`      | `8787`                          | Bind port                                 |

### As a systemd service

`/etc/systemd/system/carta.service`:

```ini
[Unit]
Description=CARTA — personal codex
After=network-online.target ollama.service

[Service]
Type=exec
User=juan123
WorkingDirectory=/home/juan123/carta/backend
Environment=CARTA_API_KEY=<your-key-here>
Environment=OLLAMA_MODEL=llama3.2:1b
ExecStart=/home/juan123/carta/backend/start.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now carta
```

## Public access to the Pi

Pick one — both give HTTPS.

### Option A · Tailscale Funnel (recommended)

Already have Tailscale on the Pi? One command exposes port 8787 on the public
internet with HTTPS, via Tailscale's infrastructure:

```bash
sudo tailscale funnel --bg --https=443 localhost:8787
```

Your URL is `https://<your-host>.<your-tailnet>.ts.net`. That's what you paste
into the frontend. Running `tailscale funnel --bg off` removes it.

### Option B · Cloudflare Tunnel

```bash
sudo apt install cloudflared
cloudflared tunnel --url http://localhost:8787
```

The ephemeral URL on `trycloudflare.com` is free but rotates. For a stable URL,
create a named tunnel bound to your own domain — see Cloudflare's docs.

### Fallback · direct Tailnet IP

Any device on your tailnet can always reach the Pi at its tailnet IP
(`tailscale ip -4`) on port 8787 — no tunnel needed.

## Frontend (Netlify)

1. Push this repo to GitHub.
2. Netlify → **Add new site → Import from Git** → pick the repo.
3. Build settings: **base directory** `frontend`, **publish directory** `frontend`.
   (No build command.)
4. Deploy. Open the Netlify URL.
5. First load shows a login screen — paste your Pi URL and the `CARTA_API_KEY`.
6. Done. The key is stored only in your browser's `localStorage`.

To change settings later, click the tagline under the CARTA wordmark
(top-left).

## Local dev (everything on the Pi)

If you just want the Pi to serve both — open `http://localhost:8787` on the
Pi, it falls back to same-origin relative URLs. Leave the frontend login
fields blank and hit Connect.

## API

```
GET  /                          — frontend (if local) or service JSON
GET  /healthz                   — Ollama status (no auth)
GET  /api/atlas                 — home data
GET  /api/provinces/{slug}      — province + its articles
GET  /api/articles/{slug}       — full article + marginalia + cross-refs
DEL  /api/articles/{slug}       — delete article
POST /api/articles/{slug}/refresh — re-fetch from Wikipedia, preserve marginalia
POST /api/capture               — fetch + mirror a Wikipedia article
POST /api/marginalia            — pin a margin note
POST /api/carta/chat            — conversational CARTA
POST /api/carta/draft           — structured draft (JSON output)
GET  /api/search?q=             — search titles, decks, marginalia
```

All endpoints except `/` and `/healthz` require `Authorization: Bearer <key>`
when `CARTA_API_KEY` is set.

## Troubleshooting

- **"Couldn't reach the Pi"** on first load → check the Funnel/Tunnel URL is
  live (`curl https://your-url/healthz` from another device).
- **"API key is missing or invalid"** → the env var on the Pi and the key
  you pasted into the frontend don't match. Click the tagline under CARTA
  (top-left) to edit.
- **"Ollama offline"** in the top bar → `systemctl status ollama` on the Pi.
- **"Model missing"** → `ollama pull llama3.2:1b`.
- **Mobile is slow to load** → the HTML is 101KB raw, ~22KB gzipped. If it's
  still slow, your Funnel is the bottleneck; try Tailscale's direct tailnet
  IP from your phone's Tailscale app.

## Data

`backend/carta.db` + `backend/static/images/` are your data. Back them up
with `rsync` or `sqlite3 carta.db .dump > backup.sql`.
