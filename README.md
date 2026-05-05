# Web2MD 🕷️

Adaptive web scraper powered by [Scrapling](https://github.com/D4Vinci/Scrapling). Extracts readable content as markdown, stores in PostgreSQL, with a RESTful API and web dashboard.

## Features

- **Scrapling-powered fetching** — Fast HTTP with TLS fingerprint impersonation, plus StealthyFetcher for Cloudflare bypass
- **Smart content extraction** — 5-layer title detection, author/date extraction, markdown conversion
- **Smart deduplication** — Listing pages re-crawl on content change; detail pages skip permanently
- **Link graph tracking** — Stores source→target relationships for site structure analysis
- **PostgreSQL storage** — Full-text search, efficient queries
- **Async REST API** — FastAPI with background task execution
- **API Key authentication** — Secure access with `X-API-Key` header
- **Webhook notifications** — Auto-push new content to Telegram
- **Web dashboard** — Interactive link graph, search, page viewer
- **Docker Compose** — One-command deployment

## Quick Start

### Install

```bash
pip install -r requirements.txt
python -m patchright install chromium
```

### CLI Usage

```bash
# Scrape a single URL
python scraper.py https://news.ycombinator.com

# Scrape from file
python scraper.py -f urls.txt

# Search
python scraper.py --search "bitcoin"

# Stats
python scraper.py --stats
```

### API Usage

```bash
# Start the API server (auto-generates API key if not set)
python api.py

# Or with uvicorn
API_KEYS=my-secret-key uvicorn api:app --host 0.0.0.0 --port 5556
```

### Docker

```bash
# Copy env file and configure
cp .env.example .env
vim .env

# All-in-one (single machine)
docker compose up -d

# Or separate deployment (see below)
docker compose -f docker-compose.producer.yml up -d
docker compose -f docker-compose.consumer.yml up -d
```

### Separated Deployment (Producer + Consumer)

Run the scraper/API on one machine, the dashboard on another:

```bash
# ── Machine A (Producer): Scraper + API + DB ──
cd /path/to/web2md
docker compose -f docker-compose.producer.yml up -d
# API: http://<machine-a-ip>:5556
# DB:  localhost:5433 (internal only)

# ── Machine B (Consumer): Dashboard ──
cd /path/to/web2md
# Point to Machine A's DB
SCRAPER_DB_HOST=<machine-a-ip> \
SCRAPER_DB_PORT=5433 \
docker compose -f docker-compose.consumer.yml up -d
# Dashboard: http://<machine-b-ip>:5555
```

**DB access security:** The producer binds PostgreSQL to `127.0.0.1:5433` by default. To allow remote access:

```bash
# Option 1: SSH tunnel (recommended)
ssh -L 5433:localhost:5433 user@<machine-a-ip>

# Option 2: Open port in firewall (less secure)
# Edit docker-compose.producer.yml → change "127.0.0.1:5433:5432" to "0.0.0.0:5433:5432"
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/health` | Health check (no auth) |
| **API Keys** |||
| `GET` | `/api/keys` | List keys |
| `POST` | `/api/keys` | Generate new key |
| `DELETE` | `/api/keys/{key}` | Delete key |
| **Webhook** |||
| `GET` | `/api/webhook` | Get webhook config |
| `POST` | `/api/webhook/test` | Send test notification |
| **Scrape** |||
| `POST` | `/api/scrape` | Async scrape URL → `task_id` |
| `POST` | `/api/scrape/batch` | Async batch scrape |
| `POST` | `/api/crawl` | Crawl entire site (follow links) |
| `GET` | `/api/tasks` | List background tasks |
| `GET` | `/api/tasks/{id}` | Task status/result |
| **Pages** |||
| `GET` | `/api/pages` | List pages (filters: `domain`, `page_type`) |
| `GET` | `/api/pages/{key}` | Page detail |
| `GET` | `/api/pages/{key}/markdown` | Raw markdown |
| `GET` | `/api/pages/{key}/links` | Outbound links |
| `GET` | `/api/pages/{key}/backlinks` | Inbound links |
| `DELETE` | `/api/pages/{key}` | Delete page |
| **Search & Stats** |||
| `GET` | `/api/search?q=` | Full-text search |
| `GET` | `/api/stats` | Database statistics |
| `GET` | `/api/graph` | Link graph |
| `GET` | `/api/domains` | Domain list |
| `GET` | `/api/domains/{domain}` | Pages by domain |
| `GET` | `/docs` | Swagger UI |

## Authentication

All API endpoints (except `/api/health` and `/docs`) require an `X-API-Key` header.

```bash
# Without key → 401
curl http://localhost:5556/api/stats
# {"detail": "Invalid or missing API key. Pass X-API-Key header."}

# With key → 200
curl -H "X-API-Key: my-secret-key" http://localhost:5556/api/stats

# Auto-generate a key on startup (when API_KEYS env is empty)
# The key is printed in the server logs on first boot.
```

## Webhook (Telegram)

Push newly scraped pages to a Telegram chat automatically.

### Setup

1. Create a Telegram bot via [@BotFather](https://t.me/BotFather), copy the token
2. Get your chat ID (send a message to your bot, then visit `https://api.telegram.org/bot<TOKEN>/getUpdates`)
3. Configure via environment variables or `.env`:

```bash
WEBHOOK_ENABLED=true
WEBHOOK_URL=123456789:ABCdefGHIjklMNOpqrsTUVwxyz   # Bot token
WEBHOOK_CHAT_ID=-1001234567890                         # Chat/group ID
WEBHOOK_MIN_WORDS=50        # Skip pages under 50 words
WEBHOOK_DOMAINS=bbc.com,cnn.com  # Only notify for these domains (empty = all)
```

### How it works

- After a scrape completes, the API checks if the page meets the webhook criteria
- If enabled, it sends a formatted notification with title, domain, word count, and link
- Test with: `curl -X POST -H "X-API-Key: your-key" http://localhost:5556/api/webhook/test`

### Example notification

```
🕷️ New Page Scraped

📄 How AI Is Changing Healthcare
🌐 bbc.com
📝 1,234 words · 🔗 15 links

View: https://bbc.com/article/xyz
```

## Example: Async Scrape

```bash
# Submit scrape task (returns immediately)
curl -X POST http://localhost:5556/api/scrape \
  -H "Content-Type: application/json" \
  -H "X-API-Key: my-secret-key" \
  -d '{"url": "https://news.ycombinator.com"}'

# Response: {"task_id": "a1b2c3d4", "status": "running"}

# Poll for result
curl -H "X-API-Key: my-secret-key" http://localhost:5556/api/tasks/a1b2c3d4
# Response: {"status": "done", "result": {"scraped": true}}
```

## Configuration

Environment variables:

```bash
# Database
SCRAPER_DB_HOST=localhost      # Remote DB IP for consumer mode
SCRAPER_DB_PORT=5432           # 5433 when connecting via Docker port mapping
SCRAPER_DB_NAME=web_scraper
SCRAPER_DB_USER=scraper
SCRAPER_DB_PASS=scraper2026

# API (Producer)
API_PORT=5556
API_KEYS=your-key-1,your-key-2    # Comma-separated, empty = auto-generate

# Dashboard (Consumer)
DASHBOARD_PORT=5555

# Webhook (Producer)
WEBHOOK_ENABLED=false
WEBHOOK_URL=                        # Telegram bot token
WEBHOOK_CHAT_ID=                    # Telegram chat ID
WEBHOOK_MIN_WORDS=50
WEBHOOK_DOMAINS=                    # Comma-separated filter
```

## Project Structure

```
web2md/
├── scraper.py         # Core scraper (Scrapling-powered)
├── scraper_v2.py      # Alias for scraper.py
├── api.py             # RESTful API (FastAPI + auth + webhooks)
├── app.py             # Web dashboard (Flask)
├── templates/         # Dashboard templates
├── requirements.txt   # Python dependencies
├── Dockerfile         # Docker image (both producer & consumer)
├── docker-compose.yml           # All-in-one (single machine)
├── docker-compose.producer.yml  # Producer only (API + DB)
├── docker-compose.consumer.yml  # Consumer only (Dashboard)
├── .env.example       # Environment variable template
└── README.md
```

## License

MIT
