# Web2MD 🕷️

Adaptive web scraper powered by [Scrapling](https://github.com/D4Vinci/Scrapling). Extracts readable content as markdown, stores in PostgreSQL, with a RESTful API and web dashboard.

## Features

- **Scrapling-powered fetching** — Fast HTTP with TLS fingerprint impersonation, plus StealthyFetcher for Cloudflare bypass
- **Smart content extraction** — 5-layer title detection, author/date extraction, markdown conversion
- **Smart deduplication** — Listing pages re-crawl on content change; detail pages skip permanently
- **Link graph tracking** — Stores source→target relationships for site structure analysis
- **PostgreSQL storage** — Full-text search, efficient queries
- **Async REST API** — FastAPI with background task execution
- **Web dashboard** — Interactive link graph, search, page viewer

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
# Start the API server
python api.py

# Or with uvicorn
uvicorn api:app --host 0.0.0.0 --port 5556
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/scrape` | Async scrape a URL → returns `task_id` |
| `POST` | `/api/scrape/batch` | Async batch scrape |
| `POST` | `/api/crawl` | Crawl entire site (follow internal links) |
| `GET` | `/api/tasks` | List all background tasks |
| `GET` | `/api/tasks/{id}` | Get task status/result |
| `GET` | `/api/pages` | List pages (with filters) |
| `GET` | `/api/pages/{key}` | Get page detail |
| `GET` | `/api/pages/{key}/markdown` | Get raw markdown |
| `GET` | `/api/pages/{key}/links` | Get outbound links |
| `GET` | `/api/pages/{key}/backlinks` | Get inbound links |
| `DELETE` | `/api/pages/{key}` | Delete a page |
| `GET` | `/api/search?q=` | Full-text search |
| `GET` | `/api/stats` | Database statistics |
| `GET` | `/api/graph` | Link graph |
| `GET` | `/api/domains` | Domain list |
| `GET` | `/api/domains/{domain}` | Pages by domain |
| `GET` | `/docs` | Swagger UI |

## Example: Async Scrape

```bash
# Submit scrape task (returns immediately)
curl -X POST http://localhost:5556/api/scrape \
  -H "Content-Type: application/json" \
  -d '{"url": "https://news.ycombinator.com"}'

# Response: {"task_id": "a1b2c3d4", "status": "running"}

# Poll for result
curl http://localhost:5556/api/tasks/a1b2c3d4
# Response: {"status": "done", "result": {"scraped": true}}
```

## Configuration

Environment variables:

```bash
SCRAPER_DB_HOST=localhost    # PostgreSQL host
SCRAPER_DB_PORT=5432         # PostgreSQL port
SCRAPER_DB_NAME=web_scraper  # Database name
SCRAPER_DB_USER=scraper      # Database user
SCRAPER_DB_PASS=scraper2026  # Database password
API_PORT=5556                # API server port
```

## Project Structure

```
web2md/
├── scraper.py         # Core scraper (Scrapling-powered)
├── api.py             # RESTful API (FastAPI)
├── templates/         # Web dashboard templates
├── requirements.txt   # Python dependencies
└── README.md
```

## License

MIT
