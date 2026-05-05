#!/usr/bin/env python3
"""
Web Scraper RESTful API (async)
FastAPI + uvicorn. Wraps scraper.py functions as async endpoints.
Scrape tasks run in a background thread pool — non-blocking.

Run: python3 api.py
Or:  uvicorn api:app --host 0.0.0.0 --port 5556 --reload
"""

import hashlib
import asyncio
import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

# Import scraper functions
from scraper_v2 import (
    get_conn, init_db,
    scrape, batch_scrape, search,
    get_stats, export_markdown, get_outlinks, get_inlinks,
    get_graph, crawl_site,
    url_hash, classify_url, content_hash,
)

# ── App ──────────────────────────────────────────────

app = FastAPI(
    title="Web Scraper API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Thread pool for blocking scraper calls
executor = ThreadPoolExecutor(max_workers=4)

# ── Task store (in-memory) ──────────────────────────

tasks: dict[str, dict] = {}


def _run_task(task_id: str, fn, *args):
    """Run a scraper function in background, update task status."""
    try:
        result = fn(*args)
        tasks[task_id]["status"] = "done"
        tasks[task_id]["result"] = result
    except Exception as e:
        tasks[task_id]["status"] = "failed"
        tasks[task_id]["error"] = str(e)
    finally:
        tasks[task_id]["finished_at"] = datetime.now(timezone.utc).isoformat()


def _submit_task(name: str, fn, *args) -> str:
    """Create a background task, return task_id."""
    tid = str(uuid.uuid4())[:8]
    tasks[tid] = {
        "id": tid,
        "name": name,
        "status": "running",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "result": None,
        "error": None,
    }
    loop = asyncio.get_running_loop()
    loop.run_in_executor(executor, _run_task, tid, fn, *args)
    return tid


# ── Pydantic models ─────────────────────────────────

class ScrapeRequest(BaseModel):
    url: str
    force: bool = False  # ignore dedup

class BatchScrapeRequest(BaseModel):
    urls: list[str]

class CrawlSiteRequest(BaseModel):
    start_url: str
    max_pages: int = Field(default=50, ge=1, le=500)


# ── Routes: Health ──────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# ── Routes: Scrape (async) ──────────────────────────

@app.post("/api/scrape")
async def api_scrape(req: ScrapeRequest):
    """Submit a single URL for scraping. Returns task_id for polling."""
    tid = _submit_task(f"scrape:{req.url}", _scrape_one, req.url, req.force)
    return {"task_id": tid, "status": "running", "url": req.url}


@app.post("/api/scrape/batch")
async def api_batch_scrape(req: BatchScrapeRequest):
    """Submit multiple URLs for scraping."""
    tid = _submit_task("batch_scrape", _batch_scrape, req.urls)
    return {"task_id": tid, "status": "running", "count": len(req.urls)}


@app.post("/api/crawl")
async def api_crawl_site(req: CrawlSiteRequest):
    """Crawl a site starting from a URL (follows internal links)."""
    tid = _submit_task(f"crawl:{req.start_url}", _crawl_site, req.start_url, req.max_pages)
    return {"task_id": tid, "status": "running", "start_url": req.start_url}


def _scrape_one(url: str, force: bool = False) -> dict:
    conn = get_conn()
    try:
        ok = scrape(url, conn)
        return {"url": url, "scraped": ok}
    finally:
        conn.close()


def _batch_scrape(urls: list) -> dict:
    conn = get_conn()
    try:
        return batch_scrape(urls, conn)
    finally:
        conn.close()


def _crawl_site(start_url: str, max_pages: int) -> dict:
    conn = get_conn()
    try:
        crawl_site(start_url, conn, max_pages=max_pages)
        return {"start_url": start_url, "max_pages": max_pages}
    finally:
        conn.close()


# ── Routes: Tasks ───────────────────────────────────

@app.get("/api/tasks")
def api_list_tasks():
    """List all background tasks."""
    return {"tasks": sorted(tasks.values(), key=lambda t: t["created_at"], reverse=True)}


@app.get("/api/tasks/{task_id}")
def api_get_task(task_id: str):
    """Get task status and result."""
    if task_id not in tasks:
        raise HTTPException(404, "Task not found")
    return tasks[task_id]


# ── Routes: Pages ───────────────────────────────────

@app.get("/api/pages")
def api_list_pages(
    domain: Optional[str] = None,
    page_type: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """List pages with optional filters."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            where = []
            params = []
            if domain:
                where.append("domain = %s")
                params.append(domain)
            if page_type:
                where.append("page_type = %s")
                params.append(page_type)
            where_sql = ("WHERE " + " AND ".join(where)) if where else ""

            cur.execute(f"SELECT COUNT(*) FROM pages {where_sql}", params)
            total = cur.fetchone()["count"]

            cur.execute(f"""
                SELECT url_hash, url, domain, title, description, author, pub_date,
                       word_count, page_type, out_link_count, in_link_count, scraped_at
                FROM pages {where_sql}
                ORDER BY scraped_at DESC
                LIMIT %s OFFSET %s
            """, params + [limit, offset])
            pages = cur.fetchall()

            # Serialize datetimes
            for p in pages:
                if p.get("scraped_at"):
                    p["scraped_at"] = p["scraped_at"].isoformat()
                if p.get("pub_date"):
                    p["pub_date"] = str(p["pub_date"])

            return {"total": total, "limit": limit, "offset": offset, "pages": pages}
    finally:
        conn.close()


@app.get("/api/pages/{key}")
def api_get_page(key: str):
    """Get a single page by URL hash or URL."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM pages WHERE url_hash = %s OR url = %s
            """, (key, key))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Page not found")
            if row.get("scraped_at"):
                row["scraped_at"] = row["scraped_at"].isoformat()
            if row.get("pub_date"):
                row["pub_date"] = str(row["pub_date"])
            return row
    finally:
        conn.close()


@app.get("/api/pages/{key}/markdown", response_class=PlainTextResponse)
def api_get_markdown(key: str):
    """Get raw markdown content of a page."""
    conn = get_conn()
    try:
        md = export_markdown(key, conn)
        return md
    finally:
        conn.close()


@app.get("/api/pages/{key}/links")
def api_get_links(key: str):
    """Get all outbound links from a page."""
    conn = get_conn()
    try:
        h = url_hash(key) if not key.startswith("{") else key
        links = get_outlinks(h, conn)
        return {"source": key, "outlinks": links, "count": len(links)}
    finally:
        conn.close()


@app.get("/api/pages/{key}/backlinks")
def api_get_backlinks(key: str):
    """Get all pages that link to this page."""
    conn = get_conn()
    try:
        h = url_hash(key) if not key.startswith("{") else key
        links = get_inlinks(h, conn)
        return {"target": key, "backlinks": links, "count": len(links)}
    finally:
        conn.close()


# ── Routes: Search ──────────────────────────────────

@app.get("/api/search")
def api_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=10, ge=1, le=100),
):
    """Full-text search pages."""
    conn = get_conn()
    try:
        results = search(q, conn, limit=limit)
        for r in results:
            if r.get("scraped_at"):
                r["scraped_at"] = r["scraped_at"].isoformat()
        return {"query": q, "count": len(results), "results": results}
    finally:
        conn.close()


# ── Routes: Stats & Graph ──────────────────────────

@app.get("/api/stats")
def api_stats():
    """Database statistics."""
    conn = get_conn()
    try:
        stats = get_stats(conn)
        # Domain breakdown
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT domain, COUNT(*) as pages, SUM(word_count) as words
                FROM pages GROUP BY domain ORDER BY pages DESC
            """)
            domains = cur.fetchall()
            for d in domains:
                d["words"] = d["words"] or 0
        stats["domains"] = domains
        return stats
    finally:
        conn.close()


@app.get("/api/graph")
def api_graph(limit: int = Query(default=20, ge=1, le=200)):
    """Get link graph — pages ranked by connections."""
    conn = get_conn()
    try:
        graph = get_graph(conn, limit=limit)
        for g in graph:
            if g.get("scraped_at"):
                g["scraped_at"] = g["scraped_at"].isoformat()
        return {"count": len(graph), "nodes": graph}
    finally:
        conn.close()


@app.get("/api/domains")
def api_domains():
    """List all domains with page counts."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT domain, COUNT(*) as pages, SUM(word_count) as words,
                       MIN(scraped_at) as first_scraped, MAX(scraped_at) as last_scraped
                FROM pages GROUP BY domain ORDER BY pages DESC
            """)
            rows = cur.fetchall()
            for r in rows:
                r["words"] = r["words"] or 0
                if r.get("first_scraped"):
                    r["first_scraped"] = r["first_scraped"].isoformat()
                if r.get("last_scraped"):
                    r["last_scraped"] = r["last_scraped"].isoformat()
            return {"domains": rows}
    finally:
        conn.close()


@app.get("/api/domains/{domain}")
def api_domain_pages(
    domain: str,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """Get all pages from a specific domain."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) FROM pages WHERE domain = %s", (domain,))
            total = cur.fetchone()["count"]

            cur.execute("""
                SELECT url_hash, url, title, description, word_count,
                       out_link_count, in_link_count, scraped_at
                FROM pages WHERE domain = %s
                ORDER BY scraped_at DESC
                LIMIT %s OFFSET %s
            """, (domain, limit, offset))
            pages = cur.fetchall()
            for p in pages:
                if p.get("scraped_at"):
                    p["scraped_at"] = p["scraped_at"].isoformat()
            return {"domain": domain, "total": total, "pages": pages}
    finally:
        conn.close()


# ── Routes: Delete ──────────────────────────────────

@app.delete("/api/pages/{key}")
def api_delete_page(key: str):
    """Delete a page and its links by URL hash or URL."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            h = url_hash(key) if not key.startswith("{") else key
            cur.execute("DELETE FROM links WHERE source_hash = %s OR target_hash = %s", (h, h))
            links_deleted = cur.rowcount
            cur.execute("DELETE FROM pages WHERE url_hash = %s OR url = %s", (key, key))
            pages_deleted = cur.rowcount
            conn.commit()
            if pages_deleted == 0:
                raise HTTPException(404, "Page not found")
            return {"deleted": True, "pages": pages_deleted, "links": links_deleted}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


# ── Init DB on startup ──────────────────────────────

@app.on_event("startup")
def startup():
    init_db()


# ── Main ────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("API_PORT", "5556"))
    uvicorn.run(app, host="0.0.0.0", port=port)
