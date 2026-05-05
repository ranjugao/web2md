#!/usr/bin/env python3
"""
Web Scraper v2 — Powered by Scrapling
Replaces the old requests + readability + bs4 approach.
Uses Scrapling's Fetcher (HTTP) + StealthyFetcher (anti-bot) for best results.
"""

import hashlib
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin

import psycopg2
import psycopg2.extras
from markdownify import markdownify as md

# Scrapling imports
from scrapling.fetchers import Fetcher, StealthyFetcher

# ── DB Config ───────────────────────────────────────

DB_CONFIG = {
    "host": os.getenv("SCRAPER_DB_HOST", "localhost"),
    "port": os.getenv("SCRAPER_DB_PORT", "5432"),
    "dbname": os.getenv("SCRAPER_DB_NAME", "web_scraper"),
    "user": os.getenv("SCRAPER_DB_USER", "scraper"),
    "password": os.getenv("SCRAPER_DB_PASS", "scraper2026"),
}

TIMEOUT = 15
DELAY = 1
RESPECT_ROBOTS = False


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def init_db():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                url_hash      VARCHAR(16) PRIMARY KEY,
                url           TEXT NOT NULL,
                domain        TEXT,
                title         TEXT,
                author        TEXT DEFAULT '',
                pub_date      TEXT DEFAULT '',
                description   TEXT DEFAULT '',
                markdown      TEXT,
                word_count    INTEGER DEFAULT 0,
                page_type     TEXT DEFAULT 'detail',
                content_hash  VARCHAR(16),
                scraped_at    TIMESTAMP WITH TIME ZONE,
                status_code   INTEGER DEFAULT 0,
                content_type  TEXT DEFAULT '',
                out_link_count INTEGER DEFAULT 0,
                in_link_count  INTEGER DEFAULT 0
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pages_scraped ON pages(scraped_at)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS links (
                source_hash  VARCHAR(16) NOT NULL,
                target_hash  VARCHAR(16) NOT NULL,
                target_url   TEXT,
                anchor_text  TEXT DEFAULT '',
                scraped_at   TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (source_hash, target_hash)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_links_target ON links(target_hash)")
        try:
            cur.execute("""
                CREATE VIEW page_rank AS
                SELECT
                    p.url, p.title, p.word_count, p.out_link_count, p.in_link_count,
                    p.scraped_at, p.url_hash,
                    (COALESCE(p.out_link_count, 0) + COALESCE(p.in_link_count, 0)) AS connections
                FROM pages p
                ORDER BY connections DESC
            """)
        except psycopg2.errors.DuplicateTable:
            pass
    conn.commit()
    conn.close()


# ── Helpers ─────────────────────────────────────────

def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def url_hash(url: str) -> str:
    return hashlib.sha256(normalize_url(url).encode()).hexdigest()[:16]


def classify_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.lower()
    if re.search(r"/\d{6,}", path) or re.search(r"/p/\w+", path):
        return "detail"
    if re.search(r"/(tag|category|page|search|archive|author)/", path):
        return "listing"
    if path.count("/") <= 2 and not re.search(r"\.\w{2,4}$", path):
        return "listing"
    return "detail"


def content_hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:16]


# ── Fetch with Scrapling ────────────────────────────

def fetch_page(url: str) -> dict:
    """Fetch a page using Scrapling. StealthyFetcher for CF sites, Fetcher for the rest."""
    try:
        # Try fast HTTP first
        page = Fetcher.get(url)
        html = page.html_content if hasattr(page, 'html_content') else str(page)
        return {
            "html": html,
            "status_code": page.status if hasattr(page, 'status') else 200,
            "content_type": "text/html",
        }
    except Exception:
        # Fallback to StealthyFetcher (anti-bot)
        try:
            page = StealthyFetcher.fetch(url, headless=True, network_idle=True, timeout=TIMEOUT)
            html = page.html_content if hasattr(page, 'html_content') else str(page)
            return {
                "html": html,
                "status_code": page.status if hasattr(page, 'status') else 200,
                "content_type": "text/html",
            }
        except Exception as e:
            raise Exception(f"Both fetchers failed for {url}: {e}")


# ── Extract content ─────────────────────────────────

def extract_content(html: str, base_url: str) -> dict:
    """Extract content from HTML using Scrapling's parser."""
    from scrapling.parser import Selector

    selector = Selector(html)

    # Title extraction (5 layers)
    title = ""
    for sel in ['meta[property="og:title"]::attr(content)',
                'meta[name="twitter:title"]::attr(content)',
                'h1::text', 'title::text']:
        t = selector.css(sel).get()
        if t and len(t.strip()) > 3:
            title = t.strip()
            break

    # Author
    author = ""
    for sel in ['meta[name="author"]::attr(content)',
                'meta[property="article:author"]::attr(content)',
                '.author::text', '.byline::text']:
        a = selector.css(sel).get()
        if a:
            author = a.strip()
            break

    # Date
    pub_date = ""
    for sel in ['meta[property="article:published_time"]::attr(content)',
                'meta[name="pubdate"]::attr(content)',
                'time[datetime]::attr(datetime)']:
        d = selector.css(sel).get()
        if d:
            pub_date = d.strip()
            break

    # Description
    description = ""
    for sel in ['meta[name="description"]::attr(content)',
                'meta[property="og:description"]::attr(content)']:
        desc = selector.css(sel).get()
        if desc:
            description = desc.strip()
            break

    # Main content — use readability-like approach
    # Try common content containers
    content_html = ""
    for container in ['article', '.post-content', '.article-body',
                      '.entry-content', '.content', 'main', '#content']:
        el = selector.css(container)
        if el:
            content_html = el[0].html_content if hasattr(el[0], 'html_content') else str(el[0])
            break

    if not content_html:
        content_html = html

    # Convert to markdown
    markdown_text = md(content_html, strip=["script", "style", "nav", "footer", "header"])

    # Clean up markdown
    markdown_text = re.sub(r'\n{3,}', '\n\n', markdown_text)
    markdown_text = re.sub(r'[ \t]+', ' ', markdown_text)
    markdown_text = markdown_text.strip()

    # Word count
    word_count = len(markdown_text.split())

    # Extract links
    links = []
    for a in selector.css('a[href]'):
        href = a.attrib.get('href', '')
        if href and not href.startswith(('#', 'javascript:', 'mailto:')):
            full_url = urljoin(base_url, href)
            if full_url.startswith(('http://', 'https://')):
                anchor = a.css('::text').get() or ''
                links.append({"url": full_url, "anchor": anchor.strip()[:200]})

    return {
        "title": title,
        "author": author,
        "pub_date": pub_date,
        "description": description[:500] if description else "",
        "markdown": markdown_text,
        "word_count": word_count,
        "links": links,
    }


# ── Scrape ──────────────────────────────────────────

def scrape(url: str, conn) -> bool:
    h = url_hash(url)
    page_type = classify_url(url)

    # Smart dedup
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT url_hash, content_hash FROM pages WHERE url_hash = %s", (h,))
        existing = cur.fetchone()

        if existing:
            if page_type == "detail":
                print(f"  ⏭ Detail page already scraped: {url}")
                return False

    try:
        print(f"  🌐 Fetching: {url}")
        page = fetch_page(url)
        content = extract_content(page["html"], url)
        parsed = urlparse(url)
        c_hash = content_hash(content["markdown"])

        # For listing pages: skip if content unchanged
        if existing and page_type == "listing":
            if existing["content_hash"] == c_hash:
                print(f"  ⏭ Listing unchanged: {url}")
                return False
            else:
                print(f"  🔄 Listing updated, re-crawling: {url}")

        with conn.cursor() as cur:
            if existing and page_type == "listing":
                cur.execute("""
                    UPDATE pages SET
                        title = %s, author = %s, pub_date = %s,
                        description = %s, markdown = %s, word_count = %s,
                        page_type = %s, content_hash = %s, scraped_at = %s,
                        status_code = %s, content_type = %s
                    WHERE url_hash = %s
                """, (
                    content["title"], content.get("author", ""), content.get("pub_date", ""),
                    content["description"], content["markdown"], content["word_count"],
                    page_type, c_hash, datetime.now(timezone.utc),
                    page["status_code"], page["content_type"], h,
                ))
                cur.execute("DELETE FROM links WHERE source_hash = %s", (h,))
            else:
                cur.execute("""
                    INSERT INTO pages (url_hash, url, domain, title, author, pub_date,
                                       description, markdown, word_count, page_type,
                                       content_hash, scraped_at, status_code, content_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    h, url, parsed.netloc, content["title"],
                    content.get("author", ""), content.get("pub_date", ""),
                    content["description"], content["markdown"], content["word_count"],
                    page_type, c_hash, datetime.now(timezone.utc),
                    page["status_code"], page["content_type"],
                ))

            # Store outbound links
            for link in content["links"]:
                target_h = url_hash(link["url"])
                try:
                    cur.execute("""
                        INSERT INTO links (source_hash, target_hash, target_url, anchor_text, scraped_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (source_hash, target_hash) DO NOTHING
                    """, (h, target_h, link["url"], link["anchor"], datetime.now(timezone.utc)))
                except Exception:
                    pass

            cur.execute("""
                UPDATE pages SET out_link_count = (
                    SELECT COUNT(*) FROM links WHERE source_hash = %s
                ) WHERE url_hash = %s
            """, (h, h))
        conn.commit()
        print(f"  ✅ Saved: {content['title'][:50]} ({content['word_count']} words, {len(content['links'])} links)")
        return True

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Failed: {e}")
        return False


def batch_scrape(urls: list, conn) -> dict:
    results = {"success": 0, "skipped": 0, "failed": 0}
    total = len(urls)
    for i, url in enumerate(urls, 1):
        print(f"\n[{i}/{total}]")
        url = url.strip()
        if not url or not url.startswith(("http://", "https://")):
            continue
        ok = scrape(url, conn)
        if ok:
            results["success"] += 1
        else:
            results["skipped"] += 1
        if i < total:
            time.sleep(DELAY)
    return results


def search(query: str, conn, limit: int = 10) -> list:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT url, title, description, word_count, scraped_at
            FROM pages
            WHERE to_tsvector('english', coalesce(title, '')) @@ plainto_tsquery('english', %s)
               OR to_tsvector('english', coalesce(markdown, '')) @@ plainto_tsquery('english', %s)
            ORDER BY scraped_at DESC LIMIT %s
        """, (query, query, limit))
        return cur.fetchall()


def get_stats(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) as total, COALESCE(SUM(word_count), 0) as total_words,
                   COUNT(DISTINCT domain) as domains FROM pages
        """)
        row = cur.fetchone()
        return {"total_pages": row[0], "total_words": row[1], "domains": row[2]}


def export_markdown(key: str, conn) -> str:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT url, title, markdown FROM pages WHERE url_hash = %s OR url = %s", (key, key))
        row = cur.fetchone()
        if row:
            return f"# {row['title']}\n\nSource: {row['url']}\n\n{row['markdown']}"
        return "Page not found."


def get_outlinks(key: str, conn) -> list:
    h = url_hash(key) if not key.startswith("{") else key
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT l.target_url, l.anchor_text, p.title AS target_title, p.url_hash AS target_hash
            FROM links l LEFT JOIN pages p ON l.target_hash = p.url_hash
            WHERE l.source_hash = %s ORDER BY l.scraped_at DESC
        """, (h,))
        return cur.fetchall()


def get_inlinks(key: str, conn) -> list:
    h = url_hash(key) if not key.startswith("{") else key
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT l.source_hash, l.anchor_text, p.title AS source_title, p.url AS source_url
            FROM links l LEFT JOIN pages p ON l.source_hash = p.url_hash
            WHERE l.target_hash = %s ORDER BY l.scraped_at DESC
        """, (h,))
        return cur.fetchall()


def get_graph(conn, limit: int = 20) -> list:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM page_rank LIMIT %s", (limit,))
        return cur.fetchall()


def crawl_site(start_url: str, conn, max_pages: int = 50):
    """Crawl a site following internal links."""
    from collections import deque
    base_domain = urlparse(start_url).netloc
    visited = set()
    queue = deque([start_url])

    while queue and len(visited) < max_pages:
        url = queue.popleft()
        h = url_hash(url)
        if h in visited:
            continue
        visited.add(h)

        print(f"\n[{len(visited)}/{max_pages}]")
        ok = scrape(url, conn)
        if ok:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT l.target_url FROM links l
                    WHERE l.source_hash = %s AND l.target_url IS NOT NULL
                """, (h,))
                for row in cur.fetchall():
                    target = row["target_url"]
                    if target and urlparse(target).netloc == base_domain:
                        nh = url_hash(target)
                        if nh not in visited:
                            queue.append(target)
        time.sleep(DELAY)


# ── CLI ─────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 scraper_v2.py <url>              Scrape single URL")
        print("  python3 scraper_v2.py -f urls.txt        Scrape from file")
        print("  python3 scraper_v2.py --auto 20          Auto-crawl from queue")
        print("  python3 scraper_v2.py --search 'query'   Search")
        print("  python3 scraper_v2.py --stats            Show stats")
        sys.exit(1)

    init_db()
    conn = get_conn()

    if sys.argv[1] == "-f":
        with open(sys.argv[2]) as f:
            urls = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        print(f"📋 Loaded {len(urls)} URLs from {sys.argv[2]}")
        results = batch_scrape(urls, conn)
        print(f"\n📊 Results: {results}")
    elif sys.argv[1] == "--stats":
        print(get_stats(conn))
    elif sys.argv[1] == "--search":
        query = " ".join(sys.argv[2:])
        results = search(query, conn)
        for r in results:
            print(f"  {r['title'][:60]} — {r['url']}")
    else:
        scrape(sys.argv[1], conn)

    conn.close()
