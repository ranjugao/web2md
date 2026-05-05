#!/usr/bin/env python3
"""
Web Scraper → Markdown → PostgreSQL
Simulates browser, extracts readable content, stores as markdown.
URL is the unique key (SHA256 hash of normalized URL).
"""

import hashlib
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin, parse_qs, urlencode

import psycopg2
import psycopg2.extras
import requests
from readability import Document
from bs4 import BeautifulSoup
from markdownify import markdownify as md


# --- DB Config ---
DB_CONFIG = {
    "host": os.getenv("SCRAPER_DB_HOST", "localhost"),
    "port": os.getenv("SCRAPER_DB_PORT", "5432"),
    "dbname": os.getenv("SCRAPER_DB_NAME", "web_scraper"),
    "user": os.getenv("SCRAPER_DB_USER", "scraper"),
    "password": os.getenv("SCRAPER_DB_PASS", "scraper2026"),
}

TIMEOUT = 15
DELAY = 1
USE_BROWSER = True  # Enable Playwright fallback for CF-protected sites
RESPECT_ROBOTS = False  # We don't care about robots.txt

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

ROBOTS_CACHE = {}  # domain -> {rules, crawl_delay, disallowed_paths}


def check_robots(url: str) -> bool:
    """Check if URL is allowed by robots.txt. Returns True if allowed."""
    if not RESPECT_ROBOTS:
        return True

    parsed = urlparse(url)
    domain = parsed.netloc
    base = f"{parsed.scheme}://{domain}"

    # Check cache
    if domain not in ROBOTS_CACHE:
        robots_url = f"{base}/robots.txt"
        try:
            resp = requests.get(robots_url, timeout=5, headers={"User-Agent": HEADERS["User-Agent"]})
            if resp.status_code != 200:
                ROBOTS_CACHE[domain] = {"allowed": True, "rules": {}}
                return True

            rules = {"*": []}
            current_agent = "*"
            for line in resp.text.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower().startswith("user-agent:"):
                    current_agent = line.split(":", 1)[1].strip()
                elif line.lower().startswith("disallow:"):
                    path = line.split(":", 1)[1].strip()
                    if path:
                        rules.setdefault(current_agent, []).append(path)
                elif line.lower().startswith("crawl-delay:"):
                    delay = line.split(":", 1)[1].strip()
                    rules["crawl_delay"] = float(delay)

            ROBOTS_CACHE[domain] = rules
        except Exception:
            ROBOTS_CACHE[domain] = {}
            return True

    rules = ROBOTS_CACHE.get(domain, {})
    url_path = parsed.path

    # Check against our user-agent and wildcard
    for agent in [HEADERS["User-Agent"], "*"]:
        disallowed = rules.get(agent, [])
        for pattern in disallowed:
            if url_path.startswith(pattern):
                return False

    return True


def get_conn():
    """Get PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)


def init_db():
    """Create table if not exists."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                url_hash       VARCHAR(16) PRIMARY KEY,
                url            TEXT NOT NULL,
                domain         VARCHAR(255),
                title          TEXT,
                author         TEXT DEFAULT '',
                pub_date       TEXT DEFAULT '',
                description    TEXT,
                markdown       TEXT NOT NULL,
                word_count     INTEGER,
                out_link_count INTEGER DEFAULT 0,
                in_link_count  INTEGER DEFAULT 0,
                scraped_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                status_code    INTEGER,
                content_type   TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS links (
                source_hash  VARCHAR(16) NOT NULL,
                target_hash  VARCHAR(16) NOT NULL,
                target_url   TEXT,
                anchor_text  TEXT,
                scraped_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (source_hash, target_hash)
            )
        """)
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_hash)",
            "CREATE INDEX IF NOT EXISTS idx_links_target ON links(target_hash)",
            "CREATE INDEX IF NOT EXISTS idx_domain ON pages(domain)",
            "CREATE INDEX IF NOT EXISTS idx_scraped ON pages(scraped_at)",
            "CREATE INDEX IF NOT EXISTS idx_title_gin ON pages USING gin(to_tsvector('english', coalesce(title, '')))",
            "CREATE INDEX IF NOT EXISTS idx_md_gin ON pages USING gin(to_tsvector('english', coalesce(markdown, '')))",
        ]:
            try:
                cur.execute(idx_sql)
            except Exception:
                conn.rollback()
                continue
    conn.commit()
    conn.close()


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    fragmentless = parsed._replace(fragment="")
    query = parse_qs(fragmentless.query, keep_blank_values=True)
    sorted_query = urlencode(sorted(query.items()), doseq=True)
    return fragmentless._replace(query=sorted_query).geturl()


def url_hash(url: str) -> str:
    return hashlib.sha256(normalize_url(url).encode()).hexdigest()[:16]


def classify_url(url: str) -> str:
    """Classify URL as 'listing' (homepage/index) or 'detail' (article/page)."""
    path = urlparse(url).path.strip("/")

    # Homepage or single-level path → listing
    if not path or path.count("/") == 0:
        return "listing"

    # URL contains date → detail
    if re.search(r"/20\d{2}[/\-]", path):
        return "detail"

    # Path has 3+ segments → detail
    if path.count("/") >= 2:
        return "detail"

    # Short path → listing
    return "listing"


def content_hash(text: str) -> str:
    """Hash content for change detection."""
    return hashlib.md5(text.encode()).hexdigest()[:16]


def fetch_page(url: str) -> dict:
    """Fetch a page, with Playwright fallback for CF-protected sites."""
    # Try requests first
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        resp = session.get(url, timeout=TIMEOUT, allow_redirects=True)
        body = resp.text
        # Detect CF challenge page
        is_cf = any(x in body for x in [
            "cf-browser-verification", "challenge-platform",
            "Checking your browser", "Just a moment",
            "Enable JavaScript", "cf-turnstile", "ray ID",
        ])
        if resp.status_code == 403 or is_cf:
            raise requests.RequestException("Cloudflare detected")
        resp.raise_for_status()
        return {
            "html": body,
            "status_code": resp.status_code,
            "content_type": resp.headers.get("Content-Type", ""),
        }
    except Exception as e:
        if not USE_BROWSER:
            raise
        print(f"  ⚠️ requests failed ({e}), trying Playwright...")

    # Playwright fallback
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
            ])
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
            )
            # Remove webdriver flag
            ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page = ctx.new_page()
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            # Wait for CF challenge to resolve (up to 10s)
            page.wait_for_timeout(5000)
            # Check if still on challenge page
            for _ in range(3):
                content = page.content()
                if not any(x in content for x in ["Just a moment", "Checking your browser", "cf-browser-verification"]):
                    break
                page.wait_for_timeout(3000)

            html = page.content()
            browser.close()

            return {
                "html": html,
                "status_code": 200,
                "content_type": "text/html",
            }
    except Exception as e2:
        raise Exception(f"Both requests and Playwright failed: {e2}")


def extract_content(html: str, base_url: str) -> dict:
    """Extract readable content and convert to clean AI-friendly markdown."""
    parsed_base = urlparse(base_url)

    # --- Title extraction (try multiple strategies) ---
    soup_full = BeautifulSoup(html, "lxml")
    title = ""

    # Strategy 1: readability title
    doc = Document(html)
    title = doc.title()

    # Strategy 2: <h1> tag
    if not title or len(title) < 5:
        h1 = soup_full.find("h1")
        if h1:
            title = h1.get_text(strip=True)

    # Strategy 3: <title> tag (clean up site name suffixes)
    if not title or len(title) < 5:
        title_tag = soup_full.find("title")
        if title_tag:
            raw = title_tag.get_text(strip=True)
            # Remove common suffixes: " | SiteName", " - SiteName", " – SiteName"
            for sep in [" | ", " – ", " — ", " - ", " :: ", " • "]:
                if sep in raw:
                    raw = raw.rsplit(sep, 1)[0]
                    break
            title = raw

    # Strategy 4: og:title
    if not title or len(title) < 5:
        og = soup_full.find("meta", property="og:title")
        if og:
            title = og.get("content", "")

    # Strategy 5: first meaningful text
    if not title or len(title) < 5:
        for tag in soup_full.find_all(["h1", "h2", "article"]):
            t = tag.get_text(strip=True)
            if t and len(t) > 5:
                title = t[:200]
                break

    title = (title or "(untitled)")[:300]

    # --- Description / snippet extraction ---
    description = ""
    for meta_name in ["description", "og:description", "twitter:description"]:
        if meta_name.startswith("og:"):
            m = soup_full.find("meta", attrs={"property": meta_name})
        else:
            m = soup_full.find("meta", attrs={"name": meta_name})
        if m and m.get("content"):
            description = m["content"][:500]
            break

    # --- Main content extraction ---
    # Try readability first
    summary_html = doc.summary()
    soup = BeautifulSoup(summary_html, "lxml")

    # Fallback: if readability gives empty/bad content, try article/section/body
    text_len = len(soup.get_text(strip=True))
    if text_len < 80:
        article = (
            soup_full.find("article")
            or soup_full.find("main")
            or soup_full.find(attrs={"role": "main"})
            or soup_full.find("div", class_=lambda c: c and any(
                x in c.lower() for x in ["article", "content", "post", "entry", "story", "body"]
            ))
        )
        if article:
            soup = BeautifulSoup(str(article), "lxml")
        else:
            soup = BeautifulSoup(html, "lxml")
        title_tag = soup.find("title")
        if title_tag and (not title or title == "(untitled)"):
            title = title_tag.get_text(strip=True)

    # --- Aggressive cleanup ---
    # Remove noise elements
    remove_tags = [
        "script", "style", "noscript", "iframe", "svg", "form",
        "button", "input", "select", "textarea",
    ]
    for tag in soup.find_all(remove_tags):
        tag.decompose()

    # Remove nav, footer, sidebar, ads, related, comments
    remove_by_role = ["navigation", "banner", "complementary", "contentinfo"]
    for role in remove_by_role:
        for tag in soup.find_all(attrs={"role": role}):
            tag.decompose()

    remove_by_tag = ["nav", "footer", "aside", "header", "figcaption"]
    for tag in soup.find_all(remove_by_tag):
        tag.decompose()

    # Remove by class/id keywords
    noise_keywords = [
        "ad", "sidebar", "widget", "popup", "modal", "cookie", "banner",
        "related", "comment", "share", "social", "newsletter", "subscribe",
        "author-bio", "post-navigation", "prev-next", "breadcrumb",
        "table-of-contents", "toc", "tags", "category", "meta-info",
        "signup", "login", "menu", "nav-", "footer-",
    ]
    for tag in soup.find_all(True):
        if tag.attrs is None:
            continue
        cls = " ".join(tag.get("class", []) or [])
        tid = tag.get("id", "") or ""
        combined = f"{cls} {tid}".lower()
        if any(kw in combined for kw in noise_keywords):
            tag.decompose()

    # --- Extract metadata ---
    # Author
    author = ""
    for sel in ['[rel="author"]', '.author', '.byline', '[itemprop="author"]']:
        el = soup.select_one(sel)
        if el:
            author = el.get_text(strip=True)[:100]
            break
    if not author:
        m = soup_full.find("meta", attrs={"name": "author"})
        if m:
            author = m.get("content", "")[:100]

    # Published date
    pub_date = ""
    for attr_name in ["article:published_time", "datePublished", "publish_date"]:
        if attr_name.startswith("article:"):
            m = soup_full.find("meta", attrs={"property": attr_name})
        elif attr_name == "datePublished":
            m = soup_full.find("meta", attrs={"itemprop": attr_name})
        else:
            m = soup_full.find("meta", attrs={"name": attr_name})
        if m:
            pub_date = m.get("content", "")[:30]
            break
    if not pub_date:
        time_tag = soup.find("time")
        if time_tag:
            pub_date = time_tag.get("datetime", time_tag.get_text(strip=True))[:30]

    # --- Process images to absolute URLs ---
    for img in soup.find_all("img"):
        src = img.get("src", "") or img.get("data-src", "")
        alt = img.get("alt", "").strip()
        if src and not src.startswith("data:"):
            abs_src = urljoin(base_url, src)
            img.replace_with(f"\n\n![{alt}]({abs_src})\n\n")
        else:
            img.decompose()

    # --- Extract links (before converting to markdown) ---
    links = []
    for a in soup.find_all("a"):
        href = a.get("href", "")
        if href and not href.startswith(("#", "javascript:", "mailto:", "tel:")):
            abs_url = urljoin(base_url, href)
            anchor = a.get_text(strip=True)[:200]
            links.append({"url": abs_url, "anchor": anchor})
            a["href"] = abs_url

    # --- Convert to markdown ---
    markdown_text = md(
        str(soup),
        heading_style="ATX",
        bullets="-",
        strip=["img"],
    )

    # --- Clean up markdown ---
    lines = markdown_text.split("\n")
    cleaned = []
    prev_blank = False
    for line in lines:
        stripped = line.strip()
        is_blank = not stripped
        if is_blank and prev_blank:
            continue
        # Remove isolated pipe characters (empty table cells)
        if stripped == "|":
            continue
        cleaned.append(stripped)
        prev_blank = is_blank
    markdown_text = "\n".join(cleaned).strip()

    # --- Build clean text for word count ---
    text = soup.get_text(separator="\n", strip=True)
    word_count = len(text.split())

    # --- Auto-generate description if none from meta ---
    if not description:
        # Take first 2-3 paragraphs
        paragraphs = []
        for p in soup.find_all("p"):
            t = p.get_text(strip=True)
            if len(t) > 30:
                paragraphs.append(t)
            if len(paragraphs) >= 3:
                break
        description = " ".join(paragraphs)[:500]

    # --- Build structured markdown header ---
    header_parts = []
    if title and title != "(untitled)":
        header_parts.append(f"# {title}")
    if author:
        header_parts.append(f"**Author:** {author}")
    if pub_date:
        header_parts.append(f"**Published:** {pub_date}")
    header_parts.append(f"**Source:** {base_url}")
    if description:
        header_parts.append(f"\n> {description}")
    header_parts.append("")

    full_markdown = "\n".join(header_parts) + markdown_text

    return {
        "title": title,
        "description": description[:500],
        "markdown": full_markdown,
        "word_count": word_count,
        "links": links,
        "author": author,
        "pub_date": pub_date,
    }


def scrape(url: str, conn) -> bool:
    h = url_hash(url)
    page_type = classify_url(url)

    # Smart dedup
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        existing = cur.execute(
            "SELECT url_hash, content_hash FROM pages WHERE url_hash = %s", (h,)
        ) or None
        existing = cur.fetchone() if cur.rowcount > 0 else None

        if existing:
            if page_type == "detail":
                print(f"  ⏭ Detail page already scraped: {url}")
                return False

    # Check robots.txt
    if not check_robots(url):
        print(f"  🚫 Blocked by robots.txt: {url}")
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
                # Update existing listing page
                cur.execute("""
                    UPDATE pages SET
                        title = %s, author = %s, pub_date = %s,
                        description = %s, markdown = %s, word_count = %s,
                        page_type = %s, content_hash = %s, scraped_at = %s,
                        status_code = %s, content_type = %s
                    WHERE url_hash = %s
                """, (
                    content["title"],
                    content.get("author", ""), content.get("pub_date", ""),
                    content["description"], content["markdown"], content["word_count"],
                    page_type, c_hash, datetime.now(timezone.utc),
                    page["status_code"], page["content_type"], h,
                ))
                # Re-extract links
                cur.execute("DELETE FROM links WHERE source_hash = %s", (h,))
            else:
                # Insert new page
                cur.execute("""
                    INSERT INTO pages (url_hash, url, domain, title, author, pub_date,
                                       description, markdown, word_count, page_type,
                                       content_hash, scraped_at, status_code, content_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    h, url, parsed.netloc, content["title"],
                    content.get("author", ""), content.get("pub_date", ""),
                    content["description"], content["markdown"], content["word_count"],
                    page_type, c_hash,
                    datetime.now(timezone.utc),
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

            # Update out_link_count
            cur.execute("""
                UPDATE pages SET out_link_count = (
                    SELECT COUNT(*) FROM links WHERE source_hash = %s
                ) WHERE url_hash = %s
            """, (h, h))
        conn.commit()
        print(f"  ✅ Saved: {content['title'][:50]} ({content['word_count']} words, {len(content['links'])} links)")
        return True

    except requests.RequestException as e:
        print(f"  ❌ Request failed: {e}")
        return False
    except Exception as e:
        conn.rollback()
        print(f"  ❌ Parse failed: {e}")
        return False


def batch_scrape(urls: list, conn) -> dict:
    results = {"success": 0, "skipped": 0, "failed": 0}
    total = len(urls)

    for i, url in enumerate(urls, 1):
        print(f"\n[{i}/{total}]")
        url = url.strip()
        if not url or not url.startswith(("http://", "https://")):
            print(f"  ⚠ Invalid URL, skipping: {url}")
            results["failed"] += 1
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
               OR markdown LIKE %s
            ORDER BY scraped_at DESC
            LIMIT %s
        """, (query, query, f"%{query}%", limit))
        return cur.fetchall()


def search_like(query: str, conn, limit: int = 10) -> list:
    """Fallback LIKE search."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT url, title, description, word_count, scraped_at
            FROM pages
            WHERE markdown ILIKE %s OR title ILIKE %s
            ORDER BY scraped_at DESC
            LIMIT %s
        """, (f"%{query}%", f"%{query}%", limit))
        return cur.fetchall()


def get_stats(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) as total,
                   COALESCE(SUM(word_count), 0) as total_words,
                   COUNT(DISTINCT domain) as domains
            FROM pages
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
    """Get all pages this URL links to."""
    h = url_hash(key) if not key.startswith("{") else key
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT l.target_url, l.anchor_text,
                   p.title AS target_title, p.url_hash AS target_hash
            FROM links l
            LEFT JOIN pages p ON l.target_hash = p.url_hash
            WHERE l.source_hash = %s
            ORDER BY l.scraped_at DESC
        """, (h,))
        return cur.fetchall()


def get_inlinks(key: str, conn) -> list:
    """Get all pages that link to this URL."""
    h = url_hash(key) if not key.startswith("{") else key
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT l.source_hash, l.anchor_text,
                   p.title AS source_title, p.url AS source_url
            FROM links l
            LEFT JOIN pages p ON l.source_hash = p.url_hash
            WHERE l.target_hash = %s
            ORDER BY l.scraped_at DESC
        """, (h,))
        return cur.fetchall()


def get_graph(conn, limit: int = 20) -> list:
    """Get pages ranked by link connections."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT url, title, word_count, out_link_count, in_link_count, scraped_at
            FROM page_rank
            LIMIT %s
        """, (limit,))
        return cur.fetchall()


def get_domain_graph(domain: str, conn) -> list:
    """Get all pages and links for a domain."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT url, title, word_count, out_link_count, in_link_count, scraped_at
            FROM pages
            WHERE domain = %s
            ORDER BY scraped_at DESC
        """, (domain,))
        return cur.fetchall()


def crawl_site(start_url: str, conn, max_pages: int = 50):
    """Crawl a site starting from a URL, following internal links."""
    from urllib.parse import urljoin
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc

    queue = [start_url]
    visited = set()

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        h = url_hash(url)

        if h in visited:
            continue
        visited.add(h)

        # Check if already scraped
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pages WHERE url_hash = %s", (h,))
            if cur.fetchone():
                # Still extract links from existing page
                out = get_outlinks(url, conn)
                for link in out:
                    target = link.get("target_url") or ""
                    if urlparse(target).netloc == base_domain:
                        nh = url_hash(target)
                        if nh not in visited:
                            queue.append(target)
                continue

        # Scrape new page
        print(f"\n🔗 [{len(visited)}/{max_pages}] Crawling: {url}")
        ok = scrape(url, conn)
        if ok:
            # Add internal links to queue
            out = get_outlinks(url, conn)
            for link in out:
                target = link.get("target_url") or ""
                if urlparse(target).netloc == base_domain:
                    nh = url_hash(target)
                    if nh not in visited:
                        queue.append(target)

        time.sleep(DELAY)

    print(f"\n🏁 Crawled {len(visited)} pages from {base_domain}")


def migrate_sqlite(sqlite_path: str, pg_conn):
    """Migrate data from existing SQLite database."""
    import sqlite3
    if not os.path.exists(sqlite_path):
        print(f"SQLite file not found: {sqlite_path}")
        return

    sqlite_conn = sqlite3.connect(sqlite_path)
    rows = sqlite_conn.execute("SELECT * FROM pages").fetchall()
    cols = [d[0] for d in sqlite_conn.execute("SELECT * FROM pages").description]
    sqlite_conn.close()

    migrated = 0
    skipped = 0
    with pg_conn.cursor() as cur:
        for row in rows:
            data = dict(zip(cols, row))
            try:
                cur.execute("""
                    INSERT INTO pages (url_hash, url, domain, title, description,
                                       markdown, word_count, scraped_at, status_code, content_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url_hash) DO NOTHING
                """, (
                    data["url_hash"], data["url"], data["domain"], data["title"],
                    data["description"], data["markdown"], data["word_count"],
                    data["scraped_at"], data["status_code"], data["content_type"],
                ))
                migrated += 1
            except Exception as e:
                skipped += 1
                print(f"  Skip {data.get('url', '?')}: {e}")
    pg_conn.commit()
    print(f"📦 Migrated {migrated} rows, skipped {skipped}")


# --- CLI ---
if __name__ == "__main__":
    init_db()
    conn = get_conn()

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 scraper.py <url>              Scrape a single URL")
        print("  python3 scraper.py -f urls.txt        Scrape URLs from file")
        print("  python3 scraper.py -c <url> [--depth N]  Crawl site (follow internal links)")
        print("  python3 scraper.py -s <keyword>       Search stored pages")
        print("  python3 scraper.py -i                 Show database stats")
        print("  python3 scraper.py -o <url/hash>      Export markdown")
        print("  python3 scraper.py -out <url>         Show outbound links")
        print("  python3 scraper.py -in <url>          Show inbound links")
        print("  python3 scraper.py -g                 Show link graph (top pages)")
        print("  python3 scraper.py -d <domain>        Show domain link graph")
        print("  python3 scraper.py -m <sqlite_path>   Migrate from SQLite")
        sys.exit(0)

    if sys.argv[1] == "-f":
        with open(sys.argv[2]) as f:
            urls = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        print(f"📋 Loaded {len(urls)} URLs from {sys.argv[2]}")
        results = batch_scrape(urls, conn)
        print(f"\n📊 Done: {results['success']} saved, {results['skipped']} skipped, {results['failed']} failed")

    elif sys.argv[1] == "-s":
        query = " ".join(sys.argv[2:])
        rows = search(query, conn)
        if not rows:
            rows = search_like(query, conn)
        if not rows:
            print("No results found.")
        for r in rows:
            print(f"\n📄 {r['title']}")
            print(f"   {r['url']}")
            print(f"   {r['word_count']} words | {str(r['scraped_at'])[:10]}")
            if r['description']:
                print(f"   {r['description'][:100]}...")

    elif sys.argv[1] == "-i":
        stats = get_stats(conn)
        print(f"📊 Pages: {stats['total_pages']} | Words: {stats['total_words']:,} | Domains: {stats['domains']}")

    elif sys.argv[1] == "-o":
        print(export_markdown(sys.argv[2], conn))

    elif sys.argv[1] == "-out":
        links = get_outlinks(sys.argv[2], conn)
        if not links:
            print("No outbound links found (or page not scraped yet).")
        else:
            print(f"📎 {len(links)} outbound links:")
            for l in links:
                title = l.get('target_title') or '(not scraped)'
                print(f"  → {title[:60]}")
                print(f"    {l['target_url']}")
                if l["anchor_text"]:
                    print(f"    \"{l['anchor_text'][:60]}\"")

    elif sys.argv[1] == "-in":
        links = get_inlinks(sys.argv[2], conn)
        if not links:
            print("No inbound links found.")
        else:
            print(f"📎 {len(links)} inbound links:")
            for l in links:
                src = l.get('source_url') or l['source_hash']
                title = l.get('source_title') or '(not scraped)'
                print(f"  ← {title[:60]}")
                print(f"    {src}")

    elif sys.argv[1] == "-g":
        graph = get_graph(conn)
        if not graph:
            print("No data.")
        else:
            print("📊 Link Graph (pages ranked by connections):")
            for p in graph:
                print(f"  {p['in_link_count']}↑ {p['out_link_count']}↓ | {p['title'][:50] or p['url'][:50]}")
                print(f"    {p['url']}")

    elif sys.argv[1] == "-d":
        pages = get_domain_graph(sys.argv[2], conn)
        if not pages:
            print("No pages found for this domain.")
        else:
            print(f"📊 Domain: {sys.argv[2]} ({len(pages)} pages)")
            for p in pages:
                print(f"  {p['in_link_count']}↑ {p['out_link_count']}↓ | {p['title'][:50] or '(no title)'} ({p['word_count']}w)")

    elif sys.argv[1] == "-c":
        depth = 50
        if "--depth" in sys.argv:
            idx = sys.argv.index("--depth")
            depth = int(sys.argv[idx + 1])
        crawl_site(sys.argv[2], conn, max_pages=depth)

    elif sys.argv[1] == "-m":
        migrate_sqlite(sys.argv[2], conn)

    else:
        scrape(sys.argv[1], conn)

    conn.close()
