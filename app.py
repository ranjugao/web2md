#!/usr/bin/env python3
"""Web Scraper Dashboard - Visualize scraped data."""

import os
from flask import Flask, render_template, jsonify, request

import psycopg2
import psycopg2.extras
import markdown as md_lib

app = Flask(__name__)

DB_CONFIG = {
    "host": os.getenv("SCRAPER_DB_HOST", "localhost"),
    "port": os.getenv("SCRAPER_DB_PORT", "5432"),
    "dbname": os.getenv("SCRAPER_DB_NAME", "web_scraper"),
    "user": os.getenv("SCRAPER_DB_USER", "scraper"),
    "password": os.getenv("SCRAPER_DB_PASS", "scraper2026"),
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats")
def api_stats():
    conn = get_db()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT COUNT(*) as pages,
                   COALESCE(SUM(word_count), 0) as words,
                   COUNT(DISTINCT domain) as domains,
                   COALESCE(SUM(out_link_count), 0) as total_links
            FROM pages
        """)
        stats = dict(cur.fetchone())

        # Domain distribution
        cur.execute("""
            SELECT domain, COUNT(*) as cnt, SUM(word_count) as words
            FROM pages GROUP BY domain ORDER BY cnt DESC
        """)
        stats["domains_list"] = [dict(r) for r in cur.fetchall()]

        # Top pages by links
        cur.execute("""
            SELECT url, title, word_count, out_link_count, in_link_count
            FROM page_rank LIMIT 20
        """)
        stats["top_pages"] = [dict(r) for r in cur.fetchall()]

        # Recent pages
        cur.execute("""
            SELECT url_hash, url, title, domain, word_count, out_link_count, scraped_at
            FROM pages ORDER BY scraped_at DESC LIMIT 20
        """)
        stats["recent_pages"] = [dict(r) for r in cur.fetchall()]

    conn.close()
    return jsonify(stats)


@app.route("/api/graph")
def api_graph():
    """Return link graph data for D3 visualization."""
    conn = get_db()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Get all pages as nodes
        cur.execute("""
            SELECT url_hash, url, title, domain, out_link_count, in_link_count
            FROM pages
        """)
        pages = [dict(r) for r in cur.fetchall()]
        page_map = {p["url_hash"]: p for p in pages}

        # Get all links as edges
        cur.execute("SELECT source_hash, target_hash, anchor_text FROM links")
        links = [dict(r) for r in cur.fetchall()]

    conn.close()

    nodes = []
    for p in pages:
        nodes.append({
            "id": p["url_hash"],
            "label": (p["title"] or p["url"])[:40],
            "url": p["url"],
            "domain": p["domain"],
            "size": max(5, min(30, (p["out_link_count"] or 0) + (p["in_link_count"] or 0) + 3)),
        })

    edges = []
    for l in links:
        if l["target_hash"] in page_map:
            edges.append({
                "source": l["source_hash"],
                "target": l["target_hash"],
                "label": (l["anchor_text"] or "")[:30],
            })

    return jsonify({"nodes": nodes, "links": edges})


@app.route("/api/page/<hash_id>")
def api_page(hash_id):
    conn = get_db()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM pages WHERE url_hash = %s", (hash_id,))
        page = cur.fetchone()
        if not page:
            conn.close()
            return jsonify({"error": "not found"}), 404

        cur.execute("""
            SELECT l.target_url, l.anchor_text, p.title as target_title
            FROM links l LEFT JOIN pages p ON l.target_hash = p.url_hash
            WHERE l.source_hash = %s
        """, (hash_id,))
        outlinks = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT l.source_hash, l.anchor_text, p.title as source_title, p.url as source_url
            FROM links l LEFT JOIN pages p ON l.source_hash = p.url_hash
            WHERE l.target_hash = %s
        """, (hash_id,))
        inlinks = [dict(r) for r in cur.fetchall()]

    conn.close()
    return jsonify({
        "page": dict(page),
        "outlinks": outlinks,
        "inlinks": inlinks,
    })


@app.route("/api/search")
def api_search():
    q = request.args.get("q", "")
    if not q:
        return jsonify([])
    conn = get_db()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT url_hash, url, title, domain, word_count, scraped_at
            FROM pages
            WHERE to_tsvector('english', coalesce(title, '')) @@ plainto_tsquery('english', %s)
               OR markdown ILIKE %s
            ORDER BY scraped_at DESC LIMIT 20
        """, (q, f"%{q}%"))
        results = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify(results)


@app.route("/page/<hash_id>")
def page_view(hash_id):
    conn = get_db()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM pages WHERE url_hash = %s", (hash_id,))
        page = cur.fetchone()
        if not page:
            conn.close()
            return "Page not found", 404
        page = dict(page)

        cur.execute("""
            SELECT l.target_url, l.anchor_text, p.title as target_title
            FROM links l LEFT JOIN pages p ON l.target_hash = p.url_hash
            WHERE l.source_hash = %s
        """, (hash_id,))
        outlinks = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT l.source_hash, l.anchor_text, p.title as source_title, p.url as source_url
            FROM links l LEFT JOIN pages p ON l.source_hash = p.url_hash
            WHERE l.target_hash = %s
        """, (hash_id,))
        inlinks = [dict(r) for r in cur.fetchall()]

    conn.close()

    # Render markdown to HTML
    md_html = md_lib.markdown(
        page.get("markdown", ""),
        extensions=["tables", "fenced_code", "nl2br"],
        output_format="html"
    )

    return render_template("page.html", page=page, md_html=md_html,
                           outlinks=outlinks, inlinks=inlinks)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5555, debug=True)
