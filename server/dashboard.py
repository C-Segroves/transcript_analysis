"""
dashboard.py

Lightweight web dashboard for the transcript-analysis task server.

Runs a small stdlib HTTP server (no extra dependencies beyond psycopg2, which
the server already uses) in a background thread alongside the asyncio task
server, so a single container/process serves both:

  * port 5000  -> task distribution to clients (async_processing_server)
  * port 8080  -> this web dashboard

The dashboard shows basic pipeline stats and lets you add a new YouTube
channel by handle. It queries the LIVE schema used by maintain_database.py:

  channel_table       (id, yt_channel_id, channel_handle, channel_snippet, insert_at)
  vid_table           (id, channel_id -> channel_table.id, yt_vid_id, insert_at)
  vid_data_table      (vid_id -> vid_table.id, title, ...)            # video metadata / "info"
  vid_transcript_table(vid_id -> vid_table.id, ..., word_count)        # downloaded transcripts
  vid_score_table     (vid_id -> vid_table.id, model_id, score)        # processed / scored

Start it with: start_dashboard_in_thread(db_config, api_key_path, logger, host, port)
"""

import html
import json
import logging
import threading
import urllib.parse
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg2


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

DEFAULT_DB_CONFIG_PATH = "config/db_config.json"
DEFAULT_API_KEY_PATH = "config/YouTube.txt"


def load_db_config(path=DEFAULT_DB_CONFIG_PATH):
    with open(path, "r") as f:
        return json.load(f)


def load_api_key(path=DEFAULT_API_KEY_PATH):
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Database access (one short-lived connection per request; the dashboard is
# low-traffic so this keeps things simple and avoids sharing the asyncio pool)
# ---------------------------------------------------------------------------

def _get_connection(db_config):
    return psycopg2.connect(**db_config)


def _scalar(cur, query):
    """Run a COUNT-style query and return the single value, or 0 on error."""
    try:
        cur.execute(query)
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else 0
    except Exception:
        # Roll back the failed statement so the connection stays usable.
        try:
            cur.connection.rollback()
        except Exception:
            pass
        return None


def fetch_stats(db_config, logger):
    """Return a dict of pipeline-wide counts."""
    stats = {
        "channels": None,
        "videos_discovered": None,
        "videos_with_info": None,
        "transcripts_downloaded": None,
        "videos_processed": None,
    }
    conn = None
    try:
        conn = _get_connection(db_config)
        with conn.cursor() as cur:
            stats["channels"] = _scalar(cur, "SELECT COUNT(*) FROM channel_table")
            stats["videos_discovered"] = _scalar(cur, "SELECT COUNT(*) FROM vid_table")
            stats["videos_with_info"] = _scalar(
                cur, "SELECT COUNT(DISTINCT vid_id) FROM vid_data_table"
            )
            stats["transcripts_downloaded"] = _scalar(
                cur, "SELECT COUNT(DISTINCT vid_id) FROM vid_transcript_table"
            )
            stats["videos_processed"] = _scalar(
                cur, "SELECT COUNT(DISTINCT vid_id) FROM vid_score_table"
            )
    except Exception as e:
        logger.error(f"Dashboard: failed to fetch stats: {e}")
    finally:
        if conn is not None:
            conn.close()
    return stats


def fetch_channels(db_config, logger):
    """Return a list of channel rows with per-channel video/transcript counts."""
    query = """
        SELECT
            c.channel_handle,
            c.channel_snippet->>'title' AS title,
            c.yt_channel_id,
            COUNT(DISTINCT v.id) AS videos,
            COUNT(DISTINCT t.vid_id) AS transcripts,
            c.insert_at
        FROM channel_table c
        LEFT JOIN vid_table v ON v.channel_id = c.id
        LEFT JOIN vid_transcript_table t ON t.vid_id = v.id
        GROUP BY c.id, c.channel_handle, c.channel_snippet, c.yt_channel_id, c.insert_at
        ORDER BY c.insert_at DESC NULLS LAST, c.id DESC
    """
    conn = None
    try:
        conn = _get_connection(db_config)
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            return [
                {
                    "handle": r[0],
                    "title": r[1],
                    "yt_channel_id": r[2],
                    "videos": r[3],
                    "transcripts": r[4],
                    "insert_at": r[5],
                }
                for r in rows
            ]
    except Exception as e:
        logger.error(f"Dashboard: failed to fetch channels: {e}")
        return None
    finally:
        if conn is not None:
            conn.close()


# ---------------------------------------------------------------------------
# Add-channel logic (fetch from YouTube Data API, insert into channel_table)
# ---------------------------------------------------------------------------

def fetch_channel_details(channel_handle, api_key, logger):
    """Resolve a YouTube handle to (channel_id, snippet) via the Data API."""
    handle = channel_handle.lstrip("@").strip()
    try:
        url = (
            "https://www.googleapis.com/youtube/v3/channels"
            f"?key={api_key}&forHandle={urllib.parse.quote(handle)}&part=snippet"
        )
        with urllib.request.urlopen(url, timeout=10) as response:
            resp = json.load(response)
        items = resp.get("items") or []
        if not items:
            return None, None
        channel_id = items[0]["id"]
        snippet = items[0].get("snippet", {})
        return channel_id, snippet
    except Exception as e:
        logger.error(f"Dashboard: error fetching channel details for '{channel_handle}': {e}")
        return None, None


def add_channel(channel_handle, db_config, api_key, logger):
    """Returns (ok: bool, message: str)."""
    handle = (channel_handle or "").strip()
    if not handle:
        return False, "Please enter a channel handle."
    if not api_key:
        return False, "No YouTube API key configured (config/YouTube.txt)."

    channel_id, snippet = fetch_channel_details(handle, api_key, logger)
    if not channel_id:
        return False, f"Could not find a YouTube channel for handle '{handle}'."

    conn = None
    try:
        conn = _get_connection(db_config)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM channel_table WHERE yt_channel_id = %s", (channel_id,)
            )
            if cur.fetchone():
                title = (snippet or {}).get("title", handle)
                return False, f"Channel '{title}' ({handle}) is already in the database."

            cur.execute(
                """
                INSERT INTO channel_table (yt_channel_id, channel_handle, channel_snippet, insert_at)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    channel_id,
                    handle.lstrip("@"),
                    json.dumps(snippet),
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                ),
            )
        conn.commit()
        title = (snippet or {}).get("title", handle)
        logger.info(f"Dashboard: added channel '{title}' ({handle} / {channel_id})")
        return True, f"Added channel '{title}' ({handle}). New videos will be pulled on the next maintenance run."
    except Exception as e:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.error(f"Dashboard: error inserting channel '{handle}': {e}")
        return False, f"Error adding channel: {e}"
    finally:
        if conn is not None:
            conn.close()


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

def _fmt(n):
    if n is None:
        return "&mdash;"
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return html.escape(str(n))


def _esc(s):
    if s is None:
        return ""
    return html.escape(str(s))


def render_page(stats, channels, message=None, message_ok=True):
    pct = ""
    if stats.get("transcripts_downloaded") and stats.get("videos_processed") is not None:
        try:
            denom = stats["transcripts_downloaded"]
            if denom:
                pct = f"{(stats['videos_processed'] / denom) * 100:.1f}%"
        except Exception:
            pct = ""

    cards = [
        ("Channels", stats.get("channels"), "channels tracked"),
        ("Videos discovered", stats.get("videos_discovered"), "in vid_table"),
        ("Videos with info", stats.get("videos_with_info"), "title/description pulled"),
        ("Transcripts downloaded", stats.get("transcripts_downloaded"), "have transcript text"),
        ("Videos processed", stats.get("videos_processed"), f"scored{(' · ' + pct + ' of transcripts') if pct else ''}"),
    ]

    card_html = "\n".join(
        f"""
        <div class="card">
          <div class="card-label">{_esc(label)}</div>
          <div class="card-value">{_fmt(value)}</div>
          <div class="card-sub">{sub}</div>
        </div>"""
        for label, value, sub in cards
    )

    if channels is None:
        rows_html = '<tr><td colspan="5" class="muted">Could not load channels (database unreachable).</td></tr>'
    elif not channels:
        rows_html = '<tr><td colspan="5" class="muted">No channels yet. Add one above.</td></tr>'
    else:
        rows = []
        for c in channels:
            handle = c["handle"] or ""
            handle_disp = f"@{handle}" if handle and not str(handle).startswith("@") else (handle or "—")
            link = (
                f'<a href="https://www.youtube.com/channel/{_esc(c["yt_channel_id"])}" target="_blank" rel="noopener">{_esc(c["yt_channel_id"])}</a>'
                if c["yt_channel_id"]
                else "—"
            )
            added = ""
            if c.get("insert_at"):
                try:
                    added = c["insert_at"].strftime("%Y-%m-%d")
                except Exception:
                    added = _esc(c["insert_at"])
            rows.append(
                f"""<tr>
                  <td>{_esc(c['title']) or '<span class="muted">(no title)</span>'}</td>
                  <td>{_esc(handle_disp)}</td>
                  <td class="num">{_fmt(c['videos'])}</td>
                  <td class="num">{_fmt(c['transcripts'])}</td>
                  <td class="muted">{added}</td>
                </tr>"""
            )
        rows_html = "\n".join(rows)

    banner = ""
    if message:
        cls = "ok" if message_ok else "err"
        banner = f'<div class="banner {cls}">{_esc(message)}</div>'

    generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Transcript Pipeline Dashboard</title>
<style>
  :root {{ --bg:#0f172a; --panel:#1e293b; --panel2:#273449; --text:#e2e8f0;
           --muted:#94a3b8; --accent:#38bdf8; --ok:#22c55e; --err:#ef4444; --border:#334155; }}
  * {{ box-sizing: border-box; }}
  body {{ margin:0; background:var(--bg); color:var(--text);
          font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }}
  .wrap {{ max-width:1000px; margin:0 auto; padding:32px 20px 64px; }}
  header {{ display:flex; align-items:baseline; justify-content:space-between; flex-wrap:wrap; gap:8px; }}
  h1 {{ font-size:22px; margin:0; }}
  h2 {{ font-size:15px; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); margin:36px 0 14px; }}
  .meta {{ color:var(--muted); font-size:13px; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:14px; margin-top:18px; }}
  .card {{ background:var(--panel); border:1px solid var(--border); border-radius:12px; padding:16px 18px; }}
  .card-label {{ font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.05em; }}
  .card-value {{ font-size:30px; font-weight:700; margin:6px 0 2px; }}
  .card-sub {{ font-size:12px; color:var(--muted); }}
  table {{ width:100%; border-collapse:collapse; background:var(--panel); border:1px solid var(--border);
           border-radius:12px; overflow:hidden; }}
  th, td {{ text-align:left; padding:10px 14px; border-bottom:1px solid var(--border); font-size:14px; }}
  th {{ background:var(--panel2); color:var(--muted); text-transform:uppercase; font-size:11px; letter-spacing:.05em; }}
  tr:last-child td {{ border-bottom:none; }}
  td.num, th.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
  .muted {{ color:var(--muted); }}
  a {{ color:var(--accent); text-decoration:none; }}
  a:hover {{ text-decoration:underline; }}
  form.add {{ display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin-top:6px; }}
  input[type=text] {{ flex:1; min-width:220px; padding:10px 12px; border-radius:8px; border:1px solid var(--border);
                      background:var(--panel2); color:var(--text); font-size:14px; }}
  button {{ padding:10px 18px; border:none; border-radius:8px; background:var(--accent); color:#02263a;
            font-weight:600; font-size:14px; cursor:pointer; }}
  button:hover {{ filter:brightness(1.08); }}
  button.secondary {{ background:var(--panel2); color:var(--text); border:1px solid var(--border); }}
  .banner {{ padding:12px 16px; border-radius:8px; margin-top:18px; font-size:14px; }}
  .banner.ok {{ background:rgba(34,197,94,.15); border:1px solid var(--ok); color:#bbf7d0; }}
  .banner.err {{ background:rgba(239,68,68,.15); border:1px solid var(--err); color:#fecaca; }}
  .hint {{ font-size:12px; color:var(--muted); margin-top:8px; }}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Transcript Pipeline Dashboard</h1>
    <span class="meta">Updated {generated} &nbsp;·&nbsp; <a href="/">refresh</a></span>
  </header>

  {banner}

  <div class="cards">
    {card_html}
  </div>

  <h2>Add a channel</h2>
  <form class="add" method="post" action="/add-channel">
    <input type="text" name="handle" placeholder="Channel handle, e.g. @desiringGod" autocomplete="off" autofocus>
    <button type="submit">Add channel</button>
  </form>
  <div class="hint">Enter a YouTube handle. The channel is resolved via the YouTube Data API and queued; its videos are pulled during the next maintenance run.</div>

  <h2>Channels ({len(channels) if channels else 0})</h2>
  <table>
    <thead>
      <tr>
        <th>Channel</th>
        <th>Handle</th>
        <th class="num">Videos</th>
        <th class="num">Transcripts</th>
        <th>Added</th>
      </tr>
    </thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

def make_handler(db_config, api_key_path, logger):

    class DashboardHandler(BaseHTTPRequestHandler):
        server_version = "TranscriptDashboard/1.0"

        # quieter logging via the app logger
        def log_message(self, fmt, *args):
            logger.debug("Dashboard HTTP: " + fmt % args)

        def _send_html(self, body, status=200):
            encoded = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _render(self, message=None, message_ok=True, status=200):
            stats = fetch_stats(db_config, logger)
            channels = fetch_channels(db_config, logger)
            self._send_html(render_page(stats, channels, message, message_ok), status)

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            if path in ("/", "/index.html", "/dashboard"):
                self._render()
            elif path in ("/health", "/healthz"):
                self._send_html("ok")
            else:
                self._send_html("<h1>404 Not Found</h1>", status=404)

        def do_POST(self):
            path = urllib.parse.urlparse(self.path).path
            if path != "/add-channel":
                self._send_html("<h1>404 Not Found</h1>", status=404)
                return
            try:
                length = int(self.headers.get("Content-Length", 0))
            except (TypeError, ValueError):
                length = 0
            raw = self.rfile.read(length).decode("utf-8") if length else ""
            fields = urllib.parse.parse_qs(raw)
            handle = (fields.get("handle", [""])[0] or "").strip()

            api_key = load_api_key(api_key_path)
            ok, msg = add_channel(handle, db_config, api_key, logger)
            self._render(message=msg, message_ok=ok)

    return DashboardHandler


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def start_dashboard_in_thread(db_config, api_key_path=DEFAULT_API_KEY_PATH,
                              logger=None, host="0.0.0.0", port=8080):
    """Start the dashboard HTTP server in a daemon thread. Returns the server."""
    if logger is None:
        logger = logging.getLogger("Dashboard")

    handler = make_handler(db_config, api_key_path, logger)
    httpd = ThreadingHTTPServer((host, port), handler)

    thread = threading.Thread(
        target=httpd.serve_forever, name="dashboard-http", daemon=True
    )
    thread.start()
    logger.info(f"Dashboard web UI started on http://{host}:{port}")
    return httpd


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("Dashboard")
    cfg = load_db_config()
    server = start_dashboard_in_thread(cfg, DEFAULT_API_KEY_PATH, log, "0.0.0.0", 8080)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        server.shutdown()
