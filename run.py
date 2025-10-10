# DABUNA – חדשות + מדד + Miniapp (fixed string literal & robust send)
from __future__ import annotations
import os, re, csv, json, time, html, datetime, random
from urllib.parse import urlparse, parse_qs, urlunparse, quote_plus
from zoneinfo import ZoneInfo
import requests, yaml, feedparser
from bs4 import BeautifulSoup

# ---------- Utils ----------
def now_il() -> datetime.datetime:
    return datetime.datetime.now(ZoneInfo("Asia/Jerusalem"))

def safe(s: str) -> str:
    return html.escape(s or "", quote=False)

def ensure_dir(p: str):
    if p and p != ".": os.makedirs(p, exist_ok=True)

def read_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def write_json(path: str, data):
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_cfg():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def is_hebrew(text: str) -> bool:
    return bool(re.search(r"[\u0590-\u05FF]", text or ""))

# ---------- Telegram ----------
_UA = "DabunaBot/1.1 (+https://t.me/DabunaNews)"

def tg_send(token, chat_id, html_text, buttons=None):
    """
    שולח HTML לטלגרם עם חיתוך להודעות ארוכות ו-429 backoff.
    אם DRY_RUN=1 — מדפיס בלבד.
    """
    if os.getenv("DRY_RUN", "0") == "1":
        print("[DRY_RUN] tg_send len=%d" % len(html_text or ""))
        return {"ok": True, "dry_run": True}

    CHUNK_LIMIT = 3900  # פחות מהגבלת 4096 של טלגרם
    base_payload = {"chat_id": chat_id, "parse_mode": "HTML", "disable_web_page_preview": False}

    txt = html_text or ""
    parts = []
    while len(txt) > CHUNK_LIMIT:
        cut = txt.rfind("\n", 0, CHUNK_LIMIT)
        if cut < CHUNK_LIMIT // 2:
            cut = CHUNK_LIMIT
        parts.append(txt[:cut])
        txt = txt[cut:]
    parts.append(txt)

    last_resp = None
    for i, part in enumerate(parts):
        payload = dict(base_payload)
        payload["text"] = part
        if buttons and i == len(parts) - 1:
            payload["reply_markup"] = {"inline_keyboard": buttons}

        backoff = 5
        while True:
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=30,
            )
            if r.status_code == 429:
                retry = 30
                try:
                    retry = int(r.json().get("parameters", {}).get("retry_after", retry))
                except Exception:
                    pass
                time.sleep(retry + 1)
                backoff = min(backoff * 2, 60)
                continue
            if not r.ok:
                raise RuntimeError(f"Telegram API error: {r.status_code} {r.text}")
            last_resp = r.json()
            break
    return last_resp

# ---------- URL normalize ----------
_SKIP_QS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "utm_name","gclid","fbclid","igshid","mc_cid","mc_eid","ref","yclid","soc_src","soc_trk"
}

def normalize_url(u: str) -> str:
    try:
        p = urlparse(u or "")
        q = parse_qs(p.query, keep_blank_values=False)
        q = {k: v for k, v in q.items() if k not in _SKIP_QS}
        query = "&".join(f"{k}={v[0]}" for k, v in sorted(q.items()) if v)
        p2 = p._replace(query=query, fragment="")
        out = urlunparse(p2)
        return out[:-1] if out.endswith("/") else out
    except Exception:
        return u or ""

def url_key(u: str) -> str:
    try:
        n = normalize_url(u)
        p = urlparse(n)
        return f"{(p.netloc or '').lower()}{(p.path or '').rstrip('/')}"
    except Exception:
        return u or ""

# ---------- HTTP helpers ----------
def http_get(url: str, timeout=12):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": _UA})
        if r.ok:
            return r
    except Exception:
        pass
    return None

def fetch_text(url: str, timeout=12) -> str:
    r = http_get(url, timeout=timeout)
    return r.text if r is not None else ""

# ---------- Sources / ingest ----------
def clean_html(ht: str) -> str:
    soup = BeautifulSoup(ht or "", "html.parser")
    for t in soup(["script", "style", "noscript"]):
        t.extract()
    return " ".join((soup.get_text(" ", strip=True) or "").split())

def load_sources(whitelist_yaml: str):
    try:
        with open(whitelist_yaml, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {"rss": ["https://www.ynet.co.il/Integration/StoryRss2.xml"], "domains_official": ["ynet.co.il"]}

# ---------- Translation (multi-provider fallback) ----------
def translate_chain(cfg) -> list[dict]:
    tcfg = cfg.get("translate", {}) or {}
    providers = tcfg.get("providers") or []
    if not providers:
        providers = [
            {"type": "libretranslate", "url": "https://translate.astian.org/translate"},
            {"type": "mymemory"},
            {"type": "libretranslate", "url": "https://libretranslate.de/translate"},
        ]
    return providers

def _translate_libre(url: str, text: str) -> str | None:
    try:
        r = requests.post(
            url,
            json={"q": text, "source": "auto", "target": "he", "format": "text"},
            timeout=15,
            headers={"User-Agent": _UA},
        )
        if r.ok:
            return r.json().get("translatedText")
    except Exception:
        pass
    return None

def _translate_mymemory(text: str) -> str | None:
    try:
        endpoint = f"https://api.mymemory.translated.net/get?q={quote_plus(text)}&langpair=auto|he"
        r = requests.get(endpoint, timeout=15, headers={"User-Agent": _UA})
        if r.ok:
            data = r.json()
            out = data.get("responseData", {}).get("translatedText")
            if out:
                return out
    except Exception:
        pass
    return None

def translate_to_he(cfg, text: str) -> str:
    if not text or is_hebrew(text):
        return text
    for prov in translate_chain(cfg):
        typ = (prov.get("type") or "").lower()
        if typ == "libretranslate":
            out = _translate_libre(prov.get("url") or "https://translate.astian.org/translate", text)
        elif typ == "mymemory":
            out = _translate_mymemory(text)
        else:
            out = None
        if out and is_hebrew(out):
            return out
        time.sleep(0.3)
    return text

# ---------- Ingest ----------
def ingest_items(cfg) -> list[dict]:
    wl = (cfg.get("sources") or {}).get("whitelist_file", "data/sources_whitelist.yaml")
    src = load_sources(wl)
    rss_list = src.get("rss", [])
    items = []
    for feed_url in rss_list:
        try:
            fp = feedparser.parse(feed_url)
            for e in fp.entries[:50]:
                url = normalize_url(e.get("link") or "")
                title = e.get("title") or ""
                summary = clean_html(e.get("summary", ""))
                html_page = fetch_text(url)
                text = clean_html(html_page) if html_page else summary
                items.append({
                    "url": url, "title": title, "summary": summary, "text": text,
                    "source": urlparse(url).netloc, "feed": feed_url
                })
        except Exception as ex:
            print("RSS error:", feed_url, ex)
    print(f"[DABUNA] fetched {len(items)} raw items from {len(rss_list)} feeds")
    return items

# ---------- Filtering / translate / anti-duplicates ----------
import re as _re
def _tokens(s: str) -> set[str]:
    s = (s or "").lower()
    s = _re.sub(r"[^a-z\u0590-\u05FF0-9 ]+", " ", s)
    s = _re.sub(r"\s+", " ", s).strip()
    return set(s.split()) if s else set()

def similar(a: str, b: str) -> float:
    A, B = _tokens(a), _tokens(b)
    if not A or not B:
        return 0.0
    inter = len(A & B)
    union = len(A | B)
    return inter / max(1, union)

def filter_and_translate(cfg, items: list[dict]) -> list[dict]:
    out = []
    min_title_len = int((cfg.get("filters") or {}).get("min_title_len", 16))
    tcfg = cfg.get("translate", {})
    translate_limit = int(tcfg.get("max_per_run", 12))
    translated = 0
    seen = set()
    kept_titles = []
    require_hebrew = bool((cfg.get("filters") or {}).get("require_hebrew", False))

    for it in items:
        title = (it.get("title") or "").strip()
        summary = (it.get("summary") or "").strip()
        text = (it.get("text") or "").strip()
        url = it.get("url") or ""

        if len(title) < min_title_len:
            continue

        k = url_key(url)
        if k in seen:
            continue

        # near-duplicate by title tokens
        if any(similar(prev, title) >= 0.77 for prev in kept_titles):
            continue

        # translate if needed
        need_he = (not is_hebrew(title) and not is_hebrew(summary) and not is_hebrew(text))
        if need_he:
            if translated < translate_limit and tcfg.get("enabled", True):
                title = translate_to_he(cfg, title[:240])
                summary = translate_to_he(cfg, summary[:600])
                translated += 1
            elif require_hebrew:
                continue

        it["title"] = title
        it["summary"] = summary
        out.append(it)
        seen.add(k)
        kept_titles.append(title)

    print(f"[DABUNA] kept {len(out)} items (translated {translated})")
    return out

# ---------- Post news ----------
def post_news_items(cfg, token, items: list[dict]):
    dest = (cfg.get("channels") or {}).get("news", "@DabunaNews")
    web = cfg.get("web") or {}

    storage_dir = cfg.get("storage_dir", "storage")
    ensure_dir(storage_dir)
    posted_path = os.path.join(storage_dir, "posted_urls.json")
    posted = read_json(posted_path, {"keys": []})
    keys = set(posted.get("keys", []))

    pub = cfg.get("publish") or {}
    sent = 0
    max_per_run = int(pub.get("max_per_run", 12))
    sleep_s = int(pub.get("sleep_seconds", 25))
    allow_dups = bool(pub.get("allow_duplicates", False))

    for it in items:
        if sent >= max_per_run:
            break
        k = url_key(it["url"])
        if not allow_dups and k in keys:
            continue

        title = it["title"] or ""
        summary = (it["summary"] or it["text"] or "")[:220]
        source = it["source"]

        # ***** FIX: use triple-quoted f-string to avoid unterminated-string issues *****
        msg = f"""🗞️ <b>{safe(title)}</b>
TL;DR: {safe(summary)}

מקור: {safe(source)}
🔗 {it['url']}
#דבונה #חדשות #ישראל #כנסת"""

        buttons = [
            [{"text": "📊 מדד", "url": web.get("dashboard_url", "")}],
            [{"text": "🔗 שתפו", "url": web.get("share_url", "")}],
        ]
        try:
            tg_send(token, dest, msg, buttons)
            sent += 1
            keys.add(k)
            posted["keys"] = list(keys)
            write_json(posted_path, posted)
            time.sleep(sleep_s)
        except Exception as ex:
            print("post_news_items error:", ex)

    print(f"[DABUNA] posted {sent} news")

# ---------- Index compute ----------
DIGITS = _re.compile(r"\d+")
DATES  = _re.compile(r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})")

def load_people(csv_path="data/politicians.csv"):
    ppl = []
    with open(csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            aliases = [a.strip() for a in (row.get("aliases", "") or "").split(";") if a.strip()]
            aliases = [row.get("name", "").strip(), *aliases]
            ppl.append({
                "id": row["id"], "name": row["name"], "party": row["party"],
                "role": row["role"], "aliases": aliases
            })
    return ppl

def specificity(text:str) -> float:
    words = max(1, len((text or "").split()))
    nums = len(DIGITS.findall(text or "")) + len(DATES.findall(text or ""))
    return 100.0 * nums / (words/100.0)

def mentions(text:str, person:dict) -> bool:
    t = text or ""
    return any(a and a in t for a in person["aliases"])

def indep_domains(urls):
    return len({urlparse(u).netloc.split(":")[0].lower() for u in urls if u})

def score_transparency(flags):
    if not flags: return 0.0
    return 100.0 * sum(1 for b in flags if b) / len(flags)

def score_fact_integrity(group, avg_spec):
    if not group: return 0.0
    indep = indep_domains([g["url"] for g in group])
    base = 50.0 + min(40.0, (indep-1) * 15.0)
    bonus = min(15.0, avg_spec / 10.0)
    return min(100.0, base + bonus)

def score_consistency(headlines):
    if not headlines: return 0.0
    toks = [set((h or "").split()) for h in headlines]
    inter = set.intersection(*toks) if len(toks) >= 2 else toks[0]
    return max(40.0, min(100.0, 60.0 + len(inter) * 10.0))

def compute_rows(items: list[dict]) -> list[dict]:
    people = load_people("data/politicians.csv")
    for it in items:
        it["specificity"] = specificity(it.get("text") or it.get("summary") or "")
        it["is_primary"] = True

    per = {}
    for it in items:
        txt = (it.get("title") or "") + " " + (it.get("summary") or "")
        for p in people:
            if mentions(txt, p):
                per.setdefault(p["id"], {"person": p, "items": []})
                per[p["id"]]["items"].append(it)

    rows = []
    for pid, data in per.items():
        p = data["person"]; group = data["items"]
        avg_spec = sum(it["specificity"] for it in group) / max(1, len(group))
        headlines = [g.get("title", "").strip() for g in group][:5]
        Consistency = score_consistency(headlines)
        FactIntegrity = score_fact_integrity(group, avg_spec)
        Transparency = score_transparency([g.get("is_primary", False) for g in group])
        Correction = 0.0
        Index = 0.45 * Consistency + 0.35 * FactIntegrity + 0.10 * Transparency + 0.10 * Correction
        rows.append({
            "id": pid, "name": p["name"], "party": p["party"], "role": p["role"],
            "Consistency": Consistency, "FactIntegrity": FactIntegrity,
            "Transparency": Transparency, "CorrectionResponsiveness": Correction,
            "IndexScore": Index, "headlines": headlines
        })
    rows.sort(key=lambda r: r["IndexScore"], reverse=True)
    return rows

def post_daily_index(cfg, token, rows: list[dict]):
    if not rows: return
    dest = (cfg.get("channels") or {}).get("rating", "@DabunaRating")
    d = now_il().strftime("%d.%m.%Y")
    lines = [f"📊 <b>מדד אמינות/עקביות – {d}</b>"]
    for i, r in enumerate(rows[:10], 1):
        lines.append(
            f"{i}) <b>{safe(r['name'])}</b> — Index {r['IndexScore']:.0f}/100 "
            f"(עק׳ {r['Consistency']:.0f} | אמ׳ {r['FactIntegrity']:.0f} | שק׳ {r['Transparency']:.0f})"
        )
    lines.append("#דבונה #מדד_דבונה #FactCheck #ישראל #כנסת")
    buttons = [
        [{"text": "📊 מדד", "url": (cfg.get("web") or {}).get("dashboard_url", "")}],
        [{"text": "🔗 שתפו", "url": (cfg.get("web") or {}).get("share_url", "")}],
    ]
    tg_send(token, dest, "\n".join(lines), buttons)

# ---------- Commands ----------
def cmd_daily(cfg, token):
    items_all = ingest_items(cfg)
    items = filter_and_translate(cfg, items_all)

    storage_dir = cfg.get("storage_dir", "storage")
    ensure_dir(storage_dir)
    write_json(os.path.join(storage_dir, "latest.json"), {"date": now_il().isoformat(), "rows": items})

    post_news_items(cfg, token, items)

    rows = compute_rows(items)
    write_json(os.path.join(storage_dir, f"daily_scores_{now_il().date()}.json"), rows)
    write_json(os.path.join(storage_dir, "latest_scores.json"), {"date": now_il().isoformat(), "rows": rows})
    print("[DABUNA] daily finished.")

def cmd_weekly(cfg, token):
    storage_dir = cfg.get("storage_dir", "storage")
    latest = read_json(os.path.join(storage_dir, "latest_scores.json"), {})
    rows = latest.get("rows") or []
    if not rows:
        items_all = ingest_items(cfg)
        items = filter_and_translate(cfg, items_all)
        rows = compute_rows(items)
    post_daily_index(cfg, token, rows)
    print("[DABUNA] weekly index posted.")

def cmd_miniapp(cfg, token):
    web = cfg.get("web") or {}
    dest = (cfg.get("channels") or {}).get("news", "@DabunaNews")
    msg = "🧩 <b>Dabuna Mini-App</b>\nעודכן בהצלחה. פתחו את המדד והדשבורד:"
    buttons = [
        [{"text": "📊 מדד", "url": web.get("dashboard_url", "")}],
        [{"text": "🔗 שתפו", "url": web.get("share_url", "")}],
    ]
    tg_send(token, dest, msg, buttons)
    print("[DABUNA] miniapp posted.")

def cmd_tick(cfg, token):
    t = now_il()
    hhmm = t.strftime("%H:%M")
    weekday = t.weekday()  # Monday=0 ... Sunday=6
    if hhmm == "18:00":
        cmd_daily(cfg, token)
    if weekday == 4 and hhmm == "14:00":
        cmd_weekly(cfg, token)
    print(f"[DABUNA] tick {hhmm} – nothing else to do.")

# ---------- main ----------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd", nargs="?", choices=["daily", "weekly", "tick", "miniapp"], default="tick")
    args = parser.parse_args()

    cfg = load_cfg()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN env")

    if args.cmd == "daily":
        cmd_daily(cfg, token)
    elif args.cmd == "weekly":
        cmd_weekly(cfg, token)
    elif args.cmd == "miniapp":
        cmd_miniapp(cfg, token)
    else:
        cmd_tick(cfg, token)
