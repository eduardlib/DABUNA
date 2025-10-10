
---

**/run.py**
```python
# DABUNA – חדשות + מדד — Audience-Optimized
from __future__ import annotations
import os, re, csv, json, time, html, datetime, hashlib
from urllib.parse import urlparse, parse_qs, urlunparse
from zoneinfo import ZoneInfo
from typing import Any, List, Dict, Tuple, Optional
import requests, yaml, feedparser
from bs4 import BeautifulSoup

# ---------- Utils ----------
def now_il() -> datetime.datetime:
    return datetime.datetime.now(ZoneInfo("Asia/Jerusalem"))

def safe(s: str) -> str:
    return html.escape(s or "", quote=False)

def ensure_dir(p: str):
    if p and p != ".":
        os.makedirs(p, exist_ok=True)

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
def tg_send(token, chat_id, html_text, buttons=None):
    """
    שולח HTML לטלגרם, עם חלוקת הודעות (4096) ו-429 backoff.
    אם DRY_RUN=1 — לא שולח בפועל (רק לוג).
    """
    if os.getenv("DRY_RUN", "0") == "1":
        print("[DRY_RUN] tg_send skipped (len=%d)" % len(html_text or ""))
        return {"ok": True, "dry_run": True}

    CHUNK_LIMIT = 3900  # מרווח בטוח
    base_payload = {
        "chat_id": chat_id,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }

    txt = html_text or ""
    chunks = []
    while len(txt) > CHUNK_LIMIT:
        cut = txt.rfind("\n", 0, CHUNK_LIMIT)
        if cut < CHUNK_LIMIT // 2:
            cut = CHUNK_LIMIT
        chunks.append(txt[:cut])
        txt = txt[cut:]
    chunks.append(txt)

    last_resp = None
    for i, part in enumerate(chunks):
        payload = dict(base_payload)
        payload["text"] = part
        if buttons and i == len(chunks) - 1:
            payload["reply_markup"] = {"inline_keyboard": buttons}
        while True:
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=30,
            )
            if r.status_code == 429:
                try:
                    retry = r.json().get("parameters", {}).get("retry_after", 30)
                except Exception:
                    retry = 30
                time.sleep(int(retry) + 1)
                continue
            if not r.ok:
                raise RuntimeError(f"Telegram API error: {r.status_code} {r.text}")
            last_resp = r.json()
            break
    return last_resp

def tg_photo(token, chat_id, photo_url: str, caption_html: str, buttons=None):
    """
    שולח תמונה + כיתוב (HTML). נופל חזרה להודעת טקסט אם נכשל.
    """
    if os.getenv("DRY_RUN", "0") == "1":
        print(f"[DRY_RUN] tg_photo skipped ({photo_url})")
        return {"ok": True, "dry_run": True}

    payload = {
        "chat_id": chat_id,
        "photo": photo_url,
        "caption": caption_html,
        "parse_mode": "HTML",
    }
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": buttons}

    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendPhoto",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=30,
    )
    if r.status_code == 429:
        try:
            retry = r.json().get("parameters", {}).get("retry_after", 30)
        except Exception:
            retry = 30
        time.sleep(int(retry) + 1)
        return tg_photo(token, chat_id, photo_url, caption_html, buttons)
    if r.ok:
        return r.json()
    # fallback: טקסט בלבד
    return tg_send(token, chat_id, caption_html, buttons)

# ---------- URL normalize & keys ----------
_SKIP_QS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content",
            "utm_name","gclid","fbclid","igshid","mc_cid","mc_eid","ref"}

def normalize_url(u: str) -> str:
    """
    מסיר פרמטרים שיווקיים ומאחד כתובת ל-key יציב למניעת כפילויות.
    """
    try:
        p = urlparse(u)
        q = parse_qs(p.query, keep_blank_values=False)
        q = {k:v for k,v in q.items() if k not in _SKIP_QS}
        query = "&".join(f"{k}={v[0]}" for k,v in sorted(q.items()) if v)
        p2 = p._replace(query=query, fragment="")
        normalized = urlunparse(p2)
        if normalized.endswith("/"):
            normalized = normalized[:-1]
        return normalized
    except Exception:
        return u or ""

def url_key(u: str) -> str:
    try:
        n = normalize_url(u)
        p = urlparse(n)
        base = (p.netloc or "").lower()
        path = (p.path or "").rstrip("/")
        return f"{base}{path}"
    except Exception:
        return u or ""

# ---------- Sources / ingest ----------
def clean_html(html_text: str) -> str:
    soup = BeautifulSoup(html_text or "", "html.parser")
    for t in soup(["script", "style", "noscript"]):
        t.extract()
    txt = soup.get_text(" ", strip=True) or ""
    return " ".join(txt.split())

def fetch_text(url: str, timeout=12) -> str:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "DabunaBot/1.1"})
        if r.ok:
            return r.text
    except Exception:
        pass
    return ""

def og_image_from_html(html_text: str) -> str:
    """
    מנסה לדוג תמונה מ-meta: og:image / twitter:image
    """
    try:
        soup = BeautifulSoup(html_text or "", "html.parser")
        for sel in [
            ('meta', {'property': 'og:image'}),
            ('meta', {'name': 'og:image'}),
            ('meta', {'name': 'twitter:image'}),
            ('meta', {'property': 'twitter:image'}),
        ]:
            tag = soup.find(*sel)
            if tag:
                u = tag.get("content") or ""
                if u.startswith("http"):
                    return u
    except Exception:
        pass
    return ""

def load_sources(whitelist_yaml: str):
    try:
        with open(whitelist_yaml, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {"rss": ["https://www.ynet.co.il/Integration/StoryRss2.xml"],
                "domains_official": ["ynet.co.il"]}

def translate_to_he(cfg, text: str) -> str:
    if not text or is_hebrew(text):
        return text
    tcfg = cfg.get("translate", {})
    if not tcfg.get("enabled", False):
        return text
    try:
        r = requests.post(
            tcfg.get("service_url", "https://translate.astian.org/translate"),
            json={"q": text, "source": "auto", "target": "he", "format": "text"},
            timeout=15,
        )
        if r.ok:
            return r.json().get("translatedText", text)
    except Exception:
        pass
    return text

def ingest_items(cfg) -> list[dict]:
    whitelist_file = (cfg.get("sources") or {}).get("whitelist_file", "data/sources_whitelist.yaml")
    src = load_sources(whitelist_file)
    rss_list = src.get("rss", [])
    items = []
    for feed_url in rss_list:
        try:
            fp = feedparser.parse(feed_url)
            for e in fp.entries[:40]:
                url = e.get("link") or ""
                title = e.get("title") or ""
                summary = clean_html(e.get("summary", ""))
                html_page = fetch_text(url)
                text = clean_html(html_page) if html_page else summary
                image = ""
                if html_page:
                    image = og_image_from_html(html_page)
                items.append({
                    "url": normalize_url(url),
                    "title": title,
                    "summary": summary,
                    "text": text,
                    "image": image,
                    "source": urlparse(url).netloc,
                    "feed": feed_url
                })
        except Exception as ex:
            print("RSS error:", feed_url, ex)
    print(f"[DABUNA] fetched {len(items)} raw items")
    return items

# ---------- Filtering, translation & anti-duplicates ----------
def _tokens(s: str) -> set[str]:
    s = (s or "").lower()
    s = re.sub(r"[^a-z\u0590-\u05FF0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return set(s.split()) if s else set()

def similar(a: str, b: str) -> float:
    A, B = _tokens(a), _tokens(b)
    if not A or not B: return 0.0
    inter = len(A & B); union = len(A | B)
    return inter / max(1, union)

def filter_and_translate(cfg, items: list[dict]) -> list[dict]:
    out = []
    min_title_len = int((cfg.get("filters") or {}).get("min_title_len", 16))
    tcfg = cfg.get("translate", {})
    translate_limit = int(tcfg.get("max_per_run", 0))
    translated_count = 0

    seen_keys = set()
    kept_titles: list[str] = []

    for it in items:
        title = (it.get("title") or "").strip()
        summary = (it.get("summary") or "").strip()
        text = (it.get("text") or "").strip()
        url = it.get("url") or ""

        if len(title) < min_title_len:
            continue

        k = url_key(url)
        if k in seen_keys:
            continue

        is_dup_title = False
        for prev in kept_titles:
            if similar(prev, title) >= 0.80:
                is_dup_title = True
                break
        if is_dup_title:
            continue

        if not (is_hebrew(title) or is_hebrew(summary) or is_hebrew(text)):
            if tcfg.get("enabled", False) and translated_count < translate_limit:
                title = translate_to_he(cfg, title[:240])
                summary = translate_to_he(cfg, summary[:600])
                translated_count += 1

        it["title"] = title
        it["summary"] = summary

        out.append(it)
        seen_keys.add(k)
        kept_titles.append(title)

    print(f"[DABUNA] kept {len(out)} items (translated {translated_count})")
    return out

# ---------- Engagement helpers ----------
def hook_for(title: str, emojis: Dict[str,str]) -> str:
    low = title.lower()
    if any(k in low for k in ["breaking","urgent","בהול","דחוף"]): return emojis.get("breaking","")
    if any(k in low for k in ["update","עדכון"]): return emojis.get("update","")
    if any(k in low for k in ["analysis","פרשנות","ניתוח"]): return emojis.get("analysis","")
    if any(k in low for k in ["econom","inflation","שוק","בורסה","מדד","מחיר"]): return emojis.get("economy","")
    if any(k in low for k in ["security","ירי","תקיפה","חמאס","חזבאללה","חיזבאללה","iran","איראן"]): return emojis.get("security","")
    if any(k in low for k in ["politic","בחירות","ממשלה","כנסת","שרים","נשיא"]): return emojis.get("politics","")
    return emojis.get("default","")

def hashtags_for(text: str, max_tags: int) -> List[str]:
    words = re.findall(r"[A-Za-z\u0590-\u05FF]{3,}", text)
    stop = set("the and for with from this that היו היה כדי אבל לכן כאשר אשר גם מאוד יותר תוך אחרי לפני בגלל אם ללא בין אין עם או הוא היא הם הן על של מה איך למה כמה לכן מי לא כן אנו אנחנו אני אתם אתן אתה את".split())
    uniq = []
    for w in words:
        wl = w.lower()
        if wl in stop:
            continue
        if wl not in uniq:
            uniq.append(wl)
    tags = [f"#{w.capitalize()}" for w in uniq[:max_tags]]
    return tags

def inline_keyboard(url: str) -> list:
    return [[{"text":"פתח כתבה ↗","url":url}]]

# ---------- Post news ----------
def format_caption(it: dict, cfg: dict) -> Tuple[str, Optional[list]]:
    e = cfg.get("engagement") or {}
    brand = (e.get("brand_prefix") or "").strip()
    add_hook = bool(e.get("add_hook", True))
    add_hash = bool(e.get("add_hashtags", True))
    max_tags = int((e.get("hashtag_max", 3)))
    emojis = (e.get("emoji_pack") or {})
    inline_buttons = bool(e.get("inline_buttons", True))

    title = it["title"]
    hook = hook_for(title, emojis) if add_hook else ""
    prefix = " ".join([p for p in [brand, hook] if p]).strip()
    prefix = f"{prefix}: " if prefix else ""

    src = safe(it.get("source",""))
    tags = " ".join(hashtags_for(title, max_tags)) if add_hash else ""

    lines = [f"<b>{safe(prefix + title)}</b>", f"<i>{src}</i>"]
    if it.get("summary"):
        lines.append(safe(it["summary"][:220]))
    if tags:
        lines.append(tags)

    caption = "\n".join(lines).strip()
    kb = inline_keyboard(it["url"]) if (inline_buttons and it.get("url")) else None
    return caption, kb

def post_news_items(cfg, token, items: list[dict]):
    dest = (cfg.get("channels") or {}).get("news", "@DabunaNews")

    storage_dir = cfg.get("storage_dir", "storage")
    posted_path = os.path.join(storage_dir, "posted_urls.json")
    posted = read_json(posted_path, {"keys": []})
    keys_set = set(posted.get("keys", []))

    pub = cfg.get("publish") or {}
    sent = 0
    max_per_run = int(pub.get("max_per_run", 10))
    sleep_s = int(pub.get("sleep_seconds", 30))
    allow_dups = bool(pub.get("allow_duplicates", False))
    per_source_cap = int(pub.get("per_source_cap", 0)) or 0
    per_src_counts: Dict[str,int] = {}

    # איזון לפי מקור (cap)
    balanced: List[dict] = []
    for it in items:
        src = it.get("source","")
        per_src_counts[src] = per_src_counts.get(src, 0) + 1
        if per_source_cap and per_src_counts[src] > per_source_cap:
            continue
        balanced.append(it)
    items = balanced

    for it in items:
        if sent >= max_per_run:
            break
        k = url_key(it["url"])
        if not allow_dups and k in keys_set:
            continue

        caption, kb = format_caption(it, cfg)
        try:
            if (cfg.get("engagement") or {}).get("send_images", True) and it.get("image"):
                tg_photo(token, dest, it["image"], caption, kb)
            else:
                # הוסף גם את הלינק בסוף אם זה טקסט
                msg = caption + (("\n" + safe(it["url"])) if it.get("url") else "")
                tg_send(token, dest, msg, kb)
            sent += 1
            keys_set.add(k)
            posted["keys"] = list(keys_set)
            write_json(posted_path, posted)
            time.sleep(sleep_s)
        except Exception as ex:
            print("post_news_items error:", ex)

    print(f"[DABUNA] posted {sent} news")

# ---------- Index compute ----------
def load_people(csv_path="data/politicians.csv"):
    ppl = []
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                aliases = [a.strip() for a in (row.get("aliases","") or "").split(";") if a.strip()]
                ppl.append({
                    "id": row["id"], "name": row["name"], "party": row["party"],
                    "role": row["role"], "aliases": [row["name"].strip(), *aliases]
                })
    except FileNotFoundError:
        return []
    return ppl

DIGITS = re.compile(r"\d+")
DATES  = re.compile(r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})")

def specificity(text:str) -> float:
    words = max(1, len((text or "").split()))
    nums = len(DIGITS.findall(text or "")) + len(DATES.findall(text or ""))
    return 100.0 * nums / (words/100.0)

def mentions(text:str, person:dict) -> bool:
    t = text or ""
    return any(a and a in t for a in person["aliases"])

def indep_domains(urls):
    return len({urlparse(u).netloc.split(":")[0].lower() for u in urls if u})

def score_transparency(primary_flags):
    if not primary_flags: return 0.0
    return 100.0 * sum(1 for b in primary_flags if b)/len(primary_flags)

def score_fact_integrity(group, avg_spec):
    if not group: return 0.0
    indep = indep_domains([g["url"] for g in group])
    base = 50.0 + min(40.0, (indep-1)*15.0)
    bonus = min(15.0, avg_spec/10.0)
    return min(100.0, base + bonus)

def score_consistency(headlines):
    if not headlines: return 0.0
    toks = [set((h or "").split()) for h in headlines]
    inter = set.intersection(*toks) if len(toks)>=2 else toks[0]
    return max(40.0, min(100.0, 60.0 + len(inter)*10.0))

def compute_rows(items: list[dict]) -> list[dict]:
    people = load_people("data/politicians.csv")
    if not people:
        return []
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
        avg_spec = sum(it["specificity"] for it in group)/max(1,len(group))
        headlines = [g.get("title","").strip() for g in group][:3]
        Consistency = score_consistency(headlines)
        FactIntegrity = score_fact_integrity(group, avg_spec)
        Transparency = score_transparency([g.get("is_primary", False) for g in group])
        Correction = 0.0
        Index = 0.45*Consistency + 0.35*FactIntegrity + 0.10*Transparency + 0.10*Correction
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
        [{"text": "📊 מדד", "url": (cfg.get('web') or {}).get('dashboard_url', '')}],
        [{"text": "🔗 שתפו", "url": (cfg.get('web') or {}).get('share_url', '')}],
    ]
    tg_send(token, dest, "\n".join(lines), buttons)

# ---------- Commands ----------
def cmd_daily(cfg, token):
    items_all = ingest_items(cfg)
    items = filter_and_translate(cfg, items_all)

    storage_dir = cfg.get("storage_dir", "storage")
    ensure_dir(storage_dir)
    write_json(os.path.join(storage_dir, "latest.json"),
               {"date": now_il().isoformat(), "rows": items})

    # פרסום חדשות עם אנגייג'מנט חזקה
    # נפרסם לפי סדר הופעה – אם תרצה דירוג, אפשר להוסיף בהמשך סקורינג לפי מדיניות
    post_news_items(cfg, token, items)

    # מדד (דורש data/politicians.csv)
    rows = compute_rows(items)
    if rows:
        write_json(os.path.join(storage_dir, f"daily_scores_{now_il().date()}.json"), rows)
        write_json(os.path.join(storage_dir, "latest_scores.json"),
                   {"date": now_il().isoformat(), "rows": rows})

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
    hhmm = t.strftime("%H:%M"); weekday = t.weekday()
    if hhmm == "18:00":
        cmd_daily(cfg, token)
    if weekday == 4 and hhmm == "14:00":
        cmd_weekly(cfg, token)
    print(f"[DABUNA] tick {hhmm} – nothing else to do.")

# ---------- main ----------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd", nargs="?", choices=["daily","weekly","tick","miniapp"], default="tick")
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
