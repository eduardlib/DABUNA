# DABUNA v3 — Source Index + Topics Sync
from __future__ import annotations
import os, re, csv, json, time, html, datetime, hashlib, math
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlunparse
from zoneinfo import ZoneInfo
import requests, yaml, feedparser
from bs4 import BeautifulSoup

# ---------- Utils ----------
def now_il(tz="Asia/Jerusalem") -> datetime.datetime:
    return datetime.datetime.now(ZoneInfo(tz))

def safe(s: str) -> str:
    return html.escape(s or "", quote=False)

def ensure_dir(path: str):
    if path and path != ".":
        os.makedirs(path, exist_ok=True)

def read_yaml(path: str) -> Dict[str,Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

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

def is_hebrew(text: str) -> bool:
    return bool(re.search(r"[\u0590-\u05FF]", text or ""))

# URL normalize/key
_SKIP_QS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","utm_name","gclid","fbclid","igshid","mc_cid","mc_eid","ref"}
def normalize_url(u: str) -> str:
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
        return f"{(p.netloc or '').lower()}{(p.path or '').rstrip('/')}"
    except Exception:
        return u or ""

# ---------- Telegram ----------
def tg_send(token, chat_id, html_text, buttons=None, preview=True):
    if os.getenv("DRY_RUN","0") == "1":
        print("[DRY_RUN] tg_send", len(html_text or ""))
        return {"ok":True}
    payload = {
        "chat_id": chat_id, "text": html_text, "parse_mode": "HTML",
        "disable_web_page_preview": not preview
    }
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": buttons}
    r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      headers={"Content-Type":"application/json"},
                      data=json.dumps(payload), timeout=30)
    if r.status_code == 429:
        try:
            retry = r.json().get("parameters",{}).get("retry_after", 30)
        except Exception:
            retry = 30
        time.sleep(int(retry)+1)
        return tg_send(token, chat_id, html_text, buttons, preview)
    r.raise_for_status()
    return r.json()

def tg_photo(token, chat_id, photo_url: str, caption_html: str, buttons=None):
    if os.getenv("DRY_RUN","0") == "1":
        print("[DRY_RUN] tg_photo", photo_url)
        return {"ok":True}
    payload = {"chat_id": chat_id, "photo": photo_url, "caption": caption_html, "parse_mode":"HTML"}
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": buttons}
    r = requests.post(f"https://api.telegram.org/bot{token}/sendPhoto",
                      headers={"Content-Type":"application/json"},
                      data=json.dumps(payload), timeout=30)
    if r.status_code == 429:
        try:
            retry = r.json().get("parameters",{}).get("retry_after", 30)
        except Exception:
            retry = 30
        time.sleep(int(retry)+1)
        return tg_photo(token, chat_id, photo_url, caption_html, buttons)
    if r.ok: return r.json()
    return tg_send(token, chat_id, caption_html, buttons)

# ---------- Content helpers ----------
def clean_html(html_text: str) -> str:
    soup = BeautifulSoup(html_text or "", "html.parser")
    for t in soup(["script","style","noscript"]): t.extract()
    txt = soup.get_text(" ", strip=True) or ""
    return " ".join(txt.split())

def fetch_html(url: str, timeout=12) -> str:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Dabuna/3.0"})
        if r.ok: return r.text
    except Exception:
        pass
    return ""

def og_image(html_text: str) -> str:
    try:
        soup = BeautifulSoup(html_text or "", "html.parser")
        for sel in [('meta',{'property':'og:image'}), ('meta',{'name':'og:image'}),
                    ('meta',{'name':'twitter:image'}), ('meta',{'property':'twitter:image'})]:
            tag = soup.find(*sel)
            if tag:
                u = tag.get("content") or ""
                if u.startswith("http"): return u
    except Exception:
        pass
    return ""

def looks_clickbait(title: str) -> bool:
    patt = r"(לא תאמינו|כך תעשו|Click here|OMG|Shock|惊|!$|\?$)"
    return bool(re.search(patt, title or "", re.I))

# ---------- Load configs ----------
def load_cfg() -> Dict[str,Any]:
    return read_yaml("config.yaml")

def load_sources_regs(path: str) -> Dict[str,Any]:
    y = read_yaml(path)
    return y.get("sources",{})

def load_topics(path: str) -> Dict[str,Any]:
    y = read_yaml(path)
    return y.get("topics",{})

# ---------- Ingest ----------
def ingest(cfg) -> List[Dict[str,Any]]:
    src_list = read_yaml((cfg.get("sources") or {}).get("whitelist_file","data/sources_whitelist.yaml")).get("rss",[])
    out = []
    for feed_url in src_list:
        try:
            fp = feedparser.parse(feed_url)
            for e in fp.entries[:40]:
                url = normalize_url(e.get("link") or "")
                title = (e.get("title") or "").strip()
                summary = clean_html(e.get("summary",""))
                published = e.get("published_parsed") or e.get("updated_parsed")
                ts = None
                if published:
                    try:
                        ts = datetime.datetime.fromtimestamp(time.mktime(published), tz=ZoneInfo(cfg.get("timezone","Asia/Jerusalem")))
                    except Exception:
                        ts = now_il(cfg.get("timezone","Asia/Jerusalem"))
                else:
                    ts = now_il(cfg.get("timezone","Asia/Jerusalem"))
                domain = urlparse(url).netloc
                html_page = fetch_html(url)
                text = clean_html(html_page) if html_page else summary
                image = og_image(html_page) if html_page else ""
                out.append({
                    "url": url, "title": title, "summary": summary, "text": text,
                    "domain": domain, "published_ts": ts.isoformat(), "image": image, "feed": feed_url
                })
        except Exception as ex:
            print("RSS error:", feed_url, ex)
    return out

# ---------- Filter + translate + dedupe ----------
def _tokens(s: str) -> set:
    s = (s or "").lower()
    s = re.sub(r"[^a-z\u0590-\u05FF0-9 ]+"," ",s)
    s = re.sub(r"\s+"," ",s).strip()
    return set(s.split()) if s else set()

def similar(a: str, b: str) -> float:
    A, B = _tokens(a), _tokens(b)
    if not A or not B: return 0.0
    inter = len(A & B); union = len(A | B)
    return inter/max(1,union)

def translate(cfg, text: str) -> str:
    if not text or is_hebrew(text): return text
    t = cfg.get("translate",{})
    if not t.get("enabled",False): return text
    try:
        r = requests.post(t.get("service_url","https://translate.astian.org/translate"),
                          json={"q":text, "source":"auto","target":"he","format":"text"}, timeout=15)
        if r.ok:
            return r.json().get("translatedText", text)
    except Exception:
        pass
    return text

def preprocess(cfg, items: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    f = cfg.get("filters",{})
    min_len = int(f.get("min_title_len",16))
    drops = {d.lower() for d in f.get("drop_keywords",[])}

    seen = set()
    titles = []
    kept = []
    tcfg = cfg.get("translate",{}); lim = int(tcfg.get("max_per_run",0)); cnt=0
    for it in items:
        title = (it["title"] or "").strip()
        if len(title) < min_len: continue
        low = title.lower()
        if any(d in low for d in drops): continue
        k = url_key(it["url"])
        if k in seen: continue
        # similar title
        if any(similar(title, t)>=0.80 for t in titles): continue
        # translation
        if not is_hebrew(title) and cnt<lim and tcfg.get("enabled",False):
            title = translate(cfg, title[:240]); cnt+=1
        it["title"] = title
        kept.append(it); seen.add(k); titles.append(title)
    return kept

# ---------- Scoring ----------
def topic_weight(topics: Dict[str,Any], text: str) -> int:
    tx = (text or "").lower()
    best = 0
    for name, t in topics.items():
        w = int(t.get("weight",50))
        kws = [k.lower() for k in (t.get("keywords") or [])]
        if any(k in tx for k in kws):
            best = max(best, w)
    return best

def source_score(regs: Dict[str,Any], domain: str) -> int:
    r = regs.get(domain) or regs.get(domain.lower()) or {}
    trust = int(r.get("trust", 50))
    reach = int(r.get("reach", 50))
    boost = int(r.get("boost", 0))
    return max(0, min(100, round(0.6*trust + 0.3*reach + boost)))

def freshness_score(published_iso: str, tz: str) -> int:
    try:
        ts = datetime.datetime.fromisoformat(published_iso)
    except Exception:
        ts = now_il(tz)
    delta = now_il(tz) - ts
    hours = max(0.1, delta.total_seconds()/3600.0)
    # 0h -> 100, 24h -> ~60, 72h -> ~40
    sc = 100 * (1/(1+0.05*hours*hours**0.25))
    return int(max(20, min(100, sc)))

def engagement_score(it: Dict[str,Any]) -> int:
    s = 50
    if it.get("image"): s += 10
    if len(it.get("title","")) >= 80: s += 6
    if looks_clickbait(it.get("title","")): s -= 12
    return max(0, min(100, s))

def total_score(cfg, regs, topics, it):
    tz = cfg.get("timezone","Asia/Jerusalem")
    S = source_score(regs, it["domain"])
    T = topic_weight(topics, it["title"]+" "+it.get("summary",""))
    F = freshness_score(it["published_ts"], tz)
    E = engagement_score(it)
    # weights tuned for “audience today”
    total = 0.35*S + 0.30*T + 0.20*F + 0.15*E
    return int(round(total))

# ---------- Posting ----------
def hook_for(title: str, emojis: Dict[str,str]) -> str:
    low = title.lower()
    if any(k in low for k in ["breaking","urgent","בהול","דחוף"]): return emojis.get("breaking","")
    if any(k in low for k in ["update","עדכון"]): return emojis.get("update","")
    if any(k in low for k in ["analysis","פרשנות","ניתוח"]): return emojis.get("analysis","")
    if any(k in low for k in ["econom","inflation","שוק","בורסה","מדד","מחיר"]): return emojis.get("economy","")
    if any(k in low for k in ["security","ירי","תקיפה","חמאס","חיזבאללה","חזבאללה","iran","איראן"]): return emojis.get("security","")
    if any(k in low for k in ["politic","בחירות","ממשלה","כנסת","שרים","נשיא"]): return emojis.get("politics","")
    return emojis.get("default","")

def hashtags_for(text: str, max_tags: int) -> List[str]:
    words = re.findall(r"[A-Za-z\u0590-\u05FF]{3,}", text)
    stop = set("the and for with from this that היו היה כדי אבל לכן כאשר אשר גם מאוד יותר תוך אחרי לפני בגלל אם ללא בין אין עם או הוא היא הם הן על של מה איך למה כמה לכן מי לא כן אנו אנחנו אני אתם אתן אתה את".split())
    uniq = []
    for w in words:
        wl = w.lower()
        if wl in stop: continue
        if wl not in uniq: uniq.append(wl)
    return [f"#{w.capitalize()}" for w in uniq[:max_tags]]

def inline_keyboard(url: str) -> list:
    return [[{"text":"פתח כתבה ↗","url":url}]]

def caption_for(cfg, it) -> Tuple[str, Optional[list]]:
    e = cfg.get("engagement") or {}
    brand = (e.get("brand_prefix") or "").strip()
    emojis = (e.get("emoji_pack") or {})
    add_hook = bool(e.get("add_hook", True))
    add_hash = bool(e.get("add_hashtags", True))
    max_tags = int(e.get("hashtag_max", 3))

    title = it["title"]
    hook = hook_for(title, emojis) if add_hook else ""
    prefix = " ".join([p for p in [brand, hook] if p]).strip()
    prefix = f"{prefix}: " if prefix else ""

    src = safe(it.get("domain",""))
    tags = " ".join(hashtags_for(title, max_tags)) if add_hash else ""

    lines = [f"<b>{safe(prefix + title)}</b>", f"<i>{src}</i>"]
    if it.get("summary"):
        lines.append(safe(it["summary"][:220]))
    if tags: lines.append(tags)
    return "\n".join(lines).strip(), inline_keyboard(it["url"])

def publish_news(cfg, token, items):
    ch = (cfg.get("channels") or {}).get("news", "@DabunaNews")
    p = cfg.get("publish") or {}
    max_per = int(p.get("max_per_run", 12))
    sleep_s = int(p.get("sleep_seconds", 2))
    send_images = bool((cfg.get("engagement") or {}).get("send_images", True))

    sent = 0
    for it in items[:max_per]:
        caption, kb = caption_for(cfg, it)
        try:
            if send_images and it.get("image"):
                tg_photo(token, ch, it["image"], caption, kb)
            else:
                tg_send(token, ch, caption+"\n"+safe(it["url"]), kb)
            sent += 1
            time.sleep(sleep_s)
        except Exception as ex:
            print("publish error:", ex)
    print("[DABUNA] posted", sent, "items")

# ---------- Index (people) ----------
def load_people(csv_path="data/politicians.csv"):
    ppl = []
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                aliases = [a.strip() for a in (row.get("aliases","") or "").split(";") if a.strip()]
                ppl.append({"id": row["id"], "name": row["name"], "party": row["party"], "role": row["role"], "aliases":[row["name"].strip(), *aliases]})
    except FileNotFoundError:
        pass
    return ppl

def mentions(text:str, person:dict) -> bool:
    t = text or ""
    return any(a and a in t for a in person["aliases"])

def compute_people_index(items):
    people = load_people()
    if not people: return []
    per = {}
    for it in items:
        txt = (it.get("title","")+" "+it.get("summary","")).strip()
        for p in people:
            if mentions(txt, p):
                per.setdefault(p["id"], {"person":p, "items":[]})
                per[p["id"]]["items"].append(it)
    rows = []
    for pid, data in per.items():
        p = data["person"]; group = data["items"]
        unique_domains = len({g["domain"] for g in group})
        # naive: more independent coverage → higher score
        score = min(100, 50 + unique_domains*10)
        rows.append({"id":pid,"name":p["name"],"party":p["party"],"role":p["role"],"IndexScore":score,"count":len(group)})
    rows.sort(key=lambda r: r["IndexScore"], reverse=True)
    return rows

def post_people_index(cfg, token, rows):
    if not rows: return
    ch = (cfg.get("channels") or {}).get("rating", "@DabunaRating")
    d = now_il().strftime("%d.%m.%Y")
    lines = [f"📊 <b>מדד דמויות – {d}</b>"]
    for i, r in enumerate(rows[:10], 1):
        lines.append(f"{i}) <b>{safe(r['name'])}</b> — {r['IndexScore']}/100 (מקורות עצמאיים: ~{r['count']})")
    tg_send(token, ch, "\n".join(lines))

# ---------- Source Index (weekly) ----------
def compute_source_index(items, regs):
    agg = {}
    for it in items:
        d = it["domain"]
        base = source_score(regs, d)
        agg.setdefault(d, {"domain":d, "base": base, "n":0})
        agg[d]["n"] += 1
    rows = []
    for d, rec in agg.items():
        # activity-weighted source score
        idx = min(100, int(round(0.8*rec["base"] + 0.2*min(100, rec["n"]*5))))
        rows.append({"domain": d, "IndexScore": idx, "posts": rec["n"]})
    rows.sort(key=lambda r: r["IndexScore"], reverse=True)
    return rows

def post_source_index(cfg, token, rows):
    if not rows: return
    ch = (cfg.get("channels") or {}).get("rating", "@DabunaRating")
    lines = ["🏷️ <b>מדד מקורות — השבוע</b>"]
    for i, r in enumerate(rows[:10], 1):
        lines.append(f"{i}) <code>{safe(r['domain'])}</code> — {r['IndexScore']}/100 (פרסומים: {r['posts']})")
    tg_send(token, ch, "\n".join(lines))

# ---------- Commands ----------
def cmd_daily(cfg, token):
    tz = cfg.get("timezone","Asia/Jerusalem")
    regs = load_sources_regs((cfg.get("sources") or {}).get("registry_file","data/sources_registry.yaml"))
    topics = load_topics((cfg.get("topics") or {}).get("file","data/topics.yaml"))
    raw = ingest(cfg)
    items = preprocess(cfg, raw)

    # scoring
    for it in items:
        it["score"] = total_score(cfg, regs, topics, it)
    # per-source cap
    cap = int((cfg.get("publish") or {}).get("per_source_cap", 0)) or 0
    counts = {}
    ranked = []
    for it in sorted(items, key=lambda x: x["score"], reverse=True):
        d = it["domain"]
        counts[d] = counts.get(d, 0) + 1
        if cap and counts[d] > cap:
            continue
        ranked.append(it)

    publish_news(cfg, token, ranked)
    ppl = compute_people_index(ranked)
    post_people_index(cfg, token, ppl)

    # persist for weekly
    storage = cfg.get("storage_dir","storage"); ensure_dir(storage)
    write_json(os.path.join(storage,"last_items.json"), ranked)

def cmd_weekly(cfg, token):
    storage = cfg.get("storage_dir","storage")
    ranked = read_json(os.path.join(storage,"last_items.json"), [])
    regs = load_sources_regs((cfg.get("sources") or {}).get("registry_file","data/sources_registry.yaml"))
    rows = compute_source_index(ranked, regs)
    post_source_index(cfg, token, rows)

def cmd_miniapp(cfg, token):
    ch = (cfg.get("channels") or {}).get("news","@DabunaNews")
    tg_send(token, ch, "🧩 DABUNA MiniApp: עודכן.")

def cmd_tick(cfg, token):
    # keep simple: only log
    print("[DABUNA] tick OK.")

# ---------- Main ----------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd", nargs="?", choices=["daily","weekly","tick","miniapp"], default="tick")
    args = parser.parse_args()

    cfg = load_cfg()
    token = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    if not token:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN env")
    if args.cmd == "daily":   cmd_daily(cfg, token)
    elif args.cmd == "weekly": cmd_weekly(cfg, token)
    elif args.cmd == "miniapp": cmd_miniapp(cfg, token)
    else: cmd_tick(cfg, token)
