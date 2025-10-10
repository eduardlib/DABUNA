# DABUNA — Production Bot
from __future__ import annotations
import os, re, json, time, html, textwrap, hashlib, datetime
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
import requests, yaml, feedparser
from bs4 import BeautifulSoup

# ---------------- Utilities ----------------
def now_il(tz:str) -> datetime.datetime:
    return datetime.datetime.now(ZoneInfo(tz))

def safe(s: str) -> str:
    return html.escape(s or "", quote=False)

def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def looks_clickbait(title: str) -> bool:
    patt = r"(לא תאמינו|הסיבה|כך תעשו|Click here|OMG|Shock|惊|!$|\?$)"
    return bool(re.search(patt, title or "", re.I))

def is_hebrew(text: str) -> bool:
    return bool(re.search(r"[\u0590-\u05FF]", text or ""))

def read_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def load_state(path: str) -> Dict[str, Any]:
    mem: Dict[str, Any] = {}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    mem[obj.get("key")] = obj
                except Exception:
                    continue
    return mem

def save_state(path: str, mem: Dict[str, Any]):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for k, v in mem.items():
            f.write(json.dumps(v, ensure_ascii=False) + "\n")
    os.replace(tmp, path)

def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def http_get(url: str, timeout: int = 20) -> Optional[str]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"DABUNA/1.0"})
        if r.ok:
            return r.text
    except Exception:
        return None
    return None

# ---------------- Telegram ----------------
class Telegram:
    def __init__(self, token: str):
        self.base = f"https://api.telegram.org/bot{token}"

    def send_message(self, chat_id: str, text: str, disable_web_page_preview: bool=False, reply_markup: Optional[dict]=None):
        data = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_web_page_preview,
        }
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
        r = requests.post(self.base+"/sendMessage", data=data, timeout=30)
        r.raise_for_status()
        return r.json()

    def set_commands(self, commands: List[Tuple[str,str]]):
        data = {"commands": json.dumps([{"command":c,"description":d} for c,d in commands], ensure_ascii=False)}
        r = requests.post(self.base+"/setMyCommands", data=data, timeout=30)
        r.raise_for_status()
        return r.json()

    def set_menu_button(self, text: str, url: str):
        # Open link button in the menu (for MiniApp or channel link)
        data = {"menu_button": json.dumps({"type":"web_app","text":text,"web_app":{"url":url}}, ensure_ascii=False)}
        r = requests.post(self.base+"/setChatMenuButton", data=data, timeout=30)
        r.raise_for_status()
        return r.json()

# ---------------- Translation ----------------
def translate_chain(text: str, cfg: Dict[str,Any]) -> str:
    chain = (cfg.get("translate") or {}).get("chain") or []
    tgt = (cfg.get("translate") or {}).get("target_lang") or "he"
    if not chain or not text.strip():
        return text
    for step in chain:
        t = step.get("type")
        try:
            if t == "libretranslate":
                url = step["url"]
                r = requests.post(url, timeout=25, data={
                    "q": text, "source": "auto", "target": tgt, "format": "text"
                })
                if r.ok:
                    js = r.json()
                    if "translatedText" in js:
                        return js["translatedText"]
            elif t == "dummy":
                return text  # no-op
        except Exception:
            continue
    return text

# ---------------- Fetch & Filter ----------------
def fetch_items(cfg: Dict[str,Any]) -> List[Dict[str,Any]]:
    items: List[Dict[str,Any]] = []
    for src in (cfg.get("sources") or []):
        url = src.get("url")
        lang_hint = src.get("lang_hint","auto")
        if not url:
            continue
        fp = feedparser.parse(url)
        for e in fp.entries:
            title = (e.get("title") or "").strip()
            link = e.get("link") or ""
            summary = BeautifulSoup((e.get("summary") or ""), "html.parser").get_text().strip()
            published = e.get("published", "") or e.get("updated", "")
            img = ""
            # Try media:content
            media = e.get("media_content") or e.get("media_thumbnail") or []
            if isinstance(media, list) and media:
                img = media[0].get("url","")
            items.append({
                "source": src.get("name",""),
                "lang_hint": lang_hint,
                "title": title,
                "link": link,
                "summary": summary,
                "published": published,
                "image": img,
            })
    return items

def apply_filters(items: List[Dict[str,Any]], cfg: Dict[str,Any]) -> List[Dict[str,Any]]:
    fcfg = cfg.get("filters") or {}
    min_len = int(fcfg.get("min_title_len", 0))
    require_he = bool(fcfg.get("require_hebrew", False))
    drops = set(fcfg.get("drop_keywords", []))
    out = []
    for it in items:
        t = it["title"]
        if len(t) < min_len:
            continue
        low = t.lower()
        if any(k.lower() in low for k in drops):
            continue
        if require_he and not is_hebrew(t):
            continue
        out.append(it)
    return out

# ---------------- Dedupe ----------------
def dedupe_items(items: List[Dict[str,Any]], cfg: Dict[str,Any]) -> List[Dict[str,Any]]:
    dc = cfg.get("dedupe") or {}
    state_file = dc.get("state_file",".state.jsonl")
    mem = load_state(state_file)
    max_mem = int(dc.get("max_memory", 5000))
    out = []
    for it in items:
        key = sha1((it["title"]+"|"+it["link"]).strip())
        if key in mem:
            continue
        it["key"] = key
        out.append(it)
        mem[key] = {"key": key, "ts": int(time.time()), "title": it["title"], "link": it["link"]}
    # trim memory
    if len(mem) > max_mem:
        # keep newest
        mem_sorted = sorted(mem.values(), key=lambda x: x["ts"], reverse=True)[:max_mem]
        mem = {x["key"]:x for x in mem_sorted}
    save_state(state_file, mem)
    return out

# ---------------- Rating ----------------
def score_item(it: Dict[str,Any], cfg: Dict[str,Any]) -> int:
    rcfg = cfg.get("rating") or {}
    rules = rcfg.get("rules") or {}
    base = int(rules.get("base", 50))
    score = base
    boosts = rules.get("boosts") or {}
    penalties = rules.get("penalties") or {}

    # heuristics
    title = it["title"]
    if len(title) >= 80:
        score += int(boosts.get("long_title", 0))
    if it.get("image"):
        score += int(boosts.get("has_image", 0))
    if is_hebrew(title):
        score += int(boosts.get("hebrew", 0))
    if domain_of(it.get("link","")) in set((cfg.get("rating") or {}).get("trusted_domains", [])):
        score += int(boosts.get("local_source", 0))

    if looks_clickbait(title):
        score -= int(penalties.get("clickbait", 0))
    if len(title) < 32:
        score -= int(penalties.get("short_title", 0))

    return max(0, min(100, score))

# ---------------- Formatters ----------------
def format_news(it: Dict[str,Any], idx: Optional[int]=None, translated: Optional[str]=None) -> str:
    badge = f"#{idx} " if idx is not None else ""
    title = safe(translated or it["title"])
    src = safe(it["source"] or domain_of(it.get("link","")))
    link = it.get("link","")
    lines = [
        f"<b>{badge}{title}</b>",
        f"<i>{src}</i>",
    ]
    if it.get("summary"):
        lines.append(safe(it["summary"][:280]))
    if link:
        lines.append(f"\n{safe(link)}")
    return "\n".join(lines).strip()

def format_rating(it: Dict[str,Any], score: int, translated: Optional[str]=None) -> str:
    title = safe(translated or it["title"])
    link = it.get("link","")
    bar = "█" * (score//5)  # 0..20 blocks
    return f"<b>{title}</b>\nמדד אמינות: <b>{score}/100</b>\n{bar}\n{safe(link)}"

# ---------------- Pipeline ----------------
def prepare(cfg: Dict[str,Any]) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    items = fetch_items(cfg)
    items = apply_filters(items, cfg)
    items = dedupe_items(items, cfg)

    # translate if needed
    if (cfg.get("translate") or {}).get("enabled"):
        tgt = (cfg.get("translate") or {}).get("target_lang","he")
        out = []
        for it in items:
            if tgt == "he" and is_hebrew(it["title"]):
                it["translated"] = it["title"]
            else:
                it["translated"] = translate_chain(it["title"], cfg)
            out.append(it)
        items = out

    # rating/score
    rated = []
    if (cfg.get("rating") or {}).get("enabled"):
        for it in items:
            it["score"] = score_item(it, cfg)
            rated.append(it)
        rated.sort(key=lambda x: x["score"], reverse=True)
    else:
        rated = items[:]

    return items, rated

def publish_items(bot: Telegram, cfg: Dict[str,Any], items: List[Dict[str,Any]]):
    p = cfg.get("publish") or {}
    max_per = int(p.get("max_per_run", 10))
    sleep_s = int(p.get("sleep_seconds", 1))
    link_preview = bool(p.get("link_preview", True))
    ch = (cfg.get("channels") or {}).get("news")

    cnt = 0
    for idx, it in enumerate(items, 1):
        if cnt >= max_per:
            break
        txt = format_news(it, idx=idx, translated=it.get("translated"))
        bot.send_message(ch, txt, disable_web_page_preview=(not link_preview))
        cnt += 1
        time.sleep(sleep_s)

def publish_ratings(bot: Telegram, cfg: Dict[str,Any], rated: List[Dict[str,Any]]):
    rc = (cfg.get("channels") or {}).get("rating")
    p = cfg.get("publish") or {}
    per_ch = (p.get("per_channel") or {}).get("rating", 4)
    sleep_s = int(p.get("sleep_seconds", 1))
    link_preview = bool(p.get("link_preview", True))
    for it in rated[:per_ch]:
        txt = format_rating(it, it.get("score", 0), translated=it.get("translated"))
        bot.send_message(rc, txt, disable_web_page_preview=(not link_preview))
        time.sleep(sleep_s)

# ---------------- Commands ----------------
def cmd_daily(cfg: Dict[str,Any], token: str):
    tz = cfg.get("timezone","Asia/Jerusalem")
    bot = Telegram(token)
    items, rated = prepare(cfg)
    publish_items(bot, cfg, items)
    publish_ratings(bot, cfg, rated)

def cmd_weekly(cfg: Dict[str,Any], token: str):
    # For demo: reuse the 'prepare' result and post a compact top-N
    tz = cfg.get("timezone","Asia/Jerusalem")
    bot = Telegram(token)
    _, rated = prepare(cfg)
    top_n = int((cfg.get("weekly") or {}).get("top_n", 10))
    msg = ["<b>סיכום שבוע – טופ {}</b>".format(top_n)]
    for i, it in enumerate(rated[:top_n], 1):
        msg.append(f"{i}. {safe(it.get('translated') or it['title'])} ({it.get('score',0)}/100)")
    bot.send_message((cfg.get("channels") or {}).get("rating"), "\n".join(msg))

def cmd_tick(cfg: Dict[str,Any], token: str):
    # Lightweight health check—fetch sources and do nothing else
    items = fetch_items(cfg)
    print(f"Tick OK, fetched {len(items)} items.")

def cmd_miniapp(cfg: Dict[str,Any], token: str):
    bot = Telegram(token)
    title = (cfg.get("miniapp") or {}).get("title","DABUNA")
    desc  = (cfg.get("miniapp") or {}).get("description","")
    news_ch = (cfg.get("channels") or {}).get("news")
    rating_ch = (cfg.get("channels") or {}).get("rating")
    # Use menu button to point to news channel (or future webapp)
    bot.set_commands([
        ("start","Start"),
        ("help","How to use DABUNA"),
        ("menu","Open menu"),
        ("ping","Health check"),
        ("summary","Post weekly summary now"),
    ])
    bot.set_menu_button(text=title, url=f"https://t.me/{news_ch.lstrip('@')}")
    # Also post a pinned message to news channel
    text = f"<b>{safe(title)}</b>\n{safe(desc)}\n\n✳️ ערוץ חדשות: {safe(news_ch)}\n📊 ערוץ מדד: {safe(rating_ch)}"
    bot.send_message(news_ch, text, disable_web_page_preview=True)

# ---------------- Entrypoint ----------------
def load_cfg() -> Dict[str,Any]:
    return read_yaml("config.yaml")

def main():
    import sys
    if len(sys.argv) < 2:
        print("Usage: python run.py [daily|weekly|tick|miniapp]")
        raise SystemExit(2)
    cmd = sys.argv[1].strip()
    token = os.environ.get("TELEGRAM_BOT_TOKEN","").strip()
    if not token:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN env")
    cfg = load_cfg()

    if   cmd == "daily":   cmd_daily(cfg, token)
    elif cmd == "weekly":  cmd_weekly(cfg, token)
    elif cmd == "tick":    cmd_tick(cfg, token)
    elif cmd == "miniapp": cmd_miniapp(cfg, token)
    else:
        raise SystemExit(f"Unknown command: {cmd}")

if __name__ == "__main__":
    main()
