# DABUNA — Source Index + Topics Sync (v3)

This edition syncs a **Source Registry** (trust/bias/geo) and **Topic Priorities** (keywords/weights)
into a single scoring pipeline. Items are sorted by audience value:
- Source trust & reach
- Topic weight (security/economy/politics...)
- Freshness
- Engagement signals (image, length)
- Anti-clickbait penalty

Edit:
- `data/sources_registry.yaml` — trust (0..100), reach (0..100), optional boost (-20..+20)
- `data/topics.yaml` — topic weights and keywords
- `data/sources_whitelist.yaml` — RSS feeds
- `data/politicians.csv` — for the rating channel

Run:
```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN=YOUR_TOKEN
python run.py daily
python run.py weekly
python run.py miniapp
```
