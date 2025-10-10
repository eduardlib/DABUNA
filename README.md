# DABUNA — News + Credibility Index Bot

Production-ready Telegram bot that fetches news from RSS sources, filters and (optionally) translates,
ranks items by a naive credibility **Index**, and publishes to channels:
- News: `@DabunaNews`
- Rating: `@DabunaRating`

## Quick Start (Local)
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN=YOUR_TOKEN
python run.py daily
python run.py weekly
python run.py miniapp
```

## Configuration
Edit `config.yaml`:
- Add RSS feeds under `sources:`
- Tune `filters`, `translate`, `rating`, `publish`

## GitHub Actions
Workflows in `.github/workflows`:
- `daily.yml` — every day 18:00 Asia/Jerusalem
- `weekly.yml` — Fridays 14:00 Asia/Jerusalem
- `tick.yml` — hourly health checks
- `miniapp.yml` — daily refresh miniapp
- `*_manual.yml` — manual triggers
Set repository secret: `TELEGRAM_BOT_TOKEN`.
