# DABUNA NewsBot — Full Auto (DST-safe) + Manual

## מה הבוט עושה
- יומי: אוסף RSS מאושרים, מזהה פוליטיקאים, מחשב מדדי אמינות/עקביות, מפרסם ל-@DabunaNews.
- שבועי (שישי): מסכם את השבוע ומפרסם ל-@DabunaRating.
- שמירת היסטוריה: JSON יומי ב-`storage/`.

## התקנה
1) העלה קבצים לריפו.
2) Settings → Secrets → Actions → **TELEGRAM_BOT_TOKEN**.
3) תן לבוט Admin עם Post Messages ב-@DabunaNews ו-@DabunaRating.
4) Actions → **Smoke test (Telegram)**.

## אוטומציה
- `.github/workflows/tick.yml` מריץ כל שעה; הקוד יפרסם רק ב-18:00 יומי וב-14:00 שישי (Asia/Jerusalem).

## ידני
- **Daily (manual)** ו-**Weekly (manual)** להפעלה ידנית.

## התאמות
- `config.yaml` (ערוצים), `data/sources_whitelist.yaml` (מקורות), `data/politicians.csv` (דמויות).
