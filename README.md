# DABUNA — Audience-Optimized

- יומי: פוסטים עם **תמונה+כיתוב**, הוק/אימוג’י, האשטאגים נקיים, כפתור “פתח כתבה ↗” לכל פריט.
- שבועי: סיכום “מדד” ל-@DabunaRating.
- דה-דופליקציה: נרמול כתובות + דמיון כותרות.
- תרגום: LibreTranslate (מוגבל ל־max_per_run כדי לא להציף).
- שעות פרסום: דרך `tick` (כל שעה), הקוד יורה ב־18:00 יומי וב־14:00 שישי (Asia/Jerusalem).

## התקנה מהירה
```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN=YOUR_TOKEN
python run.py tick      # יופעל אחת לשעה; בשעות היעד יפרסם
python run.py daily     # ידני
python run.py weekly    # ידני
python run.py miniapp   # הודעת miniapp/תפריט
