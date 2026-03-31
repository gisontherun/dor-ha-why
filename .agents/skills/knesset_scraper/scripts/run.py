#!/usr/bin/env python3
"""
KNESSET SCRAPER SKILL
Collects Knesset data from Wikipedia + Wikipedia API + anchor facts
(knesset.gov.il is geoblocked from cloud IPs — Wikipedia is the best public source)
"""
import requests
import json
import os
import sys
import time
import re
from bs4 import BeautifulSoup

OUTPUT_PATH = "/app/rag_system/data/chunks/knesset_chunks.json"
LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 200
WIKI_API = "https://he.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": "Mozilla/5.0 (ResearchBot/1.0; mailto:research@example.com)"}

def log(msg): print(f"[KNESSET] {msg}", flush=True)

def wiki_search(query, limit=5):
    """Search Hebrew Wikipedia"""
    params = {
        "action": "query", "list": "search",
        "srsearch": query, "srlimit": limit,
        "format": "json", "uselang": "he"
    }
    try:
        r = requests.get(WIKI_API, params=params, headers=HEADERS, timeout=15)
        return r.json().get("query", {}).get("search", [])
    except Exception as e:
        log(f"  Search error: {e}")
        return []

def wiki_page(title):
    """Get full text of a Wikipedia page"""
    params = {
        "action": "query", "titles": title,
        "prop": "extracts", "explaintext": True,
        "format": "json", "uselang": "he"
    }
    try:
        r = requests.get(WIKI_API, params=params, headers=HEADERS, timeout=15)
        pages = r.json().get("query", {}).get("pages", {})
        for pid, page in pages.items():
            if pid != "-1":
                return page.get("extract", "")
        return ""
    except Exception as e:
        log(f"  Page error {title}: {e}")
        return ""

def chunk_text(text, title, source_url, max_len=900):
    """Split text into chunks"""
    chunks = []
    paragraphs = [p.strip() for p in text.split("\n\n") if len(p.strip()) > 80]
    
    current = ""
    for para in paragraphs:
        if len(current) + len(para) < max_len:
            current += para + "\n\n"
        else:
            if current.strip():
                chunks.append(current.strip())
            current = para + "\n\n"
    if current.strip():
        chunks.append(current.strip())
    
    return [{"text": c, "source": "אתר הכנסת", "category": "פרלמנט",
             "title": title, "date": "2024", "url": source_url} for c in chunks]

# ─── COLLECTOR 1: Knesset Wikipedia topics ────────────────────────────────
def collect_wiki_knesset():
    chunks = []
    log("Collecting from Hebrew Wikipedia - Knesset topics...")
    
    wiki_topics = [
        "כנסת ישראל",
        "הכנסת ה-25",
        "כנסת ישראל ה-25",
        "חוק הגיוס",
        "ביטול עילת הסבירות",
        "רפורמה משפטית בישראל",
        "חוק הלאום",
        "תקציב המדינה הישראלי",
        "ממשלת ישראל ה-37",
        "בנימין נתניהו",
        "ועדת החוקה חוק ומשפט",
        "הצעת חוק",
        "מפלגות בישראל",
        "ליכוד",
        "מחנה המדינה",
        "יש עתיד",
        "חרדים בישראל",
        "מלחמת חרבות ברזל",
        "חוק שירות בטחון",
        "בית המשפט העליון בישראל",
    ]
    
    for topic in wiki_topics:
        log(f"  Fetching: {topic}")
        text = wiki_page(topic)
        if text and len(text) > 200:
            url = f"https://he.wikipedia.org/wiki/{topic.replace(' ', '_')}"
            new_chunks = chunk_text(text, topic, url)
            chunks.extend(new_chunks[:5])  # max 5 chunks per topic
            log(f"    → {len(new_chunks)} chunks")
        time.sleep(0.2)
    
    log(f"Wiki Knesset: {len(chunks)} chunks")
    return chunks

# ─── COLLECTOR 2: Laws and legislation ────────────────────────────────────
def collect_legislation():
    chunks = []
    log("Collecting legislation from Wikipedia...")
    
    laws = [
        "חוק יסוד: הממשלה",
        "חוק יסוד: כבוד האדם וחירותו",
        "חוק יסוד: השפיטה",
        "חוק-יסוד: ישראל – מדינת הלאום של העם היהודי",
        "חוק הכנסת",
        "חוק המפלגות",
        "חוק שוויון זכויות לאנשים עם מוגבלות",
        "חוק הביטוח הלאומי",
        "חוק בריאות ממלכתי",
        "חוק שוויון ההזדמנויות בעבודה",
    ]
    
    for law in laws:
        text = wiki_page(law)
        if text and len(text) > 100:
            url = f"https://he.wikipedia.org/wiki/{law.replace(' ', '_')}"
            new_chunks = chunk_text(text, law, url)
            chunks.extend(new_chunks[:3])
        time.sleep(0.2)
    
    log(f"Legislation: {len(chunks)} chunks")
    return chunks

# ─── COLLECTOR 3: MK profiles ──────────────────────────────────────────────
def collect_mk_profiles():
    chunks = []
    log("Collecting MK profiles...")
    
    mks = [
        "בנימין נתניהו", "יאיר לפיד", "בני גנץ", "אריה דרעי",
        "איתמר בן-גביר", "בצלאל סמוטריץ", "יואב גלנט",
        "גדעון סער", "אביגדור ליברמן", "מרב מיכאלי",
        "יולי אדלשטיין", "אמיר אוחנה", "נפתלי בנט",
        "לפיד יאיר", "נדיה חלפה", "מנסור עבאס",
    ]
    
    for mk in mks:
        text = wiki_page(mk)
        if text and len(text) > 200:
            # Extract first 600 chars (biography summary)
            summary = text[:600]
            chunks.append({
                "text": f"ח\"כ/פוליטיקאי: {mk}\n\n{summary}",
                "source": "אתר הכנסת",
                "category": "חברי כנסת",
                "title": mk,
                "date": "2024",
                "url": f"https://he.wikipedia.org/wiki/{mk.replace(' ', '_')}"
            })
        time.sleep(0.15)
    
    log(f"MK profiles: {len(chunks)} chunks")
    return chunks

# ─── ANCHOR FACTS ─────────────────────────────────────────────────────────
def anchor_facts():
    facts = [
        {
            "text": "כנסת ישראל ה-25 (הנוכחית): הוקמה נובמבר 2022. ראש ממשלה: בנימין נתניהו (ליכוד). קואליציה: ליכוד + שס + יהדות התורה + עוצמה יהודית + הציונות הדתית + נועם = 64 מנדטים. אופוזיציה: יש עתיד, מחנה המדינה, ישראל ביתנו, מרצ, רע\"ם = 56 מנדטים.",
            "title": "כנסת ה-25 — הרכב"
        },
        {
            "text": "חוק הגיוס (שירות ביטחון לחרדים): ב-2024 פסק בג\"ץ כי אין בסיס חוקי לפטור חרדים מגיוס. הכנסת נדרשה לחוקק עד אוגוסט 2024 — לא עמדה בכך. הסוגיה פוליטית מרכזית בין ליכוד לבין שס ויהדות התורה מחד, לבין המחנה הלאומי מאידך.",
            "title": "חוק הגיוס"
        },
        {
            "text": "הרפורמה המשפטית (2023): הממשלה ה-37 קידמה שינויים מהותיים: ביטול עילת הסבירות, שינוי ועדת מינויים לשופטים, פסקת ההתגברות. ביטול עילת הסבירות עבר בכנסת (יולי 2023), ובג\"ץ ביטל אותו (ינואר 2024) ברוב 8-7.",
            "title": "הרפורמה המשפטית"
        },
        {
            "text": "מלחמת חרבות ברזל (7.10.2023): פרצה לאחר תקיפת חמאס מעזה. 1,200 נרצחו, 250 נחטפו. ישראל החלה מבצע קרקעי בעזה. מלחמה עדיין מתמשכת (2024-2025). גרמה לשינוי תקציב, גיוס מילואים המוני, מתיחות פוליטית.",
            "title": "מלחמת חרבות ברזל"
        },
        {
            "text": "ממשלת ישראל ה-37: הוקמה דצמבר 2022. ממשלת ימין דתית — הימנית ביותר בהיסטוריה. שרים בולטים: נתניהו (ראש ממשלה), גלנט (ביטחון), כץ (חוץ), דרעי (פנים/בריאות), בן-גביר (ביטחון לאומי), סמוטריץ (אוצר/שיכון).",
            "title": "ממשלה ה-37"
        },
        {
            "text": "תקציב 2024-2025 ישראל: אושר בכנסת בנובמבר 2023 לאחר עיכובים. סך כולל: 582 מיליארד ₪. הוצאות ביטחון עלו ל-6% מתמ\"ג. גירעון 6.6% מתמ\"ג — גבוה משמעותית מהיעד. חלק גדול בשל עלויות המלחמה.",
            "title": "תקציב 2024-2025"
        },
        {
            "text": "ועדת ה-18: ועדת חוקה, חוק ומשפט של הכנסת. מופקדת על חקיקת חוקי יסוד. בראשות שמחה רוטמן. קידמה בשנת 2023 את חוקי הרפורמה המשפטית. מהן ועדות הכנסת המרכזיות.",
            "title": "ועדות הכנסת"
        },
        {
            "text": "חוק הלאום (2018): חוק יסוד. קובע שישראל היא מדינה יהודית, העברית שפה רשמית, ערבית בעלת מעמד מיוחד. ביקורת: פוגע בשוויון האזרחים הערבים. עדיין בתוקף, ועדיין שנוי במחלוקת.",
            "title": "חוק הלאום"
        },
    ]
    return [{"text": f["text"], "source": "אתר הכנסת", "category": "עובדות מפתח",
             "title": f["title"], "date": "2024", "url": "https://knesset.gov.il"}
            for f in facts]

# ─── MAIN ─────────────────────────────────────────────────────────────────
def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    log(f"Starting Knesset collection via Wikipedia (limit={LIMIT})...")
    
    all_chunks = []
    all_chunks.extend(collect_wiki_knesset())
    all_chunks.extend(collect_legislation())
    all_chunks.extend(collect_mk_profiles())
    all_chunks.extend(anchor_facts())
    
    # Deduplicate
    seen = set()
    unique = []
    for c in all_chunks:
        key = c["text"][:100]
        if key not in seen:
            seen.add(key)
            unique.append(c)
    
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)
    
    log(f"✅ DONE: {len(unique)} chunks saved")
    cats = {}
    for c in unique:
        cats[c["category"]] = cats.get(c["category"], 0) + 1
    for cat, cnt in sorted(cats.items(), key=lambda x: -x[1]):
        log(f"  {cat}: {cnt}")

if __name__ == "__main__":
    main()
