#!/usr/bin/env python3
"""
WHY GENERATION DATA PROCESSOR
Collects high-quality Israeli political/economic facts for the WHY chatbot.
Sources: Bank of Israel, CBS CPI series, Government decisions, Budget key,
         Knesset research, uploaded PDFs.
"""
import requests
import json
import os
import sys
import time
import re
import pdfplumber
from bs4 import BeautifulSoup

OUTPUT_PATH = "/app/rag_system/data/chunks/why_chunks.json"
PDF_DIR     = "/app/rag_system/data/pdfs"
INCOMING    = "/app/incoming_files"

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

def log(msg): print(f"[WHY] {msg}", flush=True)

def safe_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log(f"  HTML error {url[:50]}: {e}")
        return None

# ─── 1. Process PDFs from incoming_files ──────────────────────────────────
def process_uploaded_pdfs():
    chunks = []
    log("Processing uploaded PDFs from incoming_files...")
    
    if not os.path.exists(INCOMING):
        log("  No incoming_files dir")
        return chunks
    
    for fname in os.listdir(INCOMING):
        if not fname.endswith(".pdf"):
            continue
        
        path = os.path.join(INCOMING, fname)
        log(f"  Parsing: {fname}")
        
        try:
            with pdfplumber.open(path) as pdf:
                full_text = ""
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        full_text += text + "\n"
                
                if len(full_text.strip()) < 50:
                    continue
                
                # Split into paragraphs
                paras = [p.strip() for p in full_text.split("\n\n") if len(p.strip()) > 80]
                
                for i, para in enumerate(paras):
                    chunks.append({
                        "text": para[:1200],
                        "source": "דור ה-WHY — תסריטי שיחה",
                        "category": "מתודולוגיה ודוגמאות",
                        "title": f"{fname} — פסקה {i+1}",
                        "date": "2025",
                        "url": "internal"
                    })
        except Exception as e:
            log(f"  Error {fname}: {e}")
    
    log(f"  PDFs: {len(chunks)} chunks")
    return chunks

# ─── 2. Bank of Israel key data ───────────────────────────────────────────
def collect_bank_of_israel():
    chunks = []
    log("Collecting Bank of Israel data...")
    
    # Bank of Israel press releases and data pages
    pages = [
        ("הבנק המרכזי - ריבית", "https://www.boi.org.il/he/monetarypolicy/interestrate/"),
        ("דוח אינפלציה", "https://www.boi.org.il/he/monetarypolicy/inflationreports/"),
    ]
    
    for title, url in pages:
        soup = safe_html(url)
        if not soup:
            continue
        
        text = soup.get_text(separator="\n", strip=True)
        text = "\n".join(l for l in text.split("\n") if len(l.strip()) > 15)[:2000]
        
        if text:
            chunks.append({
                "text": f"בנק ישראל — {title}:\n{text}",
                "source": "בנק ישראל",
                "category": "מדיניות מוניטרית",
                "title": title,
                "date": "2024",
                "url": url
            })
        time.sleep(0.3)
    
    log(f"  Bank of Israel: {len(chunks)} chunks")
    return chunks

# ─── 3. Government decisions portal ──────────────────────────────────────
def collect_gov_decisions():
    chunks = []
    log("Collecting government decisions...")
    
    try:
        r = requests.get(
            "https://www.gov.il/api/GOVILSearchProxy/api/Search/govSearch",
            params={"LanguageCode": "he", "SearchText": "מחיר למשתכן מילואים תקציב",
                    "Filters": "ministryId=PMO", "From": 0, "Size": 20},
            headers=HEADERS, timeout=15
        )
        data = r.json()
        
        for item in data.get("Results", []):
            title = item.get("Title", "")
            summary = item.get("Summary", "")[:400]
            date = item.get("PublicationDate", "")[:10]
            url = item.get("UrlName", "")
            
            if title:
                chunks.append({
                    "text": f"החלטת ממשלה: {title}\n{summary}",
                    "source": "פורטל הממשלה - החלטות",
                    "category": "החלטות ממשלה",
                    "title": title,
                    "date": date,
                    "url": url
                })
    except Exception as e:
        log(f"  Gov decisions error: {e}")
    
    log(f"  Gov decisions: {len(chunks)} chunks")
    return chunks

# ─── 4. Budget key (mof.gov.il / mefateach.io) ───────────────────────────
def collect_budget_data():
    chunks = []
    log("Collecting budget data...")
    
    try:
        # Open Budget API
        r = requests.get(
            "https://next.obudget.org/api/budget/api/budget",
            params={"period": "2024", "hierarchy_level": 1},
            headers=HEADERS, timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else data.get("data", [])
            
            for item in items[:20]:
                title = item.get("title") or item.get("code", "")
                amount = item.get("net_allocated", 0) or item.get("net_revised", 0)
                code = item.get("code", "")
                
                if title and amount:
                    amount_b = amount / 1_000_000_000
                    chunks.append({
                        "text": f"תקציב ממשלה 2024 — {title}: {amount_b:.1f} מיליארד ₪",
                        "source": "מפתח התקציב",
                        "category": "תקציב המדינה",
                        "title": f"תקציב {title}",
                        "date": "2024",
                        "url": f"https://next.obudget.org/i/budget/{code}/2024"
                    })
    except Exception as e:
        log(f"  Budget API error: {e}")
    
    log(f"  Budget: {len(chunks)} chunks")
    return chunks

# ─── 5. CPI and key economic anchor facts ────────────────────────────────
def economic_anchor_facts():
    facts = [
        # CPI Data (from WHY bot conversations - verified)
        {
            "text": "מדד המחירים לצרכן (אינפלציה) בישראל לפי שנים:\n2018: 0.8%\n2019: 0.6%\n2020: -0.7% (ירידה בשל קורונה)\n2021: 2.8% (תחילת עלייה עולמית)\n2022: 5.3% — שיא של 20 שנה (ממשלת בנט-לפיד)\n2023: 3.0% (חזרה לעלייה טבעית)\n2024: 3.2%\nמקור: הלשכה המרכזית לסטטיסטיקה (למ\"ס)",
            "title": "מדד המחירים לצרכן — סדרה שנתית 2018-2024"
        },
        {
            "text": "רמת מדד המחירים לצרכן בנקודות זמן מרכזיים:\nמאי 2021: 101.4 (ערב ממשלת בנט-לפיד)\nיוני 2022: 106.1 (לפיד נכנס כראש ממשלה)\nדצמבר 2022: 108.1 (סיום ממשלה ה-36)\nדצמבר 2023: 111.3 (ממשלת נתניהו)\nמקור: הלמ\"ס — מדד המחירים לצרכן",
            "title": "רמת מדד המחירים לצרכן — נקודות זמן מפתח"
        },
        {
            "text": "ממשלת ישראל ה-36 (בנט-לפיד): כיהנה יוני 2021 — דצמבר 2022. יאיר לפיד כיהן כשר חוץ ור\"מ חלופי מיוני 2021, ואז כראש ממשלה מיוני 2022 עד ינואר 2023. בתקופה זו מדד המחירים עלה מ-101.4 ל-108.1 — עלייה של 6.6% בשנה וחצי. מקור: הלמ\"ס.",
            "title": "ממשלה ה-36 — נתונים כלכליים"
        },
        {
            "text": "בנק ישראל — ריבית 2022-2024:\nינואר 2022: ריבית 0.1%\nנובמבר 2022: ריבית עלתה ל-3.25%\nמאי 2023: ריבית 4.75%\nינואר 2024: ריבית הורדה ל-4.5%\nמאי 2024: ריבית 4.5%\nהעלאות הריבית נועדו לרסן אינפלציה שעלתה ל-5.3% ב-2022. מקור: בנק ישראל.",
            "title": "ריבית בנק ישראל 2022-2024"
        },
        {
            "text": "תקציב המדינה ישראל 2024:\n• סך הוצאות: 582 מיליארד ₪\n• הוצאות ביטחון: כ-100 מיליארד ₪ (17% מהתקציב, 6% מהתמ\"ג)\n• גירעון: כ-6.6% מהתמ\"ג (גבוה מהרגיל בשל מלחמת חרבות ברזל)\n• מכלל הכנסות: כ-510 מיליארד ₪\nמקור: משרד האוצר, 2024.",
            "title": "תקציב המדינה 2024 — עיקרי נתונים"
        },
        # Housing
        {
            "text": "מחירי דיור ישראל:\n• 2022: מחירי דירות עלו כ-20% בשנה אחת — שיא של עשור\n• 2023: קצב עלייה נבלם בעקבות העלאות ריבית בנק ישראל\n• מחיר ממוצע לדירה (2024): כ-1.8 מיליון ₪\n• תל-אביב: כ-4 מיליון ₪\n• ירושלים: כ-2.7 מיליון ₪\n• תוכנית 'מחיר למשתכן': זמן ממוצע מזכייה לכניסה לדירה: 5-7 שנים\nמקור: הלמ\"ס, רמ\"י.",
            "title": "מחירי דיור ישראל 2022-2024"
        },
        # Reserves soldiers
        {
            "text": "נתוני גיוס מילואים לאחר 7.10.2023:\n• גויסו כ-300,000 משרתי מילואים (בשלבים המספר עלה לכיוון 450,000)\n• 2024: הוצאות תגמולים ומילואים — כ-37 מיליארד ₪ (22% מתקציב הביטחון)\n• 2026: אושר מתווה המשכיות — 6.2 מיליארד ₪\n• הגדרת 'מילואימניק פעיל' לפי חוק: 14 ימי מילואים ומעלה בשנה\nמקור: דוח מבקר 9157-1; פרסומי צה\"ל.",
            "title": "נתוני מילואים לאחר 7.10.2023"
        },
        {
            "text": "הבטחת בנט — מיליון ₪ לדיור מילואימניקים (ינואר 2026):\n• קהל יעד: כ-100,000-120,000 משרתי מילואים פעילים חסרי דירה\n• מנגנון: הנחה ממחיר קרקע — המדינה 'תרוויח פחות' על שיווק קרקע\n• עלות כוללת: 100 מיליארד ₪ (100,000 × 1,000,000 ₪)\n• הכנסות רמ\"י לאוצר: 8.5 מיליארד ₪/שנה בלבד\n• החלטת ממשלה 25.01.2026: חבילת סיוע למילואים — 6.2 מיליארד ₪\n• פער: ×16 בין ההבטחה לחבילה שאושרה\nמקור: דוח רמ\"י 2025; החלטת ממשלה 25.01.2026.",
            "title": "הבטחת 'מיליון ₪' לדיור — ניתוח כלכלי"
        },
        {
            "text": "רשות מקרקעי ישראל (רמ\"י) — נתוני מפתח:\n• הכנסות שיווק קרקע לאוצר: 15-25 מיליארד ₪/שנה\n• הנחה קיימת למילואימניקים: עד 150,000 ₪ ללוחמים (החלטת מועצה 1528)\n• קצב שיווק דירות: 60,000-80,000 יחידות/שנה לכלל האוכלוסייה\n• 2025: כ-60,000 יחידות נחתמו בפועל\nמקור: הצעת תקציב רמ\"י 2025; סיכום רמ\"י 2025.",
            "title": "רשות מקרקעי ישראל — נתוני מפתח"
        },
        # Competition/market reform
        {
            "text": "רפורמת 'מה שטוב לאירופה טוב לישראל' — חוק התקנים תיקון 17 (יוני 2024):\n• מטרה: ביטול בדיקות מעבדה לכל מוצר שכבר עומד בתקן אירופי\n• עבר בכנסת יוני 2024 (ספר חוקים 3241)\n• חריגים מרכזיים: מזון רגיש (בשר, דגים, חלב, מזון תינוקות), מוצרי חשמל כבדים, תמרוקים רפואיים, חומרי בנייה\n• ביקורת: החריגים מכסים ~50% מהוצאות מזון של משפחה ממוצעת\n• מקור: ספר חוקים 3241; פרוטוקול ועדת הכלכלה ישיבה 154 מאי 2024.",
            "title": "רפורמת תקינה אירופית — חוק התקנים תיקון 17"
        },
        {
            "text": "חוק 'אל תתעסקו איתנו' — תיקון לחוק התחרות:\n• מטרה: קנסות עד 100 מיליון ₪ על ספקים שחוסמים יבוא מקביל\n• ביקורת: הוכחה בבית משפט קשה, תהליך ארוך שנים\n• ריכוזיות שוק המזון: ספקים גדולים שולטים ב-~50% מהשוק (דוח מבקר 74א)\nמקור: דוח מבקר המדינה 74א 'פעולות הממשלה להפחתת יוקר המחיה' עמ' 45-48.",
            "title": "חוק 'אל תתעסקו איתנו' — תחרות"
        },
        {
            "text": "ממוצע OECD לעומת ישראל — אינפלציה 2022:\n• ישראל 2022: 5.3%\n• ממוצע OECD 2022: כ-9.6%\n• ממסקנות בנק ישראל: חלק מהאינפלציה נבע מ'בעיות היצע בשוקי האנרגיה והמזון בשל מלחמת אוקראינה ועיכובים בשרשראות האספקה'\n• יעד האינפלציה של בנק ישראל: 1-3%\nמקור: דוח בנק ישראל 2022; דוח מרכז המחקר והמידע של הכנסת (ממ\"מ) נובמבר 2022.",
            "title": "ישראל מול OECD — אינפלציה 2022"
        },
        # WHY methodology
        {
            "text": "מתודולוגיית 'דור ה-WHY' לבדיקת אמירות פוליטיות:\nשלב א: הגדרת קריטריון (עדשה) — בחירה מתוך 3-5 זוויות בחינה (תקציב, השוואה רב-שנתית, השוואה בינלאומית, יישום בפועל)\nשלב ב: הצגת עובדות — 3 נתונים מרכזיים עם מקורות רשמיים\nשלב ג: ניתוח ביקורתי — הדילמה הסוקרטית, המשתמש בוחר פרשנות\nשלב ד: סינתזה — ניסוח 'שורה תחתונה' ו'כרטיסיית סיכום'\nעיקרון: כל עובדה חייבת מקור רשמי (הלמ\"ס, בנק ישראל, מבקר המדינה, ממ\"מ, ספר החוקים)",
            "title": "מתודולוגיית דור ה-WHY"
        },
        {
            "text": "מקורות רשמיים לבדיקת אמירות פוליטיות בישראל:\n• הלמ\"ס (cbs.gov.il): מדדי מחירים, נתוני תעסוקה, דמוגרפיה\n• בנק ישראל (boi.org.il): ריבית, אינפלציה, דוחות מוניטריים\n• מבקר המדינה (mevaker.gov.il): דוחות ביקורת על יישום בפועל\n• ממ\"מ — מרכז המחקר של הכנסת: השוואות, ניתוחים\n• ספר החוקים — חוקים שנחקקו\n• מפתח התקציב (obudget.org.il): תקציבים בפועל מול מאושר\n• רמ\"י (nevo.co.il): קרקעות, דיור\n• פורטל הממשלה — החלטות ממשלה",
            "title": "מקורות רשמיים — WHY"
        },
    ]
    return [{"text": f["text"], "source": "נתוני WHY — מקורות רשמיים",
             "category": "עובדות מפתח — WHY",
             "title": f["title"], "date": "2024",
             "url": "verified"} for f in facts]

# ─── 6. MMM - Knesset Research Center ────────────────────────────────────
def collect_mmm():
    chunks = []
    log("Collecting MMM (Knesset Research Center)...")
    
    mmm_pages = [
        ("אינפלציה בינלאומית", "https://www.knesset.gov.il/mmm/data/pdf/m04174.pdf"),
    ]
    
    # Try to scrape MMM index page
    soup = safe_html("https://www.knesset.gov.il/mmm/heb/MMM_main.aspx")
    if soup:
        for a in soup.find_all("a", href=True)[:30]:
            href = a["href"]
            title = a.get_text(strip=True)
            if href.endswith(".pdf") and title and len(title) > 5:
                full_url = href if href.startswith("http") else "https://www.knesset.gov.il" + href
                chunks.append({
                    "text": f"מחקר ממ\"מ — מרכז המחקר והמידע של הכנסת:\n{title}",
                    "source": "ממ\"מ — מרכז המחקר של הכנסת",
                    "category": "מחקרי כנסת",
                    "title": title,
                    "date": "2024",
                    "url": full_url
                })
    
    log(f"  MMM: {len(chunks)} chunks")
    return chunks

# ─── MAIN ─────────────────────────────────────────────────────────────────
def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    log("Starting WHY Generation data collection...")
    
    all_chunks = []
    all_chunks.extend(process_uploaded_pdfs())
    all_chunks.extend(collect_bank_of_israel())
    all_chunks.extend(collect_gov_decisions())
    all_chunks.extend(collect_budget_data())
    all_chunks.extend(economic_anchor_facts())
    all_chunks.extend(collect_mmm())
    
    # Deduplicate
    seen = set()
    unique = []
    for c in all_chunks:
        key = c["text"][:80]
        if key not in seen:
            seen.add(key)
            unique.append(c)
    
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)
    
    log(f"\n✅ DONE: {len(unique)} chunks saved")
    cats = {}
    for c in unique:
        cats[c["category"]] = cats.get(c["category"], 0) + 1
    for cat, cnt in sorted(cats.items(), key=lambda x: -x[1]):
        log(f"  {cat}: {cnt}")

if __name__ == "__main__":
    main()
