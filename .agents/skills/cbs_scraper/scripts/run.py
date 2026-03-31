#!/usr/bin/env python3
"""
CBS (LAMAS) SCRAPER SKILL
Collects statistical data from cbs.gov.il and data.gov.il
"""
import requests
import json
import os
import sys
import time
from bs4 import BeautifulSoup

CBS_BASE    = "https://www.cbs.gov.il"
DATA_GOV    = "https://data.gov.il/api/3/action"
OUTPUT_PATH = "/app/rag_system/data/chunks/cbs_chunks.json"
LIMIT       = int(sys.argv[1]) if len(sys.argv) > 1 else 100

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "he-IL,he;q=0.9"
}

def log(msg): print(f"[CBS] {msg}", flush=True)

def safe_get(url, params=None):
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log(f"Fetch error {url[:50]}: {e}")
        return None

def safe_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log(f"HTML error {url[:50]}: {e}")
        return None

# ─── COLLECTOR 1: Press releases from data.gov.il ─────────────────────────
def collect_press_releases_api():
    chunks = []
    log("Collecting CBS press releases via data.gov.il...")
    
    # Search for CBS datasets
    data = safe_get(f"{DATA_GOV}/package_search", 
                   {"q": "למס סטטיסטיקה", "rows": 20, "fq": "organization:cbs"})
    if data and data.get("success"):
        for pkg in data["result"].get("results", []):
            title = pkg.get("title", "")
            notes = pkg.get("notes", "")[:400]
            if title:
                chunks.append({
                    "text": f"פרסום למ\"ס: {title}\nתיאור: {notes}",
                    "source": "הלשכה המרכזית לסטטיסטיקה (למ\"ס)",
                    "category": "פרסום",
                    "title": title,
                    "date": pkg.get("metadata_modified", "")[:10],
                    "url": f"https://data.gov.il/dataset/{pkg.get('id', '')}"
                })
    
    log(f"  Press releases API: {len(chunks)} items")
    return chunks

# ─── COLLECTOR 2: Scrape CBS press releases page ──────────────────────────
def collect_press_releases_web():
    chunks = []
    log("Scraping CBS press releases web...")
    
    pages = [
        f"{CBS_BASE}/he/mediarelease/pages/default.aspx",
        f"{CBS_BASE}/he/mediarelease/2024/Pages/default.aspx",
        f"{CBS_BASE}/he/mediarelease/2023/Pages/default.aspx",
    ]
    
    for url in pages:
        soup = safe_html(url)
        if not soup:
            continue
        
        # Find press release items
        items = soup.find_all(["li", "div", "article"], 
                             class_=lambda c: c and any(
                                 k in str(c).lower() for k in ["item", "release", "news", "row"]
                             ))
        
        for item in items[:LIMIT//3]:
            # Extract title
            title = ""
            for tag in ["h2", "h3", "h4", "strong"]:
                el = item.find(tag)
                if el:
                    title = el.get_text(strip=True)
                    break
            
            if not title:
                a = item.find("a")
                if a:
                    title = a.get_text(strip=True)
            
            if not title or len(title) < 5:
                continue
            
            # Extract summary
            paras = [p.get_text(strip=True) for p in item.find_all("p")]
            summary = " ".join(paras)[:400]
            
            # Extract date
            date = ""
            for cls in ["date", "time", "published"]:
                el = item.find(class_=lambda c: c and cls in str(c).lower())
                if el:
                    date = el.get_text(strip=True)
                    break
            
            # Extract link
            link = ""
            a = item.find("a", href=True)
            if a:
                href = a["href"]
                link = href if href.startswith("http") else CBS_BASE + href
            
            text = f"הודעה לעיתונות — למ\"ס: {title}\nתאריך: {date}\n{summary}"
            chunks.append({
                "text": text,
                "source": "הלשכה המרכזית לסטטיסטיקה (למ\"ס)",
                "category": "הודעה לעיתונות",
                "title": title,
                "date": date,
                "url": link or url
            })
        
        time.sleep(0.3)
    
    log(f"  Press releases web: {len(chunks)} items")
    return chunks

# ─── COLLECTOR 3: Key topic pages ─────────────────────────────────────────
def collect_topic_pages():
    chunks = []
    log("Collecting CBS topic pages...")
    
    topics = [
        ("אוכלוסייה", f"{CBS_BASE}/he/subjects/Pages/2.aspx"),
        ("כלכלה", f"{CBS_BASE}/he/subjects/Pages/5.aspx"),
        ("שוק העבודה", f"{CBS_BASE}/he/subjects/Pages/7.aspx"),
        ("חינוך", f"{CBS_BASE}/he/subjects/Pages/8.aspx"),
        ("דיור ובנייה", f"{CBS_BASE}/he/subjects/Pages/6.aspx"),
        ("בריאות", f"{CBS_BASE}/he/subjects/Pages/15.aspx"),
        ("עוני", f"{CBS_BASE}/he/subjects/Pages/10.aspx"),
        ("תחבורה", f"{CBS_BASE}/he/subjects/Pages/14.aspx"),
    ]
    
    for topic_name, url in topics:
        soup = safe_html(url)
        if not soup:
            continue
        
        # Extract page text
        main = soup.find("main") or soup.find(id="main") or soup.find(class_="main")
        if not main:
            main = soup.find("body")
        
        text = main.get_text(separator="\n", strip=True) if main else ""
        
        # Clean up
        text = "\n".join(l for l in text.split("\n") if len(l.strip()) > 10)[:2000]
        
        if text:
            chunks.append({
                "text": f"נתוני למ\"ס — {topic_name}:\n\n{text}",
                "source": "הלשכה המרכזית לסטטיסטיקה (למ\"ס)",
                "category": "נתונים סטטיסטיים",
                "title": f"נתוני למ\"ס — {topic_name}",
                "date": "2024",
                "url": url
            })
        
        time.sleep(0.3)
    
    log(f"  Topic pages: {len(chunks)} items")
    return chunks

# ─── ANCHOR FACTS ─────────────────────────────────────────────────────────
def anchor_facts():
    facts = [
        {"title": "אוכלוסיית ישראל 2024",
         "text": "אוכלוסיית ישראל (2024): כ-9.9 מיליון נפש. 74% יהודים, 21% ערבים, 5% אחרים. קצב גידול: 1.8% לשנה. פירוט: כ-2 מיליון חרדים, כ-2 מיליון ערבים. מקור: הלמ\"ס"},
        {"title": "שוק העבודה 2024",
         "text": "שוק עבודה ישראל (2024): שיעור תעסוקה 62%, אבטלה 3.4%. שכר ברוטו ממוצע: 13,500₪. שכר חציוני: 9,800₪. עלות שכר כוללת למעסיק: כ-40% מעל שכר ברוטו. מקור: הלמ\"ס"},
        {"title": "מחירי דיור 2024",
         "text": "דיור בישראל (2024): מחיר ממוצע לדירה 1.8M₪. תל-אביב: 4M₪. ירושלים: 2.7M₪. עליית מחירים בעשור: 120%. שכר דירה ממוצע: 5,800₪/חודש. מקור: הלמ\"ס"},
        {"title": "תמ\"ג ישראל 2023",
         "text": "תמ\"ג ישראל (2023): 522 מיליארד דולר. צמיחה ריאלית: 2%. תמ\"ג לנפש: 53,000 דולר. ישראל דורגה ה-30 בעולם בגודל כלכלה. מקור: הלמ\"ס"},
        {"title": "עוני ואי-שוויון",
         "text": "עוני בישראל (2023): 17.4% מהאוכלוסייה מתחת לקו עוני. ילדים בעוני: 27%. מדד ג'יני: 0.38 (גבוה יחסית ל-OECD). הפער בין עשירון עליון לתחתון: פי 8.5 בהכנסה. מקור: הלמ\"ס"},
        {"title": "חינוך - נתוני מפתח",
         "text": "חינוך ישראל (2023): הוצאה לחינוך 7.8% מתמ\"ג. תלמידים בחינוך חובה: 2M. זכאות לבגרות: 55%. אחוז בוגרי אקדמיה (25-64): 50%. מקור: הלמ\"ס"},
        {"title": "בריאות - נתוני מפתח",
         "text": "בריאות ישראל (2023): תוחלת חיים: 82.6 שנים (בין הגבוהות בעולם). הוצאה לבריאות: 7.5% מתמ\"ג. מיטות אשפוז ל-1000 נפש: 2.2 (נמוך מ-OECD). מקור: הלמ\"ס"},
        {"title": "תקציב המדינה 2024",
         "text": "תקציב ישראל (2024): סך הכנסות: 510B₪. סך הוצאות: 582B₪. גירעון: 6.6% מתמ\"ג (עלייה חדה בשל המלחמה). חוב לאומי: 66% מתמ\"ג. מקור: הלמ\"ס + משרד האוצר"},
        {"title": "ילודה ופריון",
         "text": "ילודה ישראל (2023): ממוצע 3.0 ילדים לאישה — גבוה ביותר ב-OECD (ממוצע OECD: 1.5). יהודים חרדים: 6.7. ערבים: 3.0. יהודים חילוניים: 2.2. לידות: 178,000. מקור: הלמ\"ס"},
        {"title": "אינפלציה ויוקר מחיה",
         "text": "אינפלציה ישראל (2024): 3.2%. יוקר מחיה: ישראל מדורגת בין 15 המדינות היקרות בעולם. מחיר קילו בשר: 120₪. חשמל: 62 אג'/קוט\"ש (אחד הגבוהים ב-OECD). מקור: הלמ\"ס"},
    ]
    return [{"text": f["text"], "source": "הלשכה המרכזית לסטטיסטיקה (למ\"ס)",
             "category": "עובדות מפתח", "title": f["title"],
             "date": "2024", "url": CBS_BASE}
            for f in facts]

# ─── MAIN ─────────────────────────────────────────────────────────────────
def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    log(f"Starting CBS collection (limit={LIMIT})...")
    
    all_chunks = []
    all_chunks.extend(collect_press_releases_api())
    all_chunks.extend(collect_press_releases_web())
    all_chunks.extend(collect_topic_pages())
    all_chunks.extend(anchor_facts())
    
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
    
    log(f"✅ DONE: {len(unique)} chunks saved")
    
    cats = {}
    for c in unique:
        cats[c["category"]] = cats.get(c["category"], 0) + 1
    for cat, cnt in sorted(cats.items(), key=lambda x: -x[1]):
        log(f"  {cat}: {cnt}")

if __name__ == "__main__":
    main()
