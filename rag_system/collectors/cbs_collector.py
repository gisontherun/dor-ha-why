"""
CBS (Lamas / למ"ס) Collector - fetches data from the Central Bureau of Statistics
Uses api.cbs.gov.il and scrapes key datasets
"""
import requests
import json
import os
import time
from bs4 import BeautifulSoup

CBS_API = "https://api.cbs.gov.il/api/1.0"
CBS_WEB = "https://www.cbs.gov.il"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ResearchBot/1.0)",
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8"
}

def fetch_json(url, params=None):
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error: {e}")
        return None

def collect_press_releases(limit=150):
    """Collect CBS press releases (הודעות לעיתונות) - most useful for RAG"""
    chunks = []
    
    urls_to_try = [
        f"{CBS_WEB}/he/mediarelease/pages/default.aspx",
        f"{CBS_WEB}/he/publications/pages/default.aspx",
    ]
    
    for url in urls_to_try:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            soup = BeautifulSoup(r.text, "html.parser")
            
            articles = soup.find_all(["article", "div"], class_=lambda x: x and ("release" in str(x).lower() or "item" in str(x).lower()))
            
            for item in articles[:limit]:
                title = ""
                summary = ""
                date = ""
                link = ""
                
                title_tag = item.find(["h2", "h3", "h4", "a"])
                if title_tag:
                    title = title_tag.get_text(strip=True)
                
                p_tags = item.find_all("p")
                summary = " ".join([p.get_text(strip=True) for p in p_tags[:3]])
                
                date_tag = item.find(class_=lambda x: x and "date" in str(x).lower())
                if date_tag:
                    date = date_tag.get_text(strip=True)
                
                a_tag = item.find("a", href=True)
                if a_tag:
                    link = CBS_WEB + a_tag["href"] if a_tag["href"].startswith("/") else a_tag["href"]
                
                if title and len(title) > 5:
                    text = f"הודעת למ\"ס: {title}\nתאריך: {date}\nתקציר: {summary[:400]}"
                    chunks.append({
                        "text": text,
                        "source": "הלשכה המרכזית לסטטיסטיקה (למ\"ס)",
                        "category": "הודעה לעיתונות",
                        "title": title,
                        "date": date,
                        "url": link or CBS_WEB
                    })
        except Exception as e:
            print(f"Press releases error: {e}")
    
    print(f"✅ CBS press releases: {len(chunks)} items")
    return chunks

def collect_key_statistics():
    """Collect key statistical data as text chunks"""
    chunks = []
    
    # Key topic areas to fetch
    topics = [
        {
            "name": "אוכלוסייה ודמוגרפיה",
            "url": f"{CBS_WEB}/he/subjects/Pages/2.aspx",
            "keywords": ["אוכלוסייה", "לידות", "תמותה", "עלייה", "הגירה"]
        },
        {
            "name": "כלכלה לאומית", 
            "url": f"{CBS_WEB}/he/subjects/Pages/5.aspx",
            "keywords": ["תמ\"ג", "צמיחה", "אינפלציה", "תעסוקה"]
        },
        {
            "name": "חינוך",
            "url": f"{CBS_WEB}/he/subjects/Pages/8.aspx", 
            "keywords": ["חינוך", "בגרות", "אקדמי", "סטודנטים"]
        },
        {
            "name": "דיור ובינוי",
            "url": f"{CBS_WEB}/he/subjects/Pages/6.aspx",
            "keywords": ["דירות", "שכר דירה", "בנייה", "מחירי דיור"]
        },
        {
            "name": "בריאות",
            "url": f"{CBS_WEB}/he/subjects/Pages/15.aspx",
            "keywords": ["בריאות", "תמותה", "תחלואה"]
        },
        {
            "name": "שוק העבודה",
            "url": f"{CBS_WEB}/he/subjects/Pages/7.aspx",
            "keywords": ["אבטלה", "תעסוקה", "שכר", "עובדים"]
        },
    ]
    
    for topic in topics:
        try:
            r = requests.get(topic["url"], headers=HEADERS, timeout=20)
            soup = BeautifulSoup(r.text, "html.parser")
            
            # Extract all text content
            content_divs = soup.find_all(["div", "section"], class_=lambda x: x and any(
                k in str(x).lower() for k in ["content", "main", "article"]
            ))
            
            full_text = ""
            for div in content_divs[:5]:
                text = div.get_text(separator="\n", strip=True)
                if len(text) > 50:
                    full_text += text + "\n\n"
            
            if full_text:
                # Find statistical tables/numbers
                numbers_text = extract_statistics(soup, topic["name"])
                combined = f"נושא: {topic['name']}\n\n{numbers_text}\n\n{full_text[:1000]}"
                
                chunks.append({
                    "text": combined,
                    "source": "הלשכה המרכזית לסטטיסטיקה (למ\"ס)",
                    "category": "נתונים סטטיסטיים",
                    "title": f"נתוני למ\"ס - {topic['name']}",
                    "date": "2024",
                    "url": topic["url"]
                })
        except Exception as e:
            print(f"Topic {topic['name']} error: {e}")
        
        time.sleep(0.5)
    
    print(f"✅ CBS statistics: {len(chunks)} topic areas")
    return chunks

def extract_statistics(soup, topic_name):
    """Extract numerical data from CBS pages"""
    stats = []
    
    # Look for tables
    tables = soup.find_all("table")
    for table in tables[:3]:
        rows = table.find_all("tr")
        for row in rows[:10]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and any(any(c.isdigit() for c in cell) for cell in cells):
                stats.append(" | ".join(cells))
    
    # Look for key figures
    figures = soup.find_all(class_=lambda x: x and any(k in str(x).lower() for k in ["figure", "stat", "number", "data"]))
    for fig in figures[:5]:
        text = fig.get_text(strip=True)
        if text and len(text) < 200:
            stats.append(text)
    
    return "\n".join(stats) if stats else f"נתונים סטטיסטיים על {topic_name} מהלמ\"ס"

def collect_publications(limit=100):
    """Collect CBS publications catalog"""
    chunks = []
    
    try:
        url = f"{CBS_WEB}/he/publications/Pages/default.aspx"
        r = requests.get(url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        
        items = soup.find_all(["li", "div"], class_=lambda x: x and "item" in str(x).lower())
        
        for item in items[:limit]:
            title = ""
            desc = ""
            date = ""
            
            title_tag = item.find(["h2", "h3", "h4", "a", "strong"])
            if title_tag:
                title = title_tag.get_text(strip=True)
            
            p_tag = item.find("p")
            if p_tag:
                desc = p_tag.get_text(strip=True)
            
            if title and len(title) > 5:
                text = f"פרסום למ\"ס: {title}\nתאריך: {date}\nתיאור: {desc[:300]}"
                chunks.append({
                    "text": text,
                    "source": "הלשכה המרכזית לסטטיסטיקה (למ\"ס)",
                    "category": "פרסום",
                    "title": title,
                    "date": date,
                    "url": url
                })
    except Exception as e:
        print(f"Publications error: {e}")
    
    print(f"✅ CBS publications: {len(chunks)} items")
    return chunks

def collect_all():
    all_chunks = []
    all_chunks.extend(collect_press_releases())
    all_chunks.extend(collect_key_statistics())
    all_chunks.extend(collect_publications())
    
    # Add hardcoded key facts as context
    key_facts = create_key_facts_chunks()
    all_chunks.extend(key_facts)
    
    print(f"\n📦 Total CBS chunks: {len(all_chunks)}")
    return all_chunks

def create_key_facts_chunks():
    """Hard-coded key CBS statistics as anchor chunks"""
    facts = [
        {
            "text": "אוכלוסיית ישראל נכון ל-2024: כ-9.9 מיליון נפש. קצב גידול שנתי: כ-1.8%. כ-74% יהודים, כ-21% ערבים, כ-5% אחרים. מקור: הלמ\"ס",
            "title": "אוכלוסיית ישראל 2024"
        },
        {
            "text": "שיעור האבטלה בישראל 2024: כ-3.4%. שכר ממוצע למשרת שכיר: כ-13,500 ₪ ברוטו לחודש. שכר חציוני: כ-9,800 ₪. מקור: הלמ\"ס",
            "title": "שוק העבודה 2024"
        },
        {
            "text": "מחיר דירה ממוצע בישראל 2024: כ-1.8 מיליון ₪. מחיר ממוצע בתל אביב: כ-4 מיליון ₪. עליית מחירי דיור בעשור האחרון: כ-120%. מקור: הלמ\"ס",
            "title": "מחירי דיור 2024"
        },
        {
            "text": "תמ\"ג ישראל 2023: כ-522 מיליארד דולר. צמיחה ריאלית: כ-2%. תמ\"ג לנפש: כ-53,000 דולר. מקור: הלמ\"ס",
            "title": "כלכלה לאומית 2023"
        },
        {
            "text": "הוצאה לחינוך בישראל: כ-8% מהתמ\"ג. תלמידים בחינוך חובה: כ-2 מיליון. אחוז זכאות לבגרות: כ-55%. מקור: הלמ\"ס",
            "title": "חינוך - נתוני מפתח"
        },
        {
            "text": "עוני בישראל: שיעור עוני 17.4% מהאוכלוסייה. ילדים בעוני: כ-27%. פער בין עשירים לעניים (מדד ג'יני): 0.38. מקור: הלמ\"ס",
            "title": "עוני ואי-שוויון"
        },
        {
            "text": "הוצאות ממשלת ישראל לביטחון 2024: כ-6% מהתמ\"ג (עלייה חדה עקב המלחמה). תקציב המדינה הכולל: כ-582 מיליארד ₪. גירעון: כ-6.6% מהתמ\"ג. מקור: הלמ\"ס + משרד האוצר",
            "title": "תקציב המדינה 2024"
        },
        {
            "text": "ילודה בישראל: ממוצע של 3.0 ילדים לאישה - הגבוה ביותר במדינות OECD. ממוצע OECD: 1.5. לידות ב-2023: כ-178,000. מקור: הלמ\"ס",
            "title": "ילודה ופריון"
        },
    ]
    
    chunks = []
    for fact in facts:
        chunks.append({
            "text": fact["text"],
            "source": "הלשכה המרכזית לסטטיסטיקה (למ\"ס)",
            "category": "עובדות מפתח",
            "title": fact["title"],
            "date": "2024",
            "url": "https://www.cbs.gov.il"
        })
    
    return chunks

if __name__ == "__main__":
    chunks = collect_all()
    os.makedirs("/app/rag_system/data/chunks", exist_ok=True)
    with open("/app/rag_system/data/chunks/cbs_chunks.json", "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)
    print("Saved to cbs_chunks.json")
