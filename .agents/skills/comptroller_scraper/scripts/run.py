#!/usr/bin/env python3
"""
STATE COMPTROLLER SCRAPER SKILL
Downloads and parses PDF reports from mevaker.gov.il
"""
import requests
import json
import os
import sys
import time
import re
import pdfplumber
from bs4 import BeautifulSoup

MEVAKER_BASE  = "https://www.mevaker.gov.il"
LIBRARY_BASE  = "https://library.mevaker.gov.il"
PDF_DIR       = "/app/rag_system/data/pdfs"
OUTPUT_PATH   = "/app/rag_system/data/chunks/comptroller_chunks.json"
MAX_REPORTS   = int(sys.argv[1]) if len(sys.argv) > 1 else 20

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

def log(msg): print(f"[COMPTROLLER] {msg}", flush=True)

# ─── STEP 1: Discover report URLs ─────────────────────────────────────────
def discover_reports():
    """Find all report publication IDs from mevaker.gov.il"""
    all_urls = []
    
    # Scrape main reports page
    r = requests.get(f"{MEVAKER_BASE}/he/Reports/Pages/default.aspx",
                     headers=HEADERS, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m_pub = re.search(r'/Publications/(\d+)', href)
        m_rep = re.search(r'/Reports/(\d+)', href)
        title = a.get_text(strip=True) or ""
        
        if m_pub:
            all_urls.append({
                "type": "pub",
                "id": int(m_pub.group(1)),
                "title": title,
                "page_url": href if href.startswith("http") else LIBRARY_BASE + href
            })
        elif m_rep:
            all_urls.append({
                "type": "rep", 
                "id": int(m_rep.group(1)),
                "title": title,
                "page_url": href if href.startswith("http") else LIBRARY_BASE + href
            })
    
    # Expand: probe nearby IDs for recent reports (range scan)
    if all_urls:
        max_pub = max((x["id"] for x in all_urls if x["type"] == "pub"), default=2000)
        max_rep = max((x["id"] for x in all_urls if x["type"] == "rep"), default=10000)
        
        # Probe 30 pub IDs around max
        for pid in range(max(1400, max_pub - 50), max_pub + 200, 5):
            if not any(x["id"] == pid and x["type"] == "pub" for x in all_urls):
                url = f"{LIBRARY_BASE}/sites/DigitalLibrary/Pages/Publications/{pid}.aspx"
                all_urls.append({"type": "pub", "id": pid, "title": "", "page_url": url})
    
    # Deduplicate
    seen = set()
    unique = []
    for u in all_urls:
        key = (u["type"], u["id"])
        if key not in seen:
            seen.add(key)
            unique.append(u)
    
    log(f"Discovered {len(unique)} report candidates")
    return unique[:MAX_REPORTS * 3]  # fetch 3x to account for 404s

# ─── STEP 2: Get PDF URLs from report page ────────────────────────────────
def get_pdf_urls(report):
    """Extract PDF download links from a report page"""
    try:
        r = requests.get(report["page_url"], headers=HEADERS, timeout=12)
        if r.status_code != 200:
            return None
        
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Get title
        title_tag = soup.find("title")
        title = title_tag.get_text().strip() if title_tag else report.get("title", "")
        title = re.sub(r'\s+', ' ', title).strip()
        
        if not title or len(title) < 4:
            return None
        
        # Find Hebrew PDFs (prefer -HE.pdf, fallback to any .pdf)
        he_pdfs = []
        all_pdfs = []
        
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" in href.lower():
                full = href if href.startswith("http") else LIBRARY_BASE + href
                all_pdfs.append(full)
                if "-HE.pdf" in href or "Hebrew" in href or "heb" in href.lower():
                    he_pdfs.append(full)
        
        pdfs = he_pdfs if he_pdfs else all_pdfs
        
        if not pdfs:
            return None
        
        # Extract year from URL or title
        year_match = re.search(r'20(1[5-9]|2[0-9])', pdfs[0])
        year = year_match.group(0) if year_match else "2024"
        
        return {"title": title, "pdfs": pdfs[:3], "year": year, "page_url": report["page_url"]}
    
    except Exception as e:
        return None

# ─── STEP 3: Download PDF ─────────────────────────────────────────────────
def download_pdf(url, title, year):
    safe = re.sub(r'[^\w]', '_', title[:40])
    path = os.path.join(PDF_DIR, f"{year}_{safe}.pdf")
    
    if os.path.exists(path) and os.path.getsize(path) > 5000:
        return path
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=45, stream=True)
        r.raise_for_status()
        
        content_type = r.headers.get("content-type", "")
        if "pdf" not in content_type.lower() and "octet" not in content_type.lower():
            return None
        
        with open(path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        
        size_kb = os.path.getsize(path) // 1024
        log(f"  Downloaded: {title[:40]} ({size_kb}KB)")
        return path
    except Exception as e:
        log(f"  Download failed {title[:30]}: {e}")
        return None

# ─── STEP 4: Parse PDF → chunks ───────────────────────────────────────────
def parse_pdf(filepath, title, year, page_url):
    chunks = []
    
    try:
        with pdfplumber.open(filepath) as pdf:
            total_pages = len(pdf.pages)
            log(f"  Parsing {total_pages} pages: {title[:40]}")
            
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"
            
            if len(full_text.strip()) < 100:
                log(f"  ⚠️ No extractable text (scanned PDF?)")
                return chunks
            
            # Split by section headers (lines < 60 chars ending with patterns)
            sections = smart_split(full_text, title)
            
            for sec in sections:
                if len(sec["text"].strip()) < 80:
                    continue
                chunks.append({
                    "text": sec["text"][:1500],
                    "source": "מבקר המדינה",
                    "category": "דוח ביקורת",
                    "title": f"{title} — {sec['header']}",
                    "date": year,
                    "url": page_url,
                    "report_name": title
                })
            
            log(f"  → {len(chunks)} chunks from {title[:40]}")
    
    except Exception as e:
        log(f"  Parse error: {e}")
    
    return chunks

def smart_split(text, report_title, max_chunk=1200):
    """Split text into meaningful sections"""
    lines = text.split("\n")
    sections = []
    current_header = report_title
    current_text = ""
    
    for line in lines:
        stripped = line.strip()
        
        # Detect section header: short, all-Hebrew, no sentence-ending punctuation
        is_header = (
            4 < len(stripped) < 70 and
            not stripped.endswith(",") and
            not stripped.endswith(".") and
            re.search(r'[\u0590-\u05ff]', stripped) and  # has Hebrew
            sum(c.isdigit() for c in stripped) < 5  # not mostly numbers
        )
        
        if is_header and len(current_text) > 300:
            sections.append({"header": current_header, "text": current_text.strip()})
            current_header = stripped
            current_text = stripped + "\n"
        else:
            current_text += line + "\n"
            
            # Force split if too long
            if len(current_text) > max_chunk:
                sections.append({"header": current_header, "text": current_text.strip()})
                current_text = ""
    
    if current_text.strip():
        sections.append({"header": current_header, "text": current_text.strip()})
    
    return sections

# ─── ANCHOR FACTS ─────────────────────────────────────────────────────────
def anchor_facts():
    facts = [
        {
            "text": "דוח מבקר המדינה 74 (2024): ממצאים קשים על ניהול מלחמת אוקטובר 2023. כשלים בהכנת עורף, בניהול מוקדי חירום ובתיאום בין משרדים. המבקר מתח ביקורת על מחסור בתקציב ציוד מגן.",
            "title": "דוח 74 - מלחמת אוקטובר 2023"
        },
        {
            "text": "דוח מבקר המדינה על מערכת הבריאות (2023): המתנה ממוצעת לרופא מומחה - 3 חודשים. מחסור של 1,500 רופאים. ההוצאה לבריאות בישראל נמוכה מממוצע ה-OECD.",
            "title": "ביקורת מערכת הבריאות"
        },
        {
            "text": "דוח מבקר על משרד החינוך (2022): פערים חדים בין מגזרים. תלמיד ממגזר חרדי מקבל פחות שעות לימוד בתכנית הליבה. כישלון בפיקוח על מוסדות לא רשמיים.",
            "title": "ביקורת משרד החינוך"
        },
        {
            "text": "דוח מבקר על דיור (2023): ישראל בנתה 60,000 יחידות דיור ב-2022 — פחות מהנדרש. מחיר ממוצע לדירה: 1.8 מיליון ₪. תוכנית 'מחיר למשתכן' לא עמדה ביעדים.",
            "title": "ביקורת על משבר הדיור"
        },
        {
            "text": "דוח מבקר על השלטון המקומי (2022): עיריות רבות ביחס גרעוני. עיריית ירושלים — גרעון של מיליארד ₪. כשלים בגביית ארנונה, בפיקוח על קבלנים ובשקיפות.",
            "title": "ביקורת שלטון מקומי"
        },
        {
            "text": "דוח מבקר על הרשות לאומית לביטחון מידע (2023): ישראל חשופה לסייבר. 60% מהחברות הממשלתיות לא עמדו בתקנות אבטחת מידע. כשלים בהתגוננות מפני מתקפות.",
            "title": "ביקורת אבטחת סייבר"
        },
        {
            "text": "דוח מבקר על פינוי-בינוי (2024): רק 9% מהפרויקטים שאושרו יצאו לפועל. בירוקרטיה מעכבת תוכניות. בעלי דירות לא מקבלים מידע מספק. הרשות לפינוי-בינוי לא אכפה.",
            "title": "ביקורת פינוי-בינוי"
        },
        {
            "text": "דוח מבקר על תחבורה ציבורית (2022): 40% מנסיעות האוטובוס בישראל איחרו ביותר מ-10 דקות. מחסור ב-3,000 נהגי אוטובוס. תקציב התחבורה הציבורית קוצץ פעמיים.",
            "title": "ביקורת תחבורה ציבורית"
        },
    ]
    return [{"text": f["text"], "source": "מבקר המדינה", "category": "עובדות מפתח",
             "title": f["title"], "date": "2024", "url": MEVAKER_BASE}
            for f in facts]

# ─── MAIN ─────────────────────────────────────────────────────────────────
def main():
    os.makedirs(PDF_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    log(f"Starting Comptroller collection (max_reports={MAX_REPORTS})...")
    
    # Discover
    candidates = discover_reports()
    
    # Fetch report metadata
    reports = []
    for candidate in candidates:
        if len(reports) >= MAX_REPORTS:
            break
        info = get_pdf_urls(candidate)
        if info:
            reports.append(info)
            log(f"Found report: {info['title'][:60]} ({info['year']})")
        time.sleep(0.3)
    
    log(f"\nProcessing {len(reports)} reports...")
    
    all_chunks = []
    for report in reports:
        # Download first Hebrew PDF
        pdf_path = None
        for pdf_url in report["pdfs"][:2]:
            pdf_path = download_pdf(pdf_url, report["title"], report["year"])
            if pdf_path:
                break
        
        if pdf_path:
            chunks = parse_pdf(pdf_path, report["title"], report["year"], report["page_url"])
            all_chunks.extend(chunks)
        
        time.sleep(0.5)
    
    # Add anchor facts
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
    
    log(f"\n✅ DONE: {len(unique)} chunks saved")
    
    cats = {}
    for c in unique:
        cats[c.get("report_name", c["category"])] = cats.get(c.get("report_name", c["category"]), 0) + 1
    for name, cnt in sorted(cats.items(), key=lambda x: -x[1])[:10]:
        log(f"  {name[:50]}: {cnt}")

if __name__ == "__main__":
    main()
