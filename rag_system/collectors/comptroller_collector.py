"""
State Comptroller Collector - downloads and parses ALL available PDF reports
from mevaker.gov.il
"""
import requests
from bs4 import BeautifulSoup
import pdfplumber
import json
import os
import time
import re
import urllib.request

BASE_URL = "https://www.mevaker.gov.il"
REPORTS_URL = f"{BASE_URL}/he/Reports/Pages/default.aspx"
PDF_DIR = "/app/rag_system/data/pdfs"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def get_report_links():
    """Scrape all available report PDFs from the comptroller site"""
    links = []
    
    # Known report URLs pattern - try multiple pages
    report_pages = [
        "https://www.mevaker.gov.il/he/Reports/Pages/default.aspx",
        "https://www.mevaker.gov.il/he/Reports/Report/Pages/default.aspx",
    ]
    
    # Also try direct known annual reports
    known_reports = [
        {
            "url": "https://www.mevaker.gov.il/he/Reports/Report/582/Hebrew/0.pdf",
            "title": "דוח מבקר המדינה 74א - 2024",
            "year": "2024"
        },
        {
            "url": "https://www.mevaker.gov.il/he/Reports/Report/583/Hebrew/0.pdf", 
            "title": "דוח מבקר המדינה 74ב - 2024",
            "year": "2024"
        },
        {
            "url": "https://www.mevaker.gov.il/he/Reports/Report/577/Hebrew/0.pdf",
            "title": "דוח מבקר המדינה 73ב - 2023",
            "year": "2023"
        },
        {
            "url": "https://www.mevaker.gov.il/he/Reports/Report/572/Hebrew/0.pdf",
            "title": "דוח מבקר המדינה 73א - 2023",
            "year": "2023"
        },
        {
            "url": "https://www.mevaker.gov.il/he/Reports/Report/564/Hebrew/0.pdf",
            "title": "דוח מבקר המדינה 72ב - 2022",
            "year": "2022"
        },
        {
            "url": "https://www.mevaker.gov.il/he/Reports/Report/558/Hebrew/0.pdf",
            "title": "דוח מבקר המדינה 72א - 2022",
            "year": "2022"
        },
        {
            "url": "https://www.mevaker.gov.il/he/Reports/Report/551/Hebrew/0.pdf",
            "title": "דוח מבקר המדינה 71ב - 2021",
            "year": "2021"
        },
        {
            "url": "https://www.mevaker.gov.il/he/Reports/Report/543/Hebrew/0.pdf",
            "title": "דוח מבקר המדינה 71א - 2021",
            "year": "2021"
        },
        {
            "url": "https://www.mevaker.gov.il/he/Reports/Report/535/Hebrew/0.pdf",
            "title": "דוח מבקר המדינה 70ב - 2020",
            "year": "2020"
        },
        {
            "url": "https://www.mevaker.gov.il/he/Reports/Report/527/Hebrew/0.pdf",
            "title": "דוח מבקר המדינה 70א - 2020",
            "year": "2020"
        },
    ]
    
    # Try scraping the site first
    try:
        r = requests.get(REPORTS_URL, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" in href.lower() or "report" in href.lower():
                full_url = href if href.startswith("http") else BASE_URL + href
                title = a.get_text(strip=True) or "דוח מבקר המדינה"
                if title and len(title) > 3:
                    links.append({"url": full_url, "title": title, "year": "unknown"})
    except Exception as e:
        print(f"Scraping failed, using known reports: {e}")
    
    # Always add known reports
    links.extend(known_reports)
    
    # Deduplicate
    seen = set()
    unique = []
    for l in links:
        if l["url"] not in seen:
            seen.add(l["url"])
            unique.append(l)
    
    print(f"Found {len(unique)} report links")
    return unique

def download_pdf(url, title, year):
    """Download a PDF and return local path"""
    safe_title = re.sub(r'[^\w\u0590-\u05ff]', '_', title)[:50]
    filename = f"{year}_{safe_title}.pdf"
    filepath = os.path.join(PDF_DIR, filename)
    
    if os.path.exists(filepath):
        print(f"  Already exists: {filename}")
        return filepath
    
    try:
        print(f"  Downloading: {title[:60]}...")
        r = requests.get(url, headers=HEADERS, timeout=30, stream=True)
        r.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"  ✅ Saved: {filename}")
        return filepath
    except Exception as e:
        print(f"  ❌ Failed: {e}")
        return None

def parse_pdf_to_chunks(filepath, title, year, chunk_size=800, overlap=100):
    """Parse PDF and split into overlapping chunks"""
    chunks = []
    
    try:
        with pdfplumber.open(filepath) as pdf:
            full_text = ""
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                if text.strip():
                    full_text += f"\n[עמוד {page_num+1}]\n{text}"
            
            if not full_text.strip():
                print(f"  ⚠️ No text extracted from {title}")
                return chunks
            
            # Split into sections by headers/topics
            sections = split_into_sections(full_text)
            
            for section in sections:
                # Further chunk if too long
                if len(section["text"]) > chunk_size * 4:
                    sub_chunks = split_text(section["text"], chunk_size, overlap)
                    for i, sub in enumerate(sub_chunks):
                        chunks.append({
                            "text": sub,
                            "source": "מבקר המדינה",
                            "category": "דוח ביקורת",
                            "title": f"{title} - {section['section_title']} (חלק {i+1})",
                            "date": year,
                            "url": f"https://www.mevaker.gov.il",
                            "report_name": title
                        })
                else:
                    chunks.append({
                        "text": section["text"],
                        "source": "מבקר המדינה",
                        "category": "דוח ביקורת",
                        "title": f"{title} - {section['section_title']}",
                        "date": year,
                        "url": "https://www.mevaker.gov.il",
                        "report_name": title
                    })
    except Exception as e:
        print(f"  ❌ Parse error: {e}")
    
    return chunks

def split_into_sections(text):
    """Split text by detected section headers"""
    sections = []
    lines = text.split('\n')
    current_section = {"section_title": "כללי", "text": ""}
    
    for line in lines:
        stripped = line.strip()
        # Detect headers (short lines, often bold or ALL CAPS in Hebrew reports)
        is_header = (
            len(stripped) > 3 and 
            len(stripped) < 80 and 
            (stripped.endswith(":") or 
             re.match(r'^[א-ת\s]{5,40}$', stripped) or
             re.match(r'^\d+\.\s', stripped))
        )
        
        if is_header and len(current_section["text"]) > 200:
            sections.append(current_section)
            current_section = {"section_title": stripped, "text": stripped + "\n"}
        else:
            current_section["text"] += line + "\n"
    
    if current_section["text"].strip():
        sections.append(current_section)
    
    return sections if sections else [{"section_title": "כללי", "text": text}]

def split_text(text, chunk_size=800, overlap=100):
    """Split text into overlapping chunks"""
    words = text.split()
    chunks = []
    i = 0
    while i < len(words):
        chunk = " ".join(words[i:i+chunk_size])
        chunks.append(chunk)
        i += chunk_size - overlap
    return chunks

def collect_all():
    os.makedirs(PDF_DIR, exist_ok=True)
    all_chunks = []
    
    report_links = get_report_links()
    
    for report in report_links:
        print(f"\nProcessing: {report['title'][:60]}")
        pdf_path = download_pdf(report["url"], report["title"], report["year"])
        
        if pdf_path:
            chunks = parse_pdf_to_chunks(pdf_path, report["title"], report["year"])
            all_chunks.extend(chunks)
            print(f"  📝 Generated {len(chunks)} chunks from {report['title'][:40]}")
        
        time.sleep(1)  # Be polite
    
    print(f"\n📦 Total Comptroller chunks: {len(all_chunks)}")
    return all_chunks

if __name__ == "__main__":
    chunks = collect_all()
    with open("/app/rag_system/data/chunks/comptroller_chunks.json", "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)
    print("Saved to comptroller_chunks.json")
