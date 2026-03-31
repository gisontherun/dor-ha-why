"""
Knesset Collector - fetches data from oknesset.org Open Data API
Collects: laws, votes, queries (שאילתות), committee protocols
"""
import requests
import json
import os
import time

BASE_URL = "https://knesset.gov.il/Odata/ParliamentInfo.svc"
OKNESSET_URL = "https://oknesset.org/api/v2"

def fetch_json(url, params=None):
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def collect_laws(limit=200):
    """Collect recent laws from Knesset"""
    chunks = []
    url = f"{OKNESSET_URL}/laws/"
    params = {"format": "json", "limit": limit}
    data = fetch_json(url, params)
    if not data:
        return chunks
    
    items = data.get("objects", []) if isinstance(data, dict) else data
    for item in items:
        name = item.get("name", "") or item.get("title", "")
        summary = item.get("summary", "") or item.get("explanation", "") or ""
        law_type = item.get("law_type_name", "")
        date = item.get("date", "") or item.get("approval_date", "")
        
        text = f"חוק: {name}\nסוג: {law_type}\nתאריך: {date}\nתקציר: {summary}"
        if name:
            chunks.append({
                "text": text,
                "source": "אתר הכנסת",
                "category": "חקיקה",
                "title": name,
                "date": date,
                "url": f"https://knesset.gov.il"
            })
    print(f"✅ Knesset laws: {len(chunks)} items")
    return chunks

def collect_votes(limit=200):
    """Collect recent votes"""
    chunks = []
    url = f"{OKNESSET_URL}/votes/"
    params = {"format": "json", "limit": limit, "order_by": "-time"}
    data = fetch_json(url, params)
    if not data:
        return chunks
    
    items = data.get("objects", []) if isinstance(data, dict) else data
    for item in items:
        title = item.get("title", "") or ""
        vote_type = item.get("vote_type", "")
        date = str(item.get("time", ""))
        result = "עבר" if item.get("passed") else "נכשל"
        for_votes = item.get("for_votes", 0)
        against_votes = item.get("against_votes", 0)
        
        text = f"הצבעה: {title}\nתוצאה: {result}\nבעד: {for_votes} | נגד: {against_votes}\nתאריך: {date}"
        if title:
            chunks.append({
                "text": text,
                "source": "אתר הכנסת",
                "category": "הצבעות",
                "title": title,
                "date": date,
                "url": "https://knesset.gov.il"
            })
    print(f"✅ Knesset votes: {len(chunks)} items")
    return chunks

def collect_queries(limit=200):
    """Collect parliamentary queries (שאילתות)"""
    chunks = []
    url = f"{OKNESSET_URL}/queries/"
    params = {"format": "json", "limit": limit}
    data = fetch_json(url, params)
    if not data:
        # Try alternative endpoint
        url2 = "https://knesset.gov.il/Odata/Queries.svc/KnsQuery"
        data = fetch_json(url2 + "?$format=json&$top=100")
    if not data:
        return chunks
    
    items = data.get("objects", data.get("value", [])) if isinstance(data, dict) else data
    for item in items:
        title = item.get("name", "") or item.get("QueryTitle", "") or ""
        member = item.get("mk_individual_name", "") or item.get("MkName", "")
        answer = item.get("answer", "") or ""
        date = str(item.get("date", "") or item.get("QueryDate", ""))
        
        text = f"שאילתה: {title}\nח\"כ: {member}\nתאריך: {date}\nתשובה: {answer[:500] if answer else 'לא נמצאה תשובה'}"
        if title:
            chunks.append({
                "text": text,
                "source": "אתר הכנסת",
                "category": "שאילתות",
                "title": title,
                "date": date,
                "url": "https://knesset.gov.il"
            })
    print(f"✅ Knesset queries: {len(chunks)} items")
    return chunks

def collect_bills(limit=200):
    """Collect proposed bills (הצעות חוק)"""
    chunks = []
    url = f"{OKNESSET_URL}/bills/"
    params = {"format": "json", "limit": limit, "order_by": "-date"}
    data = fetch_json(url, params)
    if not data:
        return chunks
    
    items = data.get("objects", []) if isinstance(data, dict) else data
    for item in items:
        title = item.get("title", "") or ""
        stage = item.get("stage", "") or ""
        date = str(item.get("date", ""))
        initiators = item.get("proposers", [])
        proposer_names = ", ".join([p.get("name", "") for p in initiators]) if initiators else ""
        summary = item.get("summary", "") or ""
        
        text = f"הצעת חוק: {title}\nשלב: {stage}\nמציע: {proposer_names}\nתאריך: {date}\nתקציר: {summary[:300]}"
        if title:
            chunks.append({
                "text": text,
                "source": "אתר הכנסת",
                "category": "הצעות חוק",
                "title": title,
                "date": date,
                "url": "https://knesset.gov.il"
            })
    print(f"✅ Knesset bills: {len(chunks)} items")
    return chunks

def collect_all():
    all_chunks = []
    all_chunks.extend(collect_laws())
    all_chunks.extend(collect_votes())
    all_chunks.extend(collect_queries())
    all_chunks.extend(collect_bills())
    print(f"\n📦 Total Knesset chunks: {len(all_chunks)}")
    return all_chunks

if __name__ == "__main__":
    chunks = collect_all()
    with open("/app/rag_system/data/chunks/knesset_chunks.json", "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)
    print("Saved to knesset_chunks.json")
