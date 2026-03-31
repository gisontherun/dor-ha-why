#!/usr/bin/env python3
"""
QUERY RAG SKILL
Searches vector DB and returns GPT-4o-mini answer
"""
import json
import os
import sys
import chromadb
from openai import OpenAI

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
CHROMA_PATH    = "/app/rag_system/data/chroma_db"
COLLECTION     = "israeli_gov_data"
TOP_K          = 6

question = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""

client = OpenAI(api_key=OPENAI_API_KEY)

def log(msg): print(msg, flush=True)

SYSTEM_PROMPT = """אתה עוזר מחקרי ישראלי המתמחה בניתוח נתונים ממשלתיים.
אתה עונה אך ורק על בסיס המקורות שסופקו לך.

כללים:
1. ענה תמיד בעברית
2. ציין את המקור לכל עובדה (בסוגריים)
3. אם המידע לא קיים במקורות — אמור זאת בפירוש
4. היה מדויק ועובדתי
5. בסוף — ציין את רשימת המקורות ששימשת"""

def main():
    if not question:
        log("Usage: python run.py 'שאלה בעברית'")
        sys.exit(1)
    
    if not OPENAI_API_KEY:
        log("❌ OPENAI_API_KEY not set")
        sys.exit(1)
    
    # ── Embed question ──
    log(f"\n🔍 שאלה: {question}\n")
    resp = client.embeddings.create(model="text-embedding-3-small", input=[question])
    q_emb = resp.data[0].embedding
    
    # ── Query Chroma ──
    try:
        chroma = chromadb.PersistentClient(path=CHROMA_PATH)
        col = chroma.get_collection(COLLECTION)
        
        results = col.query(
            query_embeddings=[q_emb],
            n_results=TOP_K,
            include=["documents", "metadatas", "distances"]
        )
        
        docs   = results["documents"][0]
        metas  = results["metadatas"][0]
        dists  = results["distances"][0]
        
        # Filter by relevance
        sources = [
            {"text": d, "meta": m, "score": round(1-dist, 3)}
            for d, m, dist in zip(docs, metas, dists)
            if 1 - dist > 0.25
        ]
        
        if not sources:
            log("⚠️  לא נמצאו מקורות רלוונטיים. ייתכן שהמאגר לא נטען.")
            sys.exit(0)
        
        log(f"נמצאו {len(sources)} מקורות רלוונטיים:")
        for i, s in enumerate(sources):
            log(f"  [{s['score']}] {s['meta']['source']} | {s['meta']['title'][:50]}")
        
    except Exception as e:
        log(f"❌ Chroma error: {e}")
        log("הרץ את build_vectordb תחילה.")
        sys.exit(1)
    
    # ── Build context ──
    context = "\n\n---\n\n".join([
        f"[מקור {i+1}] {s['meta']['source']} | {s['meta']['category']} | {s['meta']['title']}\n{s['text']}"
        for i, s in enumerate(sources)
    ])
    
    # ── GPT answer ──
    log("\n💬 מייצר תשובה...\n")
    
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"מקורות:\n\n{context}\n\n---\nשאלה: {question}"}
        ],
        temperature=0.2,
        max_tokens=1000
    )
    
    answer = completion.choices[0].message.content
    
    log("=" * 60)
    log(answer)
    log("=" * 60)
    
    log("\n📚 מקורות ששימשו:")
    seen_sources = set()
    for s in sources:
        src = s['meta']['source']
        if src not in seen_sources:
            log(f"  • {src}")
            seen_sources.add(src)

if __name__ == "__main__":
    main()
