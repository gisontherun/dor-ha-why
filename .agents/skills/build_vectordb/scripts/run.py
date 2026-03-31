#!/usr/bin/env python3
"""
BUILD VECTOR DB SKILL
Embeds all chunks and builds/updates ChromaDB
"""
import json
import os
import sys
import time
import chromadb
from openai import OpenAI

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
CHROMA_PATH    = "/app/rag_system/data/chroma_db"
CHUNKS_DIR     = "/app/rag_system/data/chunks"
REBUILD        = len(sys.argv) > 1 and sys.argv[1].lower() in ("rebuild", "true", "1")
COLLECTION     = "israeli_gov_data"

client = OpenAI(api_key=OPENAI_API_KEY)

def log(msg): print(f"[VECTORDB] {msg}", flush=True)

# ─── Load chunks ──────────────────────────────────────────────────────────
def load_chunks():
    all_chunks = []
    files = ["knesset_chunks.json", "comptroller_chunks.json", "cbs_chunks.json"]
    
    for fname in files:
        path = os.path.join(CHUNKS_DIR, fname)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                chunks = json.load(f)
            all_chunks.extend(chunks)
            log(f"  Loaded {len(chunks):,} chunks ← {fname}")
        else:
            log(f"  ⚠️  Missing: {fname}")
    
    return all_chunks

# ─── Embed in batches ─────────────────────────────────────────────────────
def embed_batch(texts, batch_size=100):
    embeddings = []
    total = len(texts)
    
    for i in range(0, total, batch_size):
        batch = texts[i:i+batch_size]
        pct = (i / total) * 100
        log(f"  Embedding {i+len(batch)}/{total} ({pct:.0f}%)...")
        
        try:
            resp = client.embeddings.create(
                model="text-embedding-3-small",
                input=batch
            )
            embeddings.extend([item.embedding for item in resp.data])
        except Exception as e:
            log(f"  ❌ Embed error: {e}")
            embeddings.extend([[0.0] * 1536] * len(batch))
        
        time.sleep(0.05)
    
    return embeddings

# ─── Build ChromaDB ───────────────────────────────────────────────────────
def build_db(chunks):
    os.makedirs(CHROMA_PATH, exist_ok=True)
    chroma = chromadb.PersistentClient(path=CHROMA_PATH)
    
    if REBUILD:
        try:
            chroma.delete_collection(COLLECTION)
            log("Deleted existing collection (rebuild mode)")
        except:
            pass
    
    # Check if already built
    try:
        col = chroma.get_collection(COLLECTION)
        existing = col.count()
        if not REBUILD and existing > 0:
            log(f"Collection already exists with {existing:,} vectors. Pass 'rebuild' to recreate.")
            return col
    except:
        pass
    
    col = chroma.get_or_create_collection(
        name=COLLECTION,
        metadata={"hnsw:space": "cosine", "description": "Israeli gov data RAG"}
    )
    
    # Filter valid chunks
    valid = [c for c in chunks if c.get("text", "").strip() and len(c["text"].strip()) > 30]
    log(f"\nEmbedding {len(valid):,} valid chunks...")
    
    texts      = [c["text"] for c in valid]
    embeddings = embed_batch(texts)
    
    ids = [f"chunk_{i}" for i in range(len(valid))]
    metas = [{
        "source":      str(c.get("source", ""))[:100],
        "category":    str(c.get("category", ""))[:100],
        "title":       str(c.get("title", ""))[:200],
        "date":        str(c.get("date", ""))[:20],
        "url":         str(c.get("url", ""))[:300],
        "report_name": str(c.get("report_name", ""))[:200],
    } for c in valid]
    
    # Upsert in batches
    batch_size = 200
    for i in range(0, len(valid), batch_size):
        col.upsert(
            ids=ids[i:i+batch_size],
            embeddings=embeddings[i:i+batch_size],
            documents=texts[i:i+batch_size],
            metadatas=metas[i:i+batch_size]
        )
        log(f"  Upserted batch {i//batch_size + 1}/{(len(valid)-1)//batch_size + 1}")
    
    return col

# ─── Test query ───────────────────────────────────────────────────────────
def test_query(col, question):
    resp = client.embeddings.create(model="text-embedding-3-small", input=[question])
    q_emb = resp.data[0].embedding
    
    results = col.query(
        query_embeddings=[q_emb],
        n_results=3,
        include=["documents", "metadatas", "distances"]
    )
    
    log(f"\n🔍 Test: '{question}'")
    for doc, meta, dist in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0]
    ):
        score = 1 - dist
        log(f"  [{score:.3f}] {meta['source']} | {meta['title'][:50]}")
        log(f"       {doc[:100]}...")

# ─── MAIN ─────────────────────────────────────────────────────────────────
def main():
    if not OPENAI_API_KEY:
        log("❌ OPENAI_API_KEY not set!")
        sys.exit(1)
    
    log("Loading chunks...")
    chunks = load_chunks()
    log(f"Total: {len(chunks):,} chunks")
    
    if not chunks:
        log("❌ No chunks found. Run the scrapers first.")
        sys.exit(1)
    
    log("Building vector DB...")
    col = build_db(chunks)
    
    total = col.count()
    log(f"\n✅ Vector DB ready: {total:,} vectors in '{COLLECTION}'")
    
    # Stats by source
    sample = col.get(limit=total, include=["metadatas"])
    sources = {}
    for m in sample["metadatas"]:
        src = m.get("source", "unknown")
        sources[src] = sources.get(src, 0) + 1
    log("Distribution by source:")
    for src, cnt in sorted(sources.items(), key=lambda x: -x[1]):
        pct = cnt / total * 100
        log(f"  {src[:40]}: {cnt:,} ({pct:.1f}%)")
    
    # Run test queries
    log("\nRunning test queries...")
    test_query(col, "מה אמר מבקר המדינה על מערכת הבריאות?")
    test_query(col, "שיעור האבטלה בישראל")
    test_query(col, "הצבעות כנסת על חוק הגיוס")

if __name__ == "__main__":
    main()
