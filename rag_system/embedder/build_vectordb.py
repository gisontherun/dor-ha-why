"""
Vector DB Builder - embeds all chunks and stores in ChromaDB
Uses OpenAI text-embedding-3-small (cheap & fast)
"""
import json
import os
import sys
import chromadb
from chromadb.utils import embedding_functions
from openai import OpenAI
import time

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
CHROMA_PATH = "/app/rag_system/data/chroma_db"
CHUNKS_DIR = "/app/rag_system/data/chunks"

client = OpenAI(api_key=OPENAI_API_KEY)

def load_all_chunks():
    """Load all chunk JSON files"""
    all_chunks = []
    chunk_files = [
        "knesset_chunks.json",
        "comptroller_chunks.json", 
        "cbs_chunks.json"
    ]
    
    for filename in chunk_files:
        filepath = os.path.join(CHUNKS_DIR, filename)
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                chunks = json.load(f)
                all_chunks.extend(chunks)
                print(f"  Loaded {len(chunks)} chunks from {filename}")
        else:
            print(f"  ⚠️ Missing: {filename}")
    
    return all_chunks

def embed_texts_batch(texts, batch_size=100):
    """Embed texts in batches to avoid rate limits"""
    all_embeddings = []
    
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        print(f"  Embedding batch {i//batch_size + 1}/{(len(texts)-1)//batch_size + 1} ({len(batch)} texts)...")
        
        try:
            response = client.embeddings.create(
                model="text-embedding-3-small",
                input=batch
            )
            embeddings = [item.embedding for item in response.data]
            all_embeddings.extend(embeddings)
            time.sleep(0.1)  # Rate limit buffer
        except Exception as e:
            print(f"  ❌ Embedding error: {e}")
            # Add zero vectors as fallback
            all_embeddings.extend([[0.0] * 1536] * len(batch))
    
    return all_embeddings

def build_vectordb(chunks):
    """Build ChromaDB from chunks"""
    os.makedirs(CHROMA_PATH, exist_ok=True)
    
    # Initialize ChromaDB
    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
    
    # Delete existing collection if rebuilding
    try:
        chroma_client.delete_collection("israeli_gov_data")
        print("  Deleted existing collection")
    except:
        pass
    
    # Create new collection
    collection = chroma_client.create_collection(
        name="israeli_gov_data",
        metadata={"hnsw:space": "cosine"}
    )
    
    # Filter out empty chunks
    valid_chunks = [c for c in chunks if c.get("text") and len(c["text"].strip()) > 20]
    print(f"\n📝 Embedding {len(valid_chunks)} valid chunks...")
    
    texts = [c["text"] for c in valid_chunks]
    embeddings = embed_texts_batch(texts)
    
    # Prepare metadata and IDs
    ids = [f"chunk_{i}" for i in range(len(valid_chunks))]
    metadatas = []
    for c in valid_chunks:
        metadatas.append({
            "source": str(c.get("source", "")),
            "category": str(c.get("category", "")),
            "title": str(c.get("title", ""))[:200],
            "date": str(c.get("date", "")),
            "url": str(c.get("url", ""))[:500],
            "report_name": str(c.get("report_name", ""))[:200],
        })
    
    # Add in batches
    batch_size = 200
    for i in range(0, len(valid_chunks), batch_size):
        collection.add(
            ids=ids[i:i+batch_size],
            embeddings=embeddings[i:i+batch_size],
            documents=texts[i:i+batch_size],
            metadatas=metadatas[i:i+batch_size]
        )
        print(f"  Added batch {i//batch_size + 1} to ChromaDB")
    
    print(f"\n✅ Vector DB built! Total: {collection.count()} vectors")
    return collection

def query_test(collection, question):
    """Test a query"""
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=[question]
    )
    query_embedding = response.data[0].embedding
    
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=5,
        include=["documents", "metadatas", "distances"]
    )
    
    print(f"\n🔍 Query: {question}")
    for i, (doc, meta, dist) in enumerate(zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0]
    )):
        print(f"\n[{i+1}] Score: {1-dist:.3f} | Source: {meta['source']} | {meta['title'][:50]}")
        print(f"  {doc[:150]}...")

if __name__ == "__main__":
    print("Loading chunks...")
    chunks = load_all_chunks()
    print(f"Total chunks: {len(chunks)}")
    
    print("\nBuilding Vector DB...")
    collection = build_vectordb(chunks)
    
    # Test queries
    query_test(collection, "כמה ח\"כים הצביעו בעד הצעת החוק?")
    query_test(collection, "ממצאי מבקר המדינה על משרד הביטחון")
    query_test(collection, "שיעור האבטלה בישראל")
