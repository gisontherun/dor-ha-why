"""
Main pipeline - runs all collectors then builds the Vector DB
Run this to initialize/update the system
"""
import os
import sys
import json

# Add project to path
sys.path.insert(0, "/app/rag_system")

from collectors.knesset_collector import collect_all as collect_knesset
from collectors.comptroller_collector import collect_all as collect_comptroller
from collectors.cbs_collector import collect_all as collect_cbs
from embedder.build_vectordb import load_all_chunks, build_vectordb

CHUNKS_DIR = "/app/rag_system/data/chunks"

def save_chunks(chunks, filename):
    os.makedirs(CHUNKS_DIR, exist_ok=True)
    filepath = os.path.join(CHUNKS_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)
    print(f"  💾 Saved {len(chunks)} chunks to {filename}")

def run_pipeline(skip_collection=False):
    if not skip_collection:
        print("="*60)
        print("🏛️  STEP 1: Collecting Knesset data")
        print("="*60)
        knesset_chunks = collect_knesset()
        save_chunks(knesset_chunks, "knesset_chunks.json")
        
        print("\n" + "="*60)
        print("📋 STEP 2: Collecting State Comptroller reports")
        print("="*60)
        comptroller_chunks = collect_comptroller()
        save_chunks(comptroller_chunks, "comptroller_chunks.json")
        
        print("\n" + "="*60)
        print("📊 STEP 3: Collecting CBS (Lamas) data")
        print("="*60)
        cbs_chunks = collect_cbs()
        save_chunks(cbs_chunks, "cbs_chunks.json")
    
    print("\n" + "="*60)
    print("🧠 STEP 4: Building Vector DB")
    print("="*60)
    all_chunks = load_all_chunks()
    collection = build_vectordb(all_chunks)
    
    print("\n" + "="*60)
    print("✅ PIPELINE COMPLETE!")
    print(f"   Total vectors in DB: {collection.count()}")
    print("="*60)

if __name__ == "__main__":
    skip = "--skip-collection" in sys.argv
    run_pipeline(skip_collection=skip)
