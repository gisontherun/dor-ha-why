---
name: build_vectordb
description: Embeds all collected chunks and builds ChromaDB vector database. Reads from /app/rag_system/data/chunks/*.json and writes to /app/rag_system/data/chroma_db/
argument-hint: [rebuild=false]
---

Loads all chunk files, embeds with OpenAI text-embedding-3-small, stores in ChromaDB.
Pass "rebuild" to force rebuild even if DB exists.
Prints stats on completion.
