---
name: query_rag
description: Query the vector DB and get an AI answer from the RAG system. Pass a question in Hebrew.
argument-hint: "שאלה בעברית"
---

Queries ChromaDB for relevant chunks, then calls GPT-4o-mini to answer in Hebrew.
Returns: answer + sources used.
