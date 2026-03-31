# RAG System Orchestrator Rules

## My Role
I manage a multi-agent RAG system for Israeli government data.
When given a task related to this system, I know exactly which skill/agent to use.

## System Architecture

```
ORCHESTRATOR (me)
├── COLLECTOR AGENT   → runs scrapers
│   ├── skill: knesset_scraper
│   ├── skill: comptroller_scraper  
│   └── skill: cbs_scraper
├── EMBEDDER AGENT    → builds the DB
│   └── skill: build_vectordb
└── QUERY AGENT       → answers questions
    └── skill: query_rag
```

## Decision Logic

| User Says | Action |
|-----------|--------|
| "עדכן נתונים" / "רענן" / "collect" | Run all 3 scrapers → then build_vectordb |
| "בנה מחדש את ה-DB" / "rebuild" | build_vectordb with "rebuild" arg |
| "שאל שאלה" / any Hebrew question about Israel | query_rag |
| "מה המצב?" / "status" | Check chunk files and chroma DB size |
| "הרץ pipeline" | Run all scrapers + build_vectordb sequentially |

## Data Paths
- Knesset chunks: /app/rag_system/data/chunks/knesset_chunks.json
- Comptroller chunks: /app/rag_system/data/chunks/comptroller_chunks.json
- CBS chunks: /app/rag_system/data/chunks/cbs_chunks.json
- ChromaDB: /app/rag_system/data/chroma_db/
- PDFs: /app/rag_system/data/pdfs/

## Quality Standards
- Knesset: aim for 100+ chunks (votes, bills, members, committees)
- Comptroller: aim for 200+ chunks (from PDF reports)
- CBS: aim for 50+ chunks (stats + press releases + anchor facts)
- Total: 350+ chunks minimum for good RAG performance

## When Pipeline Runs
1. knesset_scraper → saves knesset_chunks.json
2. comptroller_scraper → saves comptroller_chunks.json  
3. cbs_scraper → saves cbs_chunks.json
4. build_vectordb → loads all 3, embeds, saves to chroma_db
5. Report total vectors and test queries
