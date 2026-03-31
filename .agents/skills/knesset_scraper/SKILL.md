---
name: knesset_scraper
description: Collects data from Knesset sources - laws, votes, bills, members via data.gov.il and knesset.gov.il. Saves chunks to /app/rag_system/data/chunks/knesset_chunks.json
argument-hint: [limit=200]
---

This skill collects Israeli Knesset data from:
1. data.gov.il - official open data API (votes, members, bills)
2. knesset.gov.il - scraping public pages
3. Built-in key legislative facts as anchor chunks

Output: JSON file with text chunks ready for embedding.
Each chunk has: text, source, category, title, date, url
