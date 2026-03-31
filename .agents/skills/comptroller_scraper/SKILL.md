---
name: comptroller_scraper
description: Downloads and parses State Comptroller (מבקר המדינה) PDF reports from library.mevaker.gov.il. Saves chunks to /app/rag_system/data/chunks/comptroller_chunks.json
argument-hint: [max_reports=20]
---

Scrapes library.mevaker.gov.il for all available PDF reports.
Downloads Hebrew PDFs, parses them with pdfplumber, splits into chunks.
Each chunk preserves report title, year, and topic section.
