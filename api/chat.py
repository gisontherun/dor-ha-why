"""
Chat API — דור ה-WHY
Vercel Serverless Function (Python runtime)
RAG + GPT-4o in one call
"""
from http.server import BaseHTTPRequestHandler
import json, os, base64, math
import numpy as np
from openai import OpenAI

_CHUNKS = None
_EMBEDDINGS = None

SYSTEM_PROMPT = """אתה "דור ה-WHY" — בוט פוליטי ישראלי שעוזר לאזרחים לבנות עמדה מבוססת עובדות.

## המתודולוגיה שלך — 4 שלבים

**שלב 1 — הגדרת קריטריון:**
שאל את המשתמש באיזו זווית לבחון את האמירה:
→ תקציב בפועל מול הבטחה
→ השוואה היסטורית (לפני/אחרי)
→ השוואה בינלאומית
→ יישום בפועל

**שלב 2 — עובדות רשמיות:**
הצג 3-4 נתונים עם מקורות מלאים. היה ספציפי: מספרים, אחוזים, תאריכים.
גם נתונים שמחזקים וגם שמפריכים.

**שלב 3 — דילמה סוקרטית:**
שתי פרשנויות לגיטימיות לאותם נתונים.
שאל: "איך אתה קורא את זה?"

**שלב 4 — סינתזה:**
עזור למשתמש לנסח שורה תחתונה **משלו** — משפט אחד.

## כללים קריטיים
- מקורות רשמיים בלבד (כנסת, מבקר, למ"ס, data.gov.il, מפתח התקציב, CECI)
- אל תסיק מסקנות — הנחה
- ניטרלי לחלוטין — לא ימין, לא שמאל
- אם אין נתון מדויק: "לא מצאתי נתון רשמי לכך"
- עברית בלבד, סגנון חכם ולקוני"""


def _load():
    global _CHUNKS, _EMBEDDINGS
    if _CHUNKS is not None:
        return _CHUNKS, _EMBEDDINGS
    data_path = os.path.join(os.path.dirname(__file__), "..", "data", "chunks.json")
    with open(data_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    chunks, embeddings = [], []
    for item in raw:
        emb = np.frombuffer(base64.b64decode(item["emb"]), dtype=np.float32)
        embeddings.append(emb)
        chunks.append({k: item[k] for k in ("text","source","title","date","url")})
    _CHUNKS = chunks
    _EMBEDDINGS = np.stack(embeddings)
    return _CHUNKS, _EMBEDDINGS


def cosine_sim(q, matrix):
    q_n = q / (np.linalg.norm(q) + 1e-9)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True) + 1e-9
    return (matrix / norms) @ q_n


class handler(BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        self._json({"status": "ok", "service": "chat"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = json.loads(self.rfile.read(length) or b"{}")
        message = body.get("message", "").strip()
        history = body.get("history", [])

        if not message:
            return self._json({"error": "message required"}, 400)

        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            return self._json({"error": "OPENAI_API_KEY not set"}, 500)

        try:
            client = OpenAI(api_key=api_key)
            chunks, embeddings = _load()

            # ── RAG: embed query ──
            emb_resp = client.embeddings.create(
                model="text-embedding-3-small", input=[message]
            )
            q_vec = np.array(emb_resp.data[0].embedding, dtype=np.float32)

            # ── RAG: top-5 chunks ──
            scores = cosine_sim(q_vec, embeddings)
            top_idx = np.argsort(scores)[::-1][:5]

            context_parts, sources = [], []
            for idx in top_idx:
                score = float(scores[idx])
                if score < 0.3:
                    continue
                c = chunks[idx]
                context_parts.append(f"[{c['source']}]\n{c['text']}")
                sources.append({"source": c["source"], "title": c["title"],
                                 "score": round(score, 3), "url": c["url"]})

            context_block = ""
            if context_parts:
                context_block = "\n\n## נתונים רלוונטיים ממאגר המקורות:\n"
                context_block += "\n\n---\n".join(context_parts)
                context_block += "\n\n---\nהשתמש בנתונים אלו בתשובתך. ציין מקורות בצורה מדויקת."

            # ── GPT-4o ──
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT + context_block},
                *[m for m in history[-8:] if m.get("role") in ("user","assistant")],
                {"role": "user", "content": message},
            ]

            completion = client.chat.completions.create(
                model="gpt-4o", messages=messages,
                temperature=0.3, max_tokens=1200
            )

            self._json({
                "reply":  completion.choices[0].message.content,
                "sources": sources,
                "rag_chunks": len(sources),
            })

        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args): pass
