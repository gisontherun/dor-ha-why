"""
Chat API — דור ה-WHY
Vercel Serverless Function (Python runtime)
RAG via /api/query + GPT-4o
"""
from http.server import BaseHTTPRequestHandler
import json, os, urllib.request, urllib.error

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


def call_query_api(message, host):
    """Call the /api/query endpoint on the same Vercel deployment."""
    payload = json.dumps({"query": message, "n_results": 5}).encode()
    req = urllib.request.Request(
        f"{host}/api/query",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        return {"context": "", "sources": [], "error": str(e)}


def call_openai(api_key, messages):
    """Call OpenAI chat completions via raw HTTP (no extra deps)."""
    payload = json.dumps({
        "model": "gpt-4o",
        "messages": messages,
        "temperature": 0.3,
        "max_tokens": 1200
    }).encode()
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        },
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


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
            # ── Step 1: RAG retrieval via /api/query ──
            host = os.environ.get("VERCEL_URL", "")
            if host:
                host = f"https://{host}"
            else:
                host = "https://dor-ha-why-rag.vercel.app"

            rag_data = call_query_api(message, host)
            context  = rag_data.get("context", "")
            sources  = rag_data.get("sources", [])

            # ── Step 2: Build prompt ──
            context_block = ""
            if context:
                context_block = (
                    "\n\n## נתונים רלוונטיים ממאגר המקורות:\n"
                    + context
                    + "\n\n---\nהשתמש בנתונים אלו בתשובתך. ציין מקורות בצורה מדויקת."
                )

            messages = [
                {"role": "system", "content": SYSTEM_PROMPT + context_block},
                *[m for m in history[-8:] if m.get("role") in ("user", "assistant")],
                {"role": "user", "content": message},
            ]

            # ── Step 3: GPT-4o ──
            completion = call_openai(api_key, messages)
            reply = completion["choices"][0]["message"]["content"]

            self._json({
                "reply":      reply,
                "sources":    sources,
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

    def log_message(self, *args):
        pass
