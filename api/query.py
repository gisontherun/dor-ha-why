"""
RAG Query API — דור ה-WHY
Vercel Serverless Function (Python runtime)
"""
from http.server import BaseHTTPRequestHandler
import json, os, base64, math
import numpy as np
from openai import OpenAI

# ── Load chunks once (module-level caching) ──────────────────────────────
_CHUNKS = None
_EMBEDDINGS = None

def _load():
    global _CHUNKS, _EMBEDDINGS
    if _CHUNKS is not None:
        return _CHUNKS, _EMBEDDINGS

    data_path = os.path.join(os.path.dirname(__file__), "..", "data", "chunks.json")
    with open(data_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    chunks = []
    embeddings = []
    for item in raw:
        emb_bytes = base64.b64decode(item["emb"])
        emb = np.frombuffer(emb_bytes, dtype=np.float32)
        embeddings.append(emb)
        chunks.append({
            "text":     item["text"],
            "source":   item["source"],
            "category": item["category"],
            "title":    item["title"],
            "date":     item["date"],
            "url":      item["url"],
        })

    _CHUNKS = chunks
    _EMBEDDINGS = np.stack(embeddings)  # shape: (N, 1536)
    return _CHUNKS, _EMBEDDINGS


def cosine_similarity(q_vec, matrix):
    """Vectorised cosine similarity between query and all chunks."""
    q_norm = q_vec / (np.linalg.norm(q_vec) + 1e-9)
    norms  = np.linalg.norm(matrix, axis=1, keepdims=True) + 1e-9
    return (matrix / norms) @ q_norm


class handler(BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        chunks, embeddings = _load()
        self._json({"status": "ok", "vectors": len(chunks)})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = json.loads(self.rfile.read(length) or b"{}")
        query  = body.get("query", "")
        n      = min(int(body.get("n_results", 5)), 10)

        if not query:
            return self._json({"error": "query required"}, 400)

        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            return self._json({"error": "OPENAI_API_KEY not set"}, 500)

        try:
            client = OpenAI(api_key=api_key)
            chunks, embeddings = _load()

            # Embed query
            resp = client.embeddings.create(
                model="text-embedding-3-small",
                input=[query]
            )
            q_vec = np.array(resp.data[0].embedding, dtype=np.float32)

            # Find top-n
            scores = cosine_similarity(q_vec, embeddings)
            top_idx = np.argsort(scores)[::-1][:n]

            context_parts = []
            sources = []
            for idx in top_idx:
                score = float(scores[idx])
                if score < 0.3:
                    continue
                c = chunks[idx]
                context_parts.append(f"[{c['source']}]\n{c['text']}")
                sources.append({
                    "source": c["source"],
                    "title":  c["title"],
                    "score":  round(score, 3),
                    "url":    c["url"],
                })

            self._json({
                "context": "\n\n---\n".join(context_parts),
                "sources": sources,
                "chunks_found": len(context_parts),
            })

        except Exception as e:
            self._json({"error": str(e), "context": "", "sources": []}, 500)

    # ── helpers ──────────────────────────────────────────────────────────
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
        pass  # silence default logs
