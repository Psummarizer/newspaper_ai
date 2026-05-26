"""
Diagnose coverage problems: inspect topics.json + articles.json from GCS
to understand why a topic has low pool.

Usage:
    python scripts/diagnose_coverage.py --topic "Política española"
    python scripts/diagnose_coverage.py --user jcgarcia2066@gmail.com
"""
import sys
import os
import json
import argparse
import tempfile
import unicodedata
from datetime import datetime, timedelta
from collections import Counter

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

# Set up Application Default Credentials from FIREBASE_CREDENTIALS_JSON so GCS works locally
if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
    creds_json = os.getenv("FIREBASE_CREDENTIALS_JSON")
    if creds_json:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
        tmp.write(creds_json)
        tmp.flush()
        tmp.close()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name

from src.services.gcs_service import GCSService


def _normalize_id(topic: str) -> str:
    s = unicodedata.normalize("NFKD", topic).encode("ascii", "ignore").decode().lower().strip()
    return s.replace(" ", "_").replace("/", "_")


def diagnose_topic(topic_id: str, hours: int = 20) -> None:
    gcs = GCSService()
    topics_data = gcs.get_json_file("topics.json") or {}
    if topic_id not in topics_data:
        print(f"[X] Topic '{topic_id}' no encontrado en topics.json")
        # Buscar similares (substring match)
        similar = [k for k in topics_data.keys() if topic_id.lower() in k.lower() or k.lower() in topic_id.lower()]
        if similar:
            print(f"Similares: {similar[:10]}")
        print(f"Disponibles (primeros 30): {sorted(topics_data.keys())[:30]}")
        return

    topic_data = topics_data[topic_id]
    news_list = topic_data.get("news", [])
    print(f"\n=== {topic_id} ===")
    print(f"Total noticias en topics.json: {len(news_list)}")
    print(f"fecha_inventariado_topic: {topic_data.get('fecha_inventariado')}")

    cutoff = datetime.now() - timedelta(hours=hours)
    by_source = Counter()
    fresh = []
    stale = []
    for n in news_list:
        fecha_inv = n.get("fecha_inventariado", "")
        try:
            dt = datetime.fromisoformat(fecha_inv[:19])
        except Exception:
            stale.append(n)
            continue
        if dt >= cutoff:
            fresh.append(n)
        else:
            stale.append(n)
        for f in n.get("fuentes", []):
            try:
                from urllib.parse import urlparse
                dom = urlparse(f).netloc.replace("www.", "")
                by_source[dom] += 1
            except Exception:
                pass

    print(f"Frescos (<{hours}h): {len(fresh)}")
    print(f"Stales (>{hours}h): {len(stale)}")
    print()
    print(f"Top 20 fuentes con artículos:")
    for src, count in by_source.most_common(20):
        print(f"  {count:3d}  {src}")
    print()
    print("Frescos (últimos 10):")
    for n in fresh[-10:]:
        title = n.get("titulo", "")[:90]
        fecha = n.get("fecha_inventariado", "")[:19]
        sources = [f for f in n.get("fuentes", [])]
        dom = ""
        if sources:
            from urllib.parse import urlparse
            dom = urlparse(sources[0]).netloc.replace("www.", "")
        print(f"  [{fecha}] {dom:30s} | {title}")


def diagnose_user_topics(user_email: str) -> None:
    from src.services.firebase_service import FirebaseService
    fb = FirebaseService()
    user_doc = fb.get_user_by_email(user_email)
    if not user_doc:
        print(f"[X] Usuario '{user_email}' no encontrado")
        return
    user_topics = user_doc.get("topic") or user_doc.get("topics") or {}
    print(f"\n=== Topics del usuario {user_email} ===")
    for t, ctx in (user_topics.items() if isinstance(user_topics, dict) else []):
        tid = _normalize_id(t)
        print(f"\n--- '{t}' (id: {tid}) ---")
        print(f"Contexto: {ctx[:200]}")
        diagnose_topic(tid)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", help="Topic ID (normalizado)")
    parser.add_argument("--user", help="User email")
    parser.add_argument("--hours", type=int, default=20)
    args = parser.parse_args()

    if args.user:
        diagnose_user_topics(args.user)
    elif args.topic:
        diagnose_topic(args.topic, args.hours)
    else:
        # Default: inspecciona todos los topics
        gcs = GCSService()
        topics_data = gcs.get_json_file("topics.json") or {}
        print(f"Topics totales en GCS: {len(topics_data)}")
        for tid in sorted(topics_data.keys()):
            n = topics_data[tid].get("news", [])
            print(f"  {tid:40s}  {len(n)} artículos")
