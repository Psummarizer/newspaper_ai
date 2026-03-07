"""
Update Firestore topics for a specific user.
Usage: python scripts/update_firestore_topics.py
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

import firebase_admin
from firebase_admin import credentials, firestore
import json


def get_firestore_client():
    creds_json = os.getenv("FIREBASE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("FIREBASE_CREDENTIALS_JSON not set")
    cred = credentials.Certificate(json.loads(creds_json))
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    return firestore.client()


def update_user_topics(db, email: str, topics: dict):
    """Update the topics map for a user in AINewspaper collection."""
    doc_ref = db.collection("AINewspaper").document(email)
    doc = doc_ref.get()
    if not doc.exists:
        print(f"ERROR: User '{email}' not found in AINewspaper collection")
        return False

    # Update just the topics field (merge=True preserves other fields)
    doc_ref.set({"topics": topics}, merge=True)
    print(f"Updated topics for {email}:")
    for k, v in topics.items():
        desc_preview = v[:60] + "..." if len(v) > 60 else v
        print(f"  {k}: \"{desc_preview}\"")
    return True


if __name__ == "__main__":
    db = get_firestore_client()

    # =========================================================================
    # User: diondijkshoorn@outlook.com
    # Condensed topics with rich context for better article matching
    # =========================================================================
    dion_topics = {
        "palm oil": "CPO futures (BMD), Indonesia B40 mandate, Malaysia export levy, India import demand, Rotterdam/ARA stocks, palm-soy spread, RSPO",
        "soy oil": "CBOT soy oil futures, WASDE report, Argentina/Brazil crop progress, US crush margins, palm-soy spread",
        "biofuels/biodiesel": "EU RED III, US RFS RVO levels, HVO/FAME feedstock spreads, blending mandates, SAF developments, renewable diesel margins",
        "energy prices": "Brent/WTI crude, TTF natural gas, refinery margins, OPEC+ output decisions, energy-biodiesel cost link",
        "tariffs & trade flows": "EU anti-dumping Indonesian biodiesel, US tariff impacts vegetable oils, trade rerouting, WTO disputes, export levies",
        "freight": "Clean tanker rates, ARA terminal stocks, Panama/Suez disruptions, bulk vessel availability, DDP/FOB spread impact",
        "bitcoin": "Spot price, ETF net flows (BlackRock/Fidelity), exchange reserves, Fed correlation, regulatory news US/EU",
        "gold & silver": "Spot prices, central bank buying, real yields, ETF holdings, gold/silver ratio, safe-haven flows",
        "macro": "ECB/Fed rate decisions, CPI/PCE/HICP prints, EUR/USD, China PMI/demand outlook, India consumption growth",
    }

    print("=" * 60)
    print("Updating Firestore topics for diondijkshoorn@outlook.com")
    print("=" * 60)
    success = update_user_topics(db, "diondijkshoorn@outlook.com", dion_topics)
    if success:
        print("\nDone! Topics updated successfully.")
    else:
        print("\nFailed to update topics.")
