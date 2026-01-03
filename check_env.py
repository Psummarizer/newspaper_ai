
import os
from dotenv import load_dotenv

print("--- Verifying .env loading ---")
try:
    load_dotenv(override=True)
    
    openai_key = os.getenv("OPENAI_API_KEY")
    firebase_json = os.getenv("FIREBASE_CREDENTIALS_JSON")
    
    print(f"OPENAI_API_KEY found: {'YES' if openai_key else 'NO'}")
    if openai_key:
        print(f"OPENAI_API_KEY length: {len(openai_key)}")
        
    print(f"FIREBASE_CREDENTIALS_JSON found: {'YES' if firebase_json else 'NO'}")
    if firebase_json:
        print(f"FIREBASE_CREDENTIALS_JSON starts with: {firebase_json[:20]}")
        # Try to parse it to see if it causes crash
        import json
        try:
            json.loads(firebase_json)
            print("FIREBASE_CREDENTIALS_JSON is valid JSON.")
        except Exception as e:
            print(f"FIREBASE_CREDENTIALS_JSON is INVALID JSON: {e}")

except Exception as e:
    print(f"CRITICAL: load_dotenv failed: {e}")
