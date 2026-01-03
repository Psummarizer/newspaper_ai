import sys
import os
from fastapi.testclient import TestClient

# Corrección de rutas (igual que antes)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from src.main import app

# Creamos un cliente de prueba
client = TestClient(app)

def test_api_health():
    print("\\n--- TEST 1: Health Check del Servidor ---")
    # Simulamos ir a <http://localhost:8080/>
    response = client.get("/")

    if response.status_code == 200:
        print(f"✅ Servidor responde: {response.json()}")
    else:
        print(f"❌ Error en servidor: {response.status_code}")

def test_api_normalize_topic():
    print("\\n--- TEST 2: Endpoint /topics/normalize ---")
    # Simulamos enviar datos al endpoint
    payload = {
        "user_id": "api_tester",
        "language": "en",
        "topics": ["  SpaceX  ", "NASA"]
    }

    response = client.post("/topics/normalize", json=payload)

    if response.status_code == 200:
        data = response.json()
        topics = data['data']['valid_topics']
        print(f"✅ API devolvió {len(topics)} topics procesados.")
        print(f"   Primer topic limpio: '{topics[0]['keyword']}'")
    else:
        print(f"❌ Error en API: {response.text}")

if __name__ == "__main__":
    test_api_health()
    test_api_normalize_topic()
