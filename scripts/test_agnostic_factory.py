import asyncio
import os
import sys

# Ajustar path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.llm_factory import LLMFactory

async def test_factory():
    print("Testing LLM Factory...\n")
    
    # 1. Test Config Loading
    tts_conf = LLMFactory.get_tts_config()
    print(f"TTS Config loaded: {tts_conf['provider']}")
    
    # 2. Test Client instantiation
    print("\nRequesting 'fast' client...")
    client_fast, model_fast = LLMFactory.get_client("fast")
    print(f"Client Fast returned model: {model_fast}")
    
    print("\nRequesting 'quality' client...")
    client_quality, model_quality = LLMFactory.get_client("quality")
    print(f"Client Quality returned model: {model_quality}")
    
    print("\nRunning test completion...")
    try:
        resp = await client_fast.chat.completions.create(
            model=model_fast,
            messages=[{"role": "user", "content": "Say hello world in 3 words."}],
            max_tokens=100
        )
        print("Raw response:", resp)
        print("Response content:", resp.choices[0].message.content)
        print("SUCCESS")
    except Exception as e:
        print(f"FAIL: {e}")

if __name__ == "__main__":
    asyncio.run(test_factory())
