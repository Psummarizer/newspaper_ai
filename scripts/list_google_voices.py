
import asyncio
import os
import sys
from google.cloud import texttospeech

# Add root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def list_voices():
    print("🔍 Buscando voces de Google Cloud TTS (es-ES)...")
    try:
        client = texttospeech.TextToSpeechClient()
        response = client.list_voices(language_code="es-ES")
        
        print(f"\nVoces encontradas: {len(response.voices)}")
        print("-" * 60)
        print(f"{'Nombre':<30} | {'Genero':<10} | {'Tipo':<15}")
        print("-" * 60)
        
        for voice in response.voices:
            # Filter for es-ES only to be safe
            if "es-ES" not in voice.name:
                continue
                
            gender = texttospeech.SsmlVoiceGender(voice.ssml_gender).name
            voice_type = "Standard"
            if "Neural2" in voice.name:
                voice_type = "Neural2"
            elif "Studio" in voice.name:
                voice_type = "Studio"
            elif "Wavenet" in voice.name:
                voice_type = "Wavenet"
            elif "Polyglot" in voice.name:
                voice_type = "Polyglot"
            elif "Chirp" in voice.name:
                voice_type = "Chirp (HD)"
            
            print(f"{voice.name:<30} | {gender:<10} | {voice_type:<15}")

    except Exception as e:
        print(f"❌ Error listando voces: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(list_voices())
