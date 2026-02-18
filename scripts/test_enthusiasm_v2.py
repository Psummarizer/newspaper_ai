from google.cloud import texttospeech
import os

def generate_elvira_enthusiasm_v2():
    client = texttospeech.TextToSpeechClient()
    voice_name = "es-ES-Neural2-H"
    
    # Updated text to reflect natural intro without meta-description
    text = "¡Hola! ¿Qué tal? Soy Elvira. Hoy tenemos un programa cargado de noticias increíbles. ¡Vamos a ello!"
    
    try:
        print(f"Generando {voice_name} (V2 Enthusiasm)...")
        synthesis_input = texttospeech.SynthesisInput(text=text)
        
        voice_params = texttospeech.VoiceSelectionParams(
            language_code="es-ES",
            name=voice_name
        )
        
        # INCREASED ENTHUSIASM
        # Pitch: 3.5 (Higher energy)
        # Rate: 1.18 (Faster, more dynamic)
        audio_config = texttospeech.AudioConfig(
            audio_encoding=texttospeech.AudioEncoding.MP3,
            speaking_rate=1.18,
            pitch=3.5
        )
        
        response = client.synthesize_speech(
            input=synthesis_input, voice=voice_params, audio_config=audio_config
        )

        filename = "scripts/test_elvira_enthusiasm_v2.mp3"
        with open(filename, "wb") as out:
            out.write(response.audio_content)
        print(f"✅ Guardado: {filename}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    generate_elvira_enthusiasm_v2()
