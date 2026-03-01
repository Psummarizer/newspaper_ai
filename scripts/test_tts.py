import asyncio
import os
import sys

# Ajustar path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.podcast_service import NewsPodcastService
from src.services.llm_factory import LLMFactory

async def test_audio():
    # Leer el provider activo desde el Factory
    config = LLMFactory.get_tts_config()
    provider = config.get("provider")
    voices = config.get("voices", {}).get(provider, {})
    
    print("=== PRUEBA DE AUDIO TTS ===")
    print(f"Proveedor activo en model_config.json: {provider}")
    print(f"Voces asignadas: {voices}")
    
    script = """ÁLVARO: ¡Hola a todos! Bienvenidos a este mini podcast de prueba, generado con la nueva arquitectura abstracta.
ELVIRA: Hola Álvaro, y hola a todos los oyentes. Es increíble cómo podemos cambiar de proveedor de voces cambiando solo una palabra en la configuración.
ÁLVARO: Totalmente de acuerdo, Elvira. Son apenas un par de líneas, pero suficientes para escuchar cómo suenan las voces actualmente configuradas en el sistema.
ELVIRA: —¡Espera, no te despidas todavía! Hay que recordarles que el formato de debate rápido también funciona de maravilla a nivel sonoro.
ÁLVARO: Tienes toda la razón. Bueno, prueba superada. ¡Hasta el próximo episodio!
ELVIRA: ¡Adiós!"""

    service = NewsPodcastService()
    output_path = os.path.join(os.path.dirname(__file__), "prueba_voces_podcast.mp3")
    
    print("\nGenerando archivo de audio (puede tardar un poco dependiendo del proveedor)...")
    success = await service._generate_audio(script, output_path)
    
    if success:
        print(f"\n✅ Audio generado con éxito.")
        print(f"Ruta: {output_path}")
    else:
        print("\n❌ Error generando el audio. Revisa los logs.")

if __name__ == "__main__":
    asyncio.run(test_audio())
