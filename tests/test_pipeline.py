import asyncio
import sys
import os
import warnings

# Filtros de ruido
warnings.filterwarnings("ignore")

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from src.agents.orchestrator import Orchestrator

async def test_pipeline_with_db():
    orchestrator = Orchestrator()
    print("--- 🚀 INICIANDO TEST CON BASE DE DATOS ---")

    # Solo damos el email. El sistema debe buscar sus intereses.
    target_email = "alex@test.com"

    try:
        # Llamamos al NUEVO método
        result = await orchestrator.run_for_user(target_email)

        if result.get("success") or isinstance(result, dict): # Ajuste por si devuelve dict directo o AgentResult
            # Nota: Dependiendo de tu implementación de BaseAgent, result puede ser objeto o dict.
            # Asumimos que run_for_user devuelve lo que devuelve execute.

            # Si es objeto AgentResult accedemos a .data, si es dict accedemos directo
            data = result.data if hasattr(result, 'data') else result

            if not data:
                 print(f"❌ Error o datos vacíos: {result}")
                 return

            newsletter = data.get('newsletter_content', [])

            print(f"\\n📧 Generando Newsletter para: {target_email}")
            for section in newsletter:
                print(f"\\n📌 {section['topic'].upper()}")
                for art in section['articles']:
                    print(f"  - {art['title']}")
        else:
            print("❌ Fallo en la ejecución")

    finally:
        await orchestrator.cleanup()

if __name__ == "__main__":
    asyncio.run(test_pipeline_with_db())
