import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.agents.orchestrator import Orchestrator
    from src.agents.content_processor import ContentProcessorAgent
    from src.utils.html_builder import build_newsletter_html
    
    print("✅ Imports successful")
    
    orch = Orchestrator()
    print("✅ Orchestrator instantiated")
    
    proc = ContentProcessorAgent()
    print("✅ ContentProcessor instantiated")
    
    print("✅ Syntax check passed")
    
except Exception as e:
    print(f"❌ Verification failed: {e}")
    sys.exit(1)
