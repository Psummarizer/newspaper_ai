import asyncio
import logging
import sys
import os
import re

# Add root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.gcs_service import GCSService
from src.services.firebase_service import FirebaseService
from src.services.llm_service import LLMService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("update_topics")

def slugify(text: str) -> str:
    # Basic slugify: "Artificial Intelligence" -> "artificial_intelligence"
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_-]+', '_', text)
    return text

async def update_topics_registry():
    logger.info("🔄 Iniciando actualización (Re-Ingeniería) de tópicos...")
    
    gcs = GCSService()
    fb = FirebaseService()
    llm = LLMService() # Assumed configured with cheap model via ENV
    
    # 1. Get Current Registry
    current_registry = gcs.get_topics() # List of dicts
    # Map for fast lookup: id -> entry
    registry_map = {item['id']: item for item in current_registry}
    
    # Build a lookup by category to optimize matching candidates
    registry_by_category = {}
    for item in current_registry:
        # Item might have 'category' (str) or 'categories' (list)? 
        # Plan said 1/2 categories. Let's support list or str.
        cats = item.get("category")
        if isinstance(cats, str): cats = [cats]
        if not cats: cats = ["General"]
        
        for c in cats:
            if c not in registry_by_category: registry_by_category[c] = []
            registry_by_category[c].append(item)

    # 2. Get User Topics from Firebase
    user_topics_set = fb.get_all_distinct_user_topics()
    logger.info(f"👥 Encontrados {len(user_topics_set)} tópicos únicos en usuarios.")
    
    # 3. Identify Unknown Topics
    # Helper to check if topic exists (by name or alias)
    # This is a basic string check. The REAL check is the LLM one later.
    # But we should skip EXACT matches to save money.
    
    known_aliases = set()
    for item in current_registry:
        known_aliases.add(item['name'].lower())
        for a in item.get('aliases', []):
            known_aliases.add(a.lower())
            
    unknown_topics = []
    for t in user_topics_set:
        if t.lower() not in known_aliases:
            unknown_topics.append(t)
            
    if not unknown_topics:
        logger.info("✅ Todos los tópicos de usuario ya existen en el registro.")
        return

    logger.info(f"🧐 Encontrados {len(unknown_topics)} tópicos NUEVOS para procesar.")
    
    # 4. Phase 1: Assign Categories (Batch)
    # We can process in batches if list is huge, but typical usage is small incremental.
    # Let's do chunks of 20
    chunk_size = 20
    changes_made = False
    
    for i in range(0, len(unknown_topics), chunk_size):
        chunk = unknown_topics[i:i+chunk_size]
        logger.info(f"🧩 Categorizando lote {i}-{i+len(chunk)}...")
        
        categories_map = await llm.get_categories_for_topics(chunk)
        # categories_map = { "topic": ["Cat1", "Cat2"] }
        
        # 5. Phase 2: Deduplication / Creation (Per Topic)
        for topic in chunk:
            cats = categories_map.get(topic, ["General"])
            if not isinstance(cats, list): cats = [cats]
            
            # Find candidates in these categories
            candidates = []
            seen_ids = set()
            for c in cats:
                for existing in registry_by_category.get(c, []):
                    if existing['id'] not in seen_ids:
                        candidates.append(existing)
                        seen_ids.add(existing['id'])
            
            logger.info(f"🔍 Tópico '{topic}' asignado a {cats}. Candidatos a alias: {len(candidates)}")
            
            match_id = None
            if candidates:
                match_id = await llm.find_matching_topic(topic, candidates)
            
            if match_id and match_id in registry_map:
                # ALIAS
                existing = registry_map[match_id]
                if "aliases" not in existing: existing["aliases"] = []
                if topic not in existing["aliases"]:
                    existing["aliases"].append(topic)
                    logger.info(f"🔗 MATCH: '{topic}' es alias de '{existing['name']}' ({match_id})")
                    changes_made = True
            else:
                # CREATE NEW
                new_id = slugify(topic)
                # Ensure unique ID
                original_new_id = new_id
                counter = 1
                while new_id in registry_map:
                    new_id = f"{original_new_id}_{counter}"
                    counter += 1
                
                new_entry = {
                    "id": new_id,
                    "name": topic.title(), # Use user topic as canonical name
                    "aliases": [topic],   # Add itself as alias per user request
                    "category": cats,
                }
                current_registry.append(new_entry)
                registry_map[new_id] = new_entry # Add to map for future dedupes in this same run?
                # Add to registry_by_category for this run?
                for c in cats:
                    if c not in registry_by_category: registry_by_category[c] = []
                    registry_by_category[c].append(new_entry)
                    
                logger.info(f"🆕 CREATE: '{topic}' (ID: {new_id}, Cats: {cats})")
                changes_made = True

    # 6. Save
    if changes_made:
        if gcs.save_topics(current_registry):
            logger.info("💾 Registro de tópicos actualizado en GCS.")
        else:
            logger.error("❌ Fallo guardando GCS.")
    else:
        logger.info("ℹ️ No se requirieron cambios.")

    await llm.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(update_topics_registry())
