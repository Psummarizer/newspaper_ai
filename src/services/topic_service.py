import json
import logging
import os
import re
from typing import List, Dict, Set, Optional
from src.services.gcs_service import GCSService

class TopicService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.gcs = GCSService()
        self.topics = self._load_topics()
        self._compile_regexes()

    def _load_topics(self) -> List[Dict]:
        """Loads topics from GCS. If empty, tries to load from local file and upload."""
        topics = self.gcs.get_topics()
        if topics:
            self.logger.info(f"✅ Loaded {len(topics)} topics from GCS.")
            return topics
        
        # Fallback to local
        local_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "topics.json")
        if os.path.exists(local_path):
            try:
                with open(local_path, "r", encoding="utf-8") as f:
                    topics = json.load(f)
                self.logger.info(f"📂 Loaded {len(topics)} topics from local file. Uploading to GCS...")
                if self.gcs.save_topics(topics):
                    self.logger.info("✅ Topics uploaded to GCS.")
                else:
                    self.logger.warning("⚠️ Failed to upload topics to GCS.")
                return topics
            except Exception as e:
                self.logger.error(f"❌ Error loading local topics: {e}")
                return []
        
        self.logger.warning("⚠️ No topics found in GCS or local file.")
        return []

    def _compile_regexes(self):
        """Pre-compiles regex patterns for aliases for performance."""
        self.topic_map = {} # id -> topic dict
        self.alias_patterns = [] # list of (compiled_regex, topic_id)
        
        if not self.topics:
            return

        for topic in self.topics:
            t_id = topic["id"]
            self.topic_map[t_id] = topic
            
            # Add implicit alias: the topic name itself
            aliases = topic.get("aliases", [])
            # Also add the ID as a hidden alias if not present, sometimes users type IDs
            
            clean_aliases = set(aliases)
            # clean_aliases.add(topic["name"]) # Name is usually handled by aliases but let's be safe? No, aliases usually cover it.
            
            for alias in clean_aliases:
                escaped_alias = re.escape(alias)
                try:
                    pattern = re.compile(rf"\b{escaped_alias}\b", re.IGNORECASE)
                    self.alias_patterns.append((pattern, t_id))
                except re.error:
                     self.logger.warning(f"⚠️ Invalid regex for alias: {alias}")
    
    def normalize(self, text: str) -> List[str]:
        """
        Scans text for topic aliases.
        Returns unique topic IDs found.
        """
        if not text:
            return []
        
        found_ids = set()
        
        for pattern, t_id in self.alias_patterns:
            try:
                if pattern.search(text):
                    found_ids.add(t_id)
            except Exception:
                continue
        
        # Fallback: Fuzzy Matching for short inputs (User Preferences)
        if not found_ids and len(text) < 100:
            from difflib import SequenceMatcher
            max_sim = 0
            best_id = None
            
            for t_id, topic in self.topic_map.items():
                # Check Name
                sim = SequenceMatcher(None, text.lower(), topic["name"].lower()).ratio()
                if sim > max_sim:
                    max_sim = sim
                    best_id = t_id
                
                # Check Aliases
                for alias in topic.get("aliases", []):
                    sim = SequenceMatcher(None, text.lower(), alias.lower()).ratio()
                    if sim > max_sim:
                        max_sim = sim
                        best_id = t_id
            
            if max_sim > 0.85: # Threshold high to avoid false positives
                found_ids.add(best_id)
                self.logger.info(f"✨ Fuzzy Match: '{text}' -> {best_id} ({max_sim:.2f})")
                
        return list(found_ids)

    def get_topic_name(self, topic_id: str) -> str:
        topic = self.topic_map.get(topic_id)
        return topic["name"] if topic else topic_id
