from typing import List, Dict

class SourceRegistry:
    """
    Base de datos estática de fuentes de alta calidad clasificadas.
    En el futuro, esto debería moverse a una tabla SQL real.
    """

    SOURCES = {
        "TECH_GENERAL": [
            "techcrunch.com", "theverge.com", "wired.com", "arstechnica.com",
            "venturebeat.com", "xataka.com", "genbeta.com"
        ],
        "AI_RESEARCH": [
            "arxiv.org", "huggingface.co", "openai.com/blog",
            "research.google/blog", "mit.edu"
        ],
        "PYTHON_DEV": [
            "realpython.com", "planetpython.org", "dev.to",
            "python.org/blogs", "medium.com/tag/python"
        ],
        "BUSINESS_STARTUPS": [
            "forbes.com", "bloomberg.com", "businessinsider.com",
            "ycombinator.com/blog", "elreferente.es"
        ]
    }

    @staticmethod
    def get_domains_for_topic(topic: str) -> List[str]:
        """
        Devuelve una lista de dominios recomendados según las palabras clave del tema.
        """
        topic_lower = topic.lower()
        domains = []

        # Lógica de enrutamiento simple (se puede mejorar con IA)
        if "python" in topic_lower or "code" in topic_lower:
            domains.extend(SourceRegistry.SOURCES["PYTHON_DEV"])
            domains.extend(SourceRegistry.SOURCES["TECH_GENERAL"])

        elif "ia" in topic_lower or "ai" in topic_lower or "artificial" in topic_lower:
            domains.extend(SourceRegistry.SOURCES["AI_RESEARCH"])
            domains.extend(SourceRegistry.SOURCES["TECH_GENERAL"])

        elif "startup" in topic_lower or "negocio" in topic_lower:
            domains.extend(SourceRegistry.SOURCES["BUSINESS_STARTUPS"])
            domains.extend(SourceRegistry.SOURCES["TECH_GENERAL"])

        else:
            # Fallback: Mezcla general
            domains.extend(SourceRegistry.SOURCES["TECH_GENERAL"])

        return list(set(domains)) # Eliminar duplicados
