from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    # Cambiamos a String para soportar el UUID que usas en seed_db.py
    id = Column(String, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    topics = Column(String, nullable=True)
    language = Column(String, default="es")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class Source(Base):
    __tablename__ = "sources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)
    # Tu script seed_sources usa 'rss_url', así que actualizamos el modelo
    rss_url = Column(String, nullable=False, unique=True)
    category = Column(String, nullable=True)

    # Nuevos campos que usa tu seed_sources.py
    language = Column(String, default="es")
    country = Column(String, default="ES")

    is_active = Column(Boolean, default=True)

class Article(Base):
    __tablename__ = "articles"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    url = Column(String, unique=True, nullable=False, index=True)
    source_name = Column(String, nullable=True)
    category = Column(String, nullable=True)
    published_at = Column(DateTime, default=datetime.utcnow)
    # Eliminamos image_url para evitar errores si no la usamos
