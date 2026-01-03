from sqlalchemy import Column, String, Integer, Boolean, ForeignKey, DateTime, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from src.database.connection import Base
import uuid

def generate_uuid():
    return str(uuid.uuid4())

class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=generate_uuid)
    email = Column(String, unique=True, index=True, nullable=False)
    language = Column(String, default="es")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relación con topics
    topics = relationship("Topic", back_populates="owner", cascade="all, delete-orphan")

class Topic(Base):
    __tablename__ = "topics"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"))
    keyword = Column(String, nullable=False)

    # Configuración extra (filtros, relevancia mínima...) guardada como JSON
    settings = Column(JSON, default={})

    last_sent_at = Column(DateTime(timezone=True), nullable=True)

    owner = relationship("User", back_populates="topics")
