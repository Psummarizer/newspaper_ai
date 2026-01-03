from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pydantic import BaseModel
import logging

# Configuración básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AgentResult(BaseModel):
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class BaseAgent(ABC):
    """Clase abstracta para todos los agentes del sistema"""

    def __init__(self, agent_name: str):
        self.name = agent_name
        self.logger = logging.getLogger(f"agent.{agent_name}")

    async def execute(self, payload: Dict[str, Any]) -> AgentResult:
        """Método público para ejecutar el agente con manejo de errores"""
        self.logger.info(f"Iniciando tarea para: {self.name}")
        try:
            result = await self.process(payload)
            return AgentResult(success=True, data=result)
        except Exception as e:
            self.logger.error(f"Error en {self.name}: {str(e)}")
            return AgentResult(success=False, error=str(e))

    @abstractmethod
    async def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Lógica específica del agente a implementar por las subclases"""
        pass
