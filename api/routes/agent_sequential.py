from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from typing import Optional
import logging
from concurrent.futures import ThreadPoolExecutor
import asyncio

from api.models.schemas import Query, AgentResponse
from core.agent_manager_sequential import AgentManagerSequential
from config.settings import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/agent",
    tags=["agent"],
)

# Create a singleton agent manager
_agent_manager = None
_executor = ThreadPoolExecutor(max_workers=1)


def get_agent_manager() -> AgentManagerSequential:
    """
    Get or create an agent manager instance.

    Returns:
        AgentManager: The agent manager instance
    """
    global _agent_manager
    if _agent_manager is None:
        logger.info("Initializing AgentManager singleton.")
        _agent_manager = AgentManagerSequential(
            api_key=settings.LLM_API_KEY,
            model=settings.LLM_MODEL,
            temperature=settings.LLM_TEMPERATURE
        )
        print("instantiated AgentManagerSequential object")
    return _agent_manager


@router.post("/query", response_model=AgentResponse)
async def process_query(
    query: Query,
    agent_manager: AgentManagerSequential = Depends(get_agent_manager),
) -> AgentResponse:
    """
    Process a query through the database agent system.

    Args:
        query (Query): The user query to process
        agent_manager (AgentManager): The agent manager dependency

    Returns:
        AgentResponse: The response from the agent system
    """
    logger.info(f"Console: Received query in agent.py endpoint: {query.query}") # Goes to console
    
    try:
        loop = asyncio.get_running_loop()
        
        # --- PASS req_logger TO agent_manager.process_query ---
        api_response = await loop.run_in_executor(
            _executor,
            agent_manager.process_query, # This is AgentManager.process_query
            query.query,
        )
        
        logger.info("API Endpoint: agent_manager.process_query returned.")
        return api_response # AgentResponse instance is already returned

    except HTTPException as http_exc:
        logger.error(f"API Endpoint: HTTPException: {http_exc.detail}", exc_info=True) # Console & file
        raise http_exc
    except Exception as e:
        logger.error(f"API Endpoint: Unexpected error: {str(e)}", exc_info=True) # Console & file
        raise HTTPException(
            status_code=500,
            detail=f"Error processing query: {str(e)}"
        )


@router.get("/health")
def agent_health() -> dict:
    """
    Health check for the agent subsystem.
    """
    logger.info("API Endpoint: Agent health check.")
    return {"status": "ok", "model": settings.LLM_MODEL}
