from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any


class Query(BaseModel):
    """
    Model for incoming query requests.
    """
    query: str = Field(..., description="The database query in natural language")
    model: Optional[str] = Field(None, description="Override the default LLM model")
    temperature: Optional[float] = Field(None, description="Override the default temperature value")


class MessageContent(BaseModel):
    """
    Model for a message in the conversation.
    """
    role: str = Field(..., description="The role of the sender (system, user, assistant)")
    content: str = Field(..., description="The content of the message")
    name: Optional[str] = Field(None, description="The name of the agent that sent the message")


class AgentResponse(BaseModel):
    """
    Model for the response from the agent system.
    """
    conversation: List[MessageContent] = Field(
        ..., description="The conversation history between agents"
    )
    final_answer: str = Field(
        ..., description="The final answer synthesized by the result analyst"
    )
