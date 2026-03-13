"""Chat API route for the fraud analyst assistant."""

import asyncio
import json
import logging
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address
from typing import Optional

from ..agent.agent import agent
from ..db import db, PGSCHEMA

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/chat", tags=["chat"])
limiter = Limiter(key_func=get_remote_address)


class ChatMessage(BaseModel):
    role: str
    content: str = Field(..., max_length=10000)


class ChatRequest(BaseModel):
    messages: list[ChatMessage] = Field(..., max_length=50)
    transaction_id: Optional[str] = Field(default=None, max_length=100)


@router.post("")
@limiter.limit("10/minute")
async def chat(request: Request, req: ChatRequest):
    """Send a message to the fraud investigation agent."""
    case_context = None
    if req.transaction_id:
        rows = await db.execute(
            f"SELECT * FROM {PGSCHEMA}.transaction_risk WHERE transaction_id = $1",
            req.transaction_id,
        )
        if rows:
            case = rows[0]
            case_context = "\n".join(f"  {k}: {v}" for k, v in case.items() if v)

    messages = [{"role": m.role, "content": m.content} for m in req.messages]

    try:
        result = await agent.chat(messages, case_context)
        return {
            "reply": result["response"],
            "tool_calls": result["tool_calls_made"],
        }
    except Exception as e:
        logger.error("Agent error: %s", e)
        return {"reply": "I encountered an error. Please try again.", "tool_calls": [], "error": True}


@router.post("/stream")
@limiter.limit("10/minute")
async def chat_stream(request: Request, req: ChatRequest):
    """Stream a response from the fraud investigation agent via SSE."""
    case_context = None
    if req.transaction_id:
        rows = await db.execute(
            f"SELECT * FROM {PGSCHEMA}.transaction_risk WHERE transaction_id = $1",
            req.transaction_id,
        )
        if rows:
            case = rows[0]
            case_context = "\n".join(f"  {k}: {v}" for k, v in case.items() if v)

    messages = [{"role": m.role, "content": m.content} for m in req.messages]

    async def event_stream():
        try:
            result = await agent.chat(messages, case_context)

            # Send tool calls as events first
            if result["tool_calls_made"]:
                yield f"data: {json.dumps({'type': 'tools', 'tool_calls': result['tool_calls_made']})}\n\n"

            # Stream the response text in chunks for a typing effect
            text = result["response"]
            chunk_size = 20  # characters per chunk
            for i in range(0, len(text), chunk_size):
                chunk = text[i:i + chunk_size]
                yield f"data: {json.dumps({'type': 'text', 'content': chunk})}\n\n"
                await asyncio.sleep(0.02)  # small delay for typing effect

            yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except Exception as e:
            logger.error("Agent stream error: %s", e)
            yield f"data: {json.dumps({'type': 'error', 'content': 'I encountered an error. Please try again.'})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
