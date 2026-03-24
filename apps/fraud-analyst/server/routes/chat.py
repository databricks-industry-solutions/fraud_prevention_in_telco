"""Chat API route for the fraud analyst assistant."""

import json
import logging
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address
from typing import Optional

from ..agent.agent import agent
from ..config import get_oauth_token, get_workspace_host
from ..db import db, PGSCHEMA
import os
import aiohttp

from ..industry_config import KA_ENDPOINT_DEFAULT
KA_ENDPOINT = os.environ.get("KA_ENDPOINT", KA_ENDPOINT_DEFAULT)

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
            f"""SELECT transaction_id, transaction_date, transaction_state, transaction_region,
               transaction_type, transaction_subtype, transaction_cost, account_id,
               customer_user_id, customer_name, account_services, account_detail,
               high_risk_flag, subscriber_location_lat, subscriber_location_long,
               fraud_score, fraud_label_engine, fraud_label, risk_status_engine,
               risk_reason_engine, review_status, assigned_analyst, analyst_notes,
               last_review_date, mitigation_steps, fraud_root_cause, case_exposure_usd,
               is_fp, is_fn
        FROM {PGSCHEMA}.transactions_synced WHERE transaction_id = $1""",
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
        return JSONResponse(
            status_code=500,
            content={"reply": "I encountered an error. Please try again.", "tool_calls": [], "error": True},
        )


@router.post("/stream")
@limiter.limit("10/minute")
async def chat_stream(request: Request, req: ChatRequest):
    """Stream a response from the fraud investigation agent via SSE."""
    case_context = None
    if req.transaction_id:
        rows = await db.execute(
            f"""SELECT transaction_id, transaction_date, transaction_state, transaction_region,
               transaction_type, transaction_subtype, transaction_cost, account_id,
               customer_user_id, customer_name, account_services, account_detail,
               high_risk_flag, subscriber_location_lat, subscriber_location_long,
               fraud_score, fraud_label_engine, fraud_label, risk_status_engine,
               risk_reason_engine, review_status, assigned_analyst, analyst_notes,
               last_review_date, mitigation_steps, fraud_root_cause, case_exposure_usd,
               is_fp, is_fn
        FROM {PGSCHEMA}.transactions_synced WHERE transaction_id = $1""",
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

            # Send full response immediately
            yield f"data: {json.dumps({'type': 'text', 'content': result['response']})}\n\n"

            yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except Exception as e:
            logger.error("Agent stream error: %s", e)
            yield f"data: {json.dumps({'type': 'error', 'content': 'I encountered an error. Please try again.'})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/ka")
@limiter.limit("10/minute")
async def chat_ka(request: Request, req: ChatRequest):
    """Send a message to the Knowledge Assistant for historical fraud analysis."""
    host = get_workspace_host()
    token = get_oauth_token()

    # Build messages for the KA — include case context if available
    ka_messages = []
    if req.transaction_id:
        rows = await db.execute(
            f"""SELECT transaction_id, customer_name, fraud_score, case_exposure_usd,
                   risk_reason_engine, review_status, transaction_region, transaction_type,
                   mitigation_steps, assigned_analyst
            FROM {PGSCHEMA}.transactions_synced WHERE transaction_id = $1""",
            req.transaction_id,
        )
        if rows:
            case = rows[0]
            context = "\n".join(f"  {k}: {v}" for k, v in case.items() if v)
            ka_messages.append({
                "role": "user",
                "content": f"I'm reviewing case {req.transaction_id}. Here is the case context:\n{context}\n\nPlease use this context when answering my questions.",
            })
            ka_messages.append({
                "role": "assistant",
                "content": f"I have the context for case {req.transaction_id}. How can I help with your historical analysis?",
            })

    ka_messages.extend([{"role": m.role, "content": m.content} for m in req.messages])

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{host}/serving-endpoints/{KA_ENDPOINT}/invocations",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={
                    "input": ka_messages,
                },
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                data = await resp.json()

        # Extract response — KA uses Responses API format
        reply = ""
        citations = []
        if isinstance(data, dict):
            # Responses API: output[].content[].text + annotations
            outputs = data.get("output", [])
            for output_item in outputs:
                if output_item.get("type") == "message":
                    for content_part in output_item.get("content", []):
                        if content_part.get("type") == "output_text":
                            reply += content_part.get("text", "")
                            for ann in content_part.get("annotations", []):
                                if ann.get("type") == "url_citation":
                                    title = ann.get("title", "")
                                    if title and title not in citations:
                                        citations.append(title)

            # Fallback: chat completions format
            if not reply:
                choices = data.get("choices", [])
                if choices:
                    reply = choices[0].get("message", {}).get("content", "")
                else:
                    reply = data.get("content", data.get("result", ""))

            if not reply:
                reply = str(data)

        else:
            reply = str(data)

        # Append source citations
        if citations:
            reply += "\n\n---\n*Sources: " + ", ".join(citations) + "*"

        return {"reply": reply, "tool_calls": [], "source": "Company Playbook"}

    except Exception as e:
        logger.error("KA error: %s", e)
        return JSONResponse(
            status_code=500,
            content={"reply": "Company Playbook is unavailable. Please try Case Investigation.", "tool_calls": [], "error": True},
        )
