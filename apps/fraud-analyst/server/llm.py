"""Foundation Model API client for the fraud analyst assistant."""

import os
from openai import AsyncOpenAI
from .config import get_oauth_token, get_workspace_host

SERVING_ENDPOINT = os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4-5")

_llm_client: AsyncOpenAI | None = None
_llm_token: str | None = None


def get_llm_client() -> AsyncOpenAI:
    """Get OpenAI-compatible client (cached; refreshes when token changes)."""
    global _llm_client, _llm_token
    token = get_oauth_token()
    if _llm_client is None or token != _llm_token:
        host = get_workspace_host()
        _llm_client = AsyncOpenAI(api_key=token, base_url=f"{host}/serving-endpoints")
        _llm_token = token
    return _llm_client


SYSTEM_PROMPT = """You are a fraud analyst assistant for a telecommunications company.
You help analysts investigate suspicious transactions by:
- Summarizing case details and risk factors
- Explaining fraud patterns and risk signals
- Suggesting investigation steps
- Recommending mitigation actions
- Answering questions about telecom fraud typologies

When given case data, analyze it thoroughly and provide actionable insights.
Be concise and specific. Use bullet points for clarity.
Do not make up data — only reference what is provided."""


async def chat_completion(messages: list[dict], case_context: str | None = None) -> str:
    """Get chat completion from Foundation Model."""
    client = get_llm_client()

    system_msg = SYSTEM_PROMPT
    if case_context:
        system_msg += f"\n\nCurrent case context:\n{case_context}"

    full_messages = [{"role": "system", "content": system_msg}] + messages

    response = await client.chat.completions.create(
        model=SERVING_ENDPOINT,
        messages=full_messages,
        max_tokens=2048,
        temperature=0.3,
    )
    return response.choices[0].message.content
