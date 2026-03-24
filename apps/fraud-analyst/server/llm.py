"""Foundation Model API client for the analyst AI assistant."""

import os
from openai import AsyncOpenAI
from .config import get_oauth_token, get_workspace_host
from .industry_config import SERVING_ENDPOINT_DEFAULT

SERVING_ENDPOINT = os.environ.get("SERVING_ENDPOINT", SERVING_ENDPOINT_DEFAULT)

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


