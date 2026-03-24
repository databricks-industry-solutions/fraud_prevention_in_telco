"""System prompt for the fraud investigation agent.

The prompt content is defined in server/industry_config.py so it can be swapped
per-industry. Edit industry_config.py to change the AI's personality and domain knowledge.
"""

from ..industry_config import AGENT_SYSTEM_PROMPT

SYSTEM_PROMPT = AGENT_SYSTEM_PROMPT
