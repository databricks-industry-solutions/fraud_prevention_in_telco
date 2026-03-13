"""ReAct agent with Foundation Model API tool calling."""

import json
import logging
from openai import AsyncOpenAI
from .tools import TOOL_DEFINITIONS, TOOL_DISPATCH
from .prompts import SYSTEM_PROMPT
from ..llm import get_llm_client, SERVING_ENDPOINT

logger = logging.getLogger(__name__)

MAX_TOOL_ROUNDS = 5


def _tc_id(tc) -> str:
    """Extract tool call ID from object or dict (Databricks may return dicts)."""
    return tc.id if hasattr(tc, "id") else tc.get("id", "")


def _tc_func(tc) -> tuple[str, str]:
    """Extract (name, arguments) from a tool call object or dict."""
    if hasattr(tc, "function"):
        func = tc.function
        if hasattr(func, "name"):
            return func.name, func.arguments
        return func.get("name", ""), func.get("arguments", "{}")
    func = tc.get("function", {})
    return func.get("name", ""), func.get("arguments", "{}")


class FraudInvestigationAgent:
    """ReAct-style agent that uses Foundation Model API with tool calling."""

    def __init__(self):
        self.max_rounds = MAX_TOOL_ROUNDS

    async def chat(self, messages: list[dict], case_context: str | None = None) -> dict:
        """Run the agent loop. Returns { response: str, tool_calls_made: list }."""
        client = get_llm_client()

        system_msg = SYSTEM_PROMPT
        if case_context:
            system_msg += f"\n\n## Current Case Context\n{case_context}"

        full_messages = [{"role": "system", "content": system_msg}] + messages
        tool_calls_made = []

        for round_num in range(self.max_rounds):
            try:
                response = await client.chat.completions.create(
                    model=SERVING_ENDPOINT,
                    messages=full_messages,
                    tools=TOOL_DEFINITIONS,
                    max_tokens=2048,
                    temperature=0.2,
                )
            except Exception as e:
                logger.error("Agent LLM call failed: %s", e)
                return {"response": "I encountered an error connecting to the AI service. Please try again.", "tool_calls_made": tool_calls_made}

            choice = response.choices[0]
            message = choice.message

            # If the model returns text (no tool calls), we're done
            if not message.tool_calls:
                return {"response": message.content or "", "tool_calls_made": tool_calls_made}

            # Process tool calls
            # Append the assistant message with tool_calls
            full_messages.append({
                "role": "assistant",
                "content": message.content or "",
                "tool_calls": [
                    {
                        "id": _tc_id(tc),
                        "type": "function",
                        "function": {"name": _tc_func(tc)[0], "arguments": _tc_func(tc)[1]}
                    }
                    for tc in message.tool_calls
                ]
            })

            for tc in message.tool_calls:
                func_name, func_args_str = _tc_func(tc)
                try:
                    args = json.loads(func_args_str)
                except json.JSONDecodeError:
                    args = {}

                logger.info("Agent tool call: %s(%s)", func_name, args)
                tool_calls_made.append({"tool": func_name, "args": args})

                # Execute the tool
                func = TOOL_DISPATCH.get(func_name)
                if func:
                    try:
                        result = await func(**args)
                    except Exception as e:
                        logger.error("Tool %s failed: %s", func_name, e)
                        result = f"Error executing {func_name}: tool query failed"
                else:
                    result = f"Unknown tool: {func_name}"

                # Add tool result to messages
                full_messages.append({
                    "role": "tool",
                    "tool_call_id": _tc_id(tc),
                    "content": result,
                })

        # If we hit max rounds, ask the model for a final summary
        full_messages.append({"role": "user", "content": "Please provide your final analysis based on the data gathered so far."})
        try:
            response = await client.chat.completions.create(
                model=SERVING_ENDPOINT,
                messages=full_messages,
                max_tokens=2048,
                temperature=0.2,
            )
            return {"response": response.choices[0].message.content or "", "tool_calls_made": tool_calls_made}
        except Exception:
            return {"response": "I gathered data but couldn't generate a final summary. Please review the tool calls above.", "tool_calls_made": tool_calls_made}


# Singleton
agent = FraudInvestigationAgent()
