---
inclusion: always
---

<!-- trustmem-version: 0.2.2 -->

# Memory Integration (TrustMem Lite)

You have access to a shared memory service via MCP tools (TrustMem Lite — local single-user mode). Use it proactively:

## 🔴 MANDATORY: Start of every conversation
**ALWAYS call `memory_retrieve` with the user's first message before responding.**
This is not optional. Without this, you have no memory of past interactions.
If the response includes ⚠️ Memory health warnings, proactively inform the user and offer to help clean up.

## 🔴 MANDATORY: End of every conversation turn
After each response, check if the turn contained anything worth remembering:
- User stated a preference, fact, constraint, or decision → `memory_store`
- User corrected something you said → `memory_store` with the correction
- You learned something new about the user's project/workflow → `memory_store`

Do NOT store: greetings, trivial questions, things already in memory.

## Other triggers
- User says a stored memory is wrong → `memory_correct`
- User asks to forget something → `memory_purge` (single ID or by topic)
- User says "forget everything about X" → `memory_purge` with `topic="X"` to bulk-delete
- User asks "what do you know about me" → `memory_profile`

## Memory types
- `profile`: user/agent profiles
- `semantic`: project facts, technical decisions, architecture choices (default)
- `procedural`: how-to knowledge, workflows, processes
- `working`: temporary context for current task
- `tool_result`: results from tool executions
