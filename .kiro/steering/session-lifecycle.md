---
inclusion: auto
name: session-lifecycle
description: Detailed session lifecycle management - bootstrap, mid-session re-retrieval, wrap-up cleanup. Use when starting conversations or managing session state.
---

<!-- memoria-version: 0.2.1-->

# Session Lifecycle Management

Systematic memory management across conversation phases: bootstrap, mid-session, and wrap-up.

## Phase 1: Conversation Start (Bootstrap)

Before your first response, run a multi-query bootstrap to load full context:

1. **Primary query** — derive from user's message: `memory_retrieve(query="<semantic extraction>")`
2. **Active goals** — `memory_search(query="GOAL ACTIVE")` (if user's message references ongoing work, a previous task, or doesn't start a clearly new topic)
3. **User profile** — `memory_profile()` (if user asks about preferences or you need style context)

Combine retrieved context into a mental model. Flag anything that looks stale (e.g., "Currently debugging X" from days ago).

**session_id**: If the user's tool provides a session ID, pass it to `memory_retrieve` and `memory_store` throughout the conversation. This enables episodic memory and per-session retrieval boosting.

## Phase 2: Mid-Session (Active Work)

### Re-retrieval triggers

Call `memory_retrieve` again mid-conversation when:
- User shifts to a completely different topic
- You need context about something not covered in the initial bootstrap
- User references a past decision or preference you don't have loaded

### Store cadence

- Don't batch-store at the end. Store facts as they emerge — this gives each memory accurate timestamps.
- One fact per `memory_store` call. Don't combine unrelated facts into one memory.

Working memory discipline (when to store as `working`, when to promote/purge) is defined in [memory.md](memory.md) — follow those rules here.

## Phase 3: Conversation End (Wrap-Up)

When the conversation is winding down (user says thanks, goodbye, or stops engaging):

### 1. Clean up working memories

```
memory_purge(topic="<task keyword>", reason="session complete")
```

Only purge working memories for tasks that are actually done. Leave active task working memories for next session.

### 2. Promote durable findings

Any working memory that turned out to be a lasting fact should already be stored as `semantic`. Double-check: did you learn something important this session that's still only in `working`? Promote it.

### 3. Generate episodic summary (if session was substantive)

If the session involved meaningful work (not just a quick question), and `session_id` is available:

- The agent itself can synthesize a summary and store it:
```
memory_store(
  content="Session Summary: [topic]\n\nActions: [what was done]\n\nOutcome: [result/status]",
  memory_type="episodic",
  session_id="<session_id>"
)
```

### 4. Update goal status

If you were working on a tracked goal, update its status via `memory_correct`.

## Anti-Patterns

- ❌ Storing 10+ memories at conversation end in a burst — timestamps all identical, retrieval ranking suffers
- ❌ Leaving stale working memories from completed tasks — they pollute future retrieval
- ❌ Never re-retrieving mid-conversation — you miss context when topics shift
- ❌ Skipping session summary for long productive sessions — loses the high-level narrative
- ❌ Storing the same fact as both `working` and `semantic` — pick one
