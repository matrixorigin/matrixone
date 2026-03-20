---
inclusion: auto
name: memory-hygiene
description: Memory health management - governance triggers, contradiction resolution, snapshot cleanup. Use when memory seems noisy or contradictory.
---

<!-- memoria-version: 0.2.1-->

# Memory Hygiene & Self-Governance

Proactive memory health management — don't wait for the user to ask.

## Proactive Governance Triggers

Run `memory_governance` (1h cooldown) when you notice ANY of these:
- Retrieval returns clearly outdated or contradictory results
- You stored 10+ memories in this session without any cleanup
- User mentions memory feels "noisy" or "wrong"

After governance, check the response for:
- `snapshot_health.auto_ratio > 50%` → suggest `memory_snapshot_delete(prefix="auto:")`
- Quarantined memories → inform user what was quarantined and why

## Contradiction Resolution

When you detect two memories that contradict each other:

1. `memory_search` to find both memories and their IDs
2. Determine which is newer/more accurate based on timestamps and context
3. `memory_correct` the older one with the accurate information, OR
4. `memory_purge` the wrong one if it's completely invalid
5. Never leave both — contradictions poison retrieval

Run `memory_consolidate` (30min cooldown) when:
- You found a contradiction manually
- User reports "memory says X but it should be Y" more than once in a session
- After a large batch of corrections

## Snapshot Hygiene

Snapshots accumulate from auto-saves and safety snapshots. Clean periodically:

```
memory_snapshots(limit=20)  # check current state
```

If too many:
- `memory_snapshot_delete(prefix="pre_")` — purge safety snapshots from purge/correct
- `memory_snapshot_delete(prefix="auto:")` — purge auto-generated snapshots
- `memory_snapshot_delete(older_than="<3 months ago>")` — age-based cleanup

Keep named snapshots the user created explicitly.

## Entity Graph Maintenance

Entity extraction is automatic — every `memory_store` triggers regex-based extraction, with LLM extraction as a fallback when configured. No manual intervention needed.

## Reflection Cadence

`memory_reflect` (2h cooldown) synthesizes high-level insights. Suggest it when:
- User asks "what patterns do you see" or "summarize what you know"
- `memory_search` returns a high volume of results and no reflection has been done recently
- Starting a new project phase — reflect on the previous phase first

## Memory Volume Monitoring

Watch for these signals during retrieval:
- **Too many results all relevant** → memories are too granular, suggest consolidation
- **Results mostly irrelevant** → memories may be too broad, or index needs rebuild
- **Same fact appears multiple times** → deduplication needed, use `memory_correct` to merge
