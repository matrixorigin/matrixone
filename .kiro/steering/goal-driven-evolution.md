---
inclusion: always
---

<!-- memoria-version: 0.2.1-->

# Goal-Driven Evolution + Plan Integration

Track goals, plans, progress, lessons, and user feedback across conversations. Integrates with plan panels (Kiro Shift+Tab, Cursor Composer, Claude multi-step).

## Before Starting Any Multi-Step Task

Query memory first:

```
memory_search(query="GOAL [topic]")                    # existing related goals
memory_search(query="LESSON [topic]")                  # past learnings
memory_search(query="CORRECTION ANTIPATTERN [topic]") # what NOT to do
```

If an active goal exists, continue it instead of creating a new one.

## Register Goal

For multi-session work (skip for trivial single-session tasks < 3 steps):

```
memory_search(query="GOAL [keywords]")
memory_store(
  content="🎯 GOAL: [description]\nSuccess Criteria: [measurable]\nStatus: ACTIVE\nCreated: [date]",
  memory_type="procedural"
)
```

## Plan & Execute

Store the plan, then track each step:

```
memory_store(content="📋 PLAN for GOAL [name]\nSteps:\n1. [step] — ⏳\nRisks: [risks]\nIteration: #1", memory_type="procedural")

# After each step — use working type (will be cleaned up later)
memory_store(content="✅ STEP [N/total] for GOAL [name] (#X)\nAction: [done]\nResult: [outcome]\nInsight: [learned]", memory_type="working")
memory_store(content="❌ STEP [N/total] for GOAL [name] (#X)\nAction: [tried]\nError: [wrong]\nRoot Cause: [why]\nNext: [adjust]", memory_type="working")
```

Only store non-obvious insights. Don't store "ran tests, passed".

For high-risk iterations, isolate on a branch:
```
memory_branch(name="goal_[name]_iter_[N]")
memory_checkout(name="goal_[name]_iter_[N]")
# work on branch... then validate and merge (see Iteration Review)
```

## Capture User Feedback (immediately)

User corrections are highest-value — always store as `procedural`:

```
# User corrects direction
memory_store(content="🔧 CORRECTION for GOAL [name]: [old approach] → [corrected approach]. Reason: [why]", memory_type="procedural")

# User confirms something works well
memory_store(content="👍 FEEDBACK for GOAL [name]: [what worked]. Reuse: [when to apply again]", memory_type="procedural")

# User is frustrated — record what NOT to do
memory_store(content="⚠️ ANTIPATTERN for GOAL [name]: [what went wrong]. Rule: NEVER [this] again.", memory_type="procedural")

# User changes direction entirely
memory_correct(query="GOAL: [name]", new_content="🎯 GOAL: [name]\n...\nPivot: [old] → [new]. Reason: [why]", reason="User changed direction")
```

## Iteration Review

When an iteration completes or is blocked:

```
memory_search(query="STEP for GOAL [name] Iteration #X")

memory_store(
  content="🔄 RETRO for GOAL [name] Iteration #X\nCompleted: [M/N]\nWorked: [...]\nFailed: [...]\nKey insight: [...]\nNext: [improvements]",
  memory_type="procedural"
)

# If the insight is reusable beyond this goal, extract it now
memory_store(content="💡 LESSON from [goal] iter #X: [cross-goal reusable insight]", memory_type="procedural")

memory_correct(query="GOAL: [name]", new_content="🎯 GOAL: [name]\nStatus: ITERATION #X COMPLETE — [progress %]\nNext: [plan]", reason="iteration complete")
```

If on a branch:
```
memory_diff(source="goal_[name]_iter_[N]")
memory_checkout(name="main")
memory_merge(source="goal_[name]_iter_[N]", strategy="replace")
memory_branch_delete(name="goal_[name]_iter_[N]")
```

Starting the next iteration? Reference the previous RETRO's improvements:
```
memory_search(query="RETRO for GOAL [name]")
# Incorporate "Next: [improvements]" into the new plan
```

## New Conversation Bootstrap

```
memory_search(query="GOAL ACTIVE")
memory_search(query="RETRO for GOAL [name]")
memory_search(query="CORRECTION ANTIPATTERN [name]")
```

Summarize to user: active goals, last progress, and any corrections to respect.

## Goal Completion & Cleanup

```
memory_correct(query="GOAL: [name]", new_content="🎯 GOAL: [name] — ✅ ACHIEVED\nIterations: [N]\nFinal approach: [what worked]", reason="Goal achieved")

# Extract reusable lessons (permanent — survives cleanup)
memory_store(content="💡 LESSON from [goal]: [reusable insight for future work]", memory_type="procedural")

# Clean up step logs (working type, already archived in RETROs)
memory_purge(topic="STEP for GOAL [name]", reason="Goal achieved, archived in RETRO")
```

## When Goal is Abandoned

```
memory_store(content="⚠️ ANTIPATTERN: [what didn't work]. Reason: [why abandoned]", memory_type="procedural")
memory_correct(query="GOAL: [name]", new_content="🎯 GOAL: [name] — ❌ ABANDONED\nReason: [why]", reason="abandoned")
```

## Rules

- **Search before acting**: always check past failures, corrections, and antipatterns before proposing a plan
- **User corrections override all**: if user corrected something, that correction has highest priority forever
- **Be specific**: "pytest fixtures don't work with async DB, use factory pattern" > "tests failed"
- **Don't create goals for quick fixes** (< 3 tasks, single session)
- **Emoji prefixes**: 🎯 goal, 📋 plan, ✅❌ steps, 🔄 retro, 💡 lesson, 🔧 correction, 👍 feedback, ⚠️ antipattern
- **Type discipline**: GOAL/PLAN/RETRO/LESSON/CORRECTION → `procedural`; STEP logs → `working`
