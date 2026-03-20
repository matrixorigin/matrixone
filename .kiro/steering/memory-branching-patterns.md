---
inclusion: auto
name: memory-branching
description: Git-like branching for memory - isolated experiments, tech evaluation, A/B comparison. Use when exploring alternatives or risky changes.
---

<!-- memoria-version: 0.2.1-->

# Memory Branching Patterns

Use Memoria's Git-like branching to isolate experiments, evaluate alternatives, and protect stable memory state.

## Pattern 1: Tech Evaluation

Compare alternatives without polluting main memory.

```
memory_branch(name="eval_[technology]")
memory_checkout(name="eval_[technology]")

# Store findings on branch
memory_store(content="[technology] evaluation: [findings]", memory_type="semantic")

# When done — preview and decide
memory_diff(source="eval_[technology]")

# Accept: merge back
memory_checkout(name="main")
memory_merge(source="eval_[technology]", strategy="replace")
memory_branch_delete(name="eval_[technology]")

# Reject: just delete
memory_checkout(name="main")
memory_branch_delete(name="eval_[technology]")
```

## Pattern 2: Pre-Refactor Safety Net

Snapshot + branch before risky memory changes.

```
memory_snapshot(name="pre_[task]", description="before [task]")
memory_branch(name="refactor_[task]")
memory_checkout(name="refactor_[task]")

# Do risky work on branch...

# If it goes wrong:
memory_checkout(name="main")
memory_branch_delete(name="refactor_[task]")
# main is untouched

# If it succeeds:
memory_diff(source="refactor_[task]")
memory_checkout(name="main")
memory_merge(source="refactor_[task]")  # default strategy: branch wins on conflicts
memory_branch_delete(name="refactor_[task]")
```

## Pattern 3: A/B Memory Comparison

Two branches for competing approaches, diff both before deciding.

```
memory_branch(name="approach_a")
memory_branch(name="approach_b")

# Work on A
memory_checkout(name="approach_a")
memory_store(content="Approach A: [details]", memory_type="semantic")

# Work on B
memory_checkout(name="approach_b")
memory_store(content="Approach B: [details]", memory_type="semantic")

# Compare
memory_diff(source="approach_a")
memory_diff(source="approach_b")

# Merge winner, delete both
memory_checkout(name="main")
memory_merge(source="approach_a")  # default strategy: branch wins on conflicts
memory_branch_delete(name="approach_a")
memory_branch_delete(name="approach_b")
```

## When to Branch

- ✅ Evaluating a technology, framework, or architecture change
- ✅ About to bulk-correct or purge many memories
- ✅ User says "let's try something different" or "what if we..."
- ✅ Exploring a hypothesis that might be wrong
- ❌ Simple fact storage — just use main
- ❌ Quick corrections — use `memory_correct` directly

## Naming Convention

- `eval_[thing]` — technology/approach evaluation
- `refactor_[task]` — risky memory restructuring
- `goal_[name]_iter_[N]` — goal iteration (see goal-driven-evolution)
- `experiment_[topic]` — open-ended exploration

## Cleanup

Always delete branches after merge or abandonment. Check with `memory_branches()` periodically. Stale branches waste cognitive overhead when listed.

## Merge Strategies

- `replace` (default, also called `accept`): branch wins on conflicts — if the same memory exists on both main and branch, the branch version replaces main's
- `append`: skip-on-conflict — only adds new memories from branch, never overwrites existing main memories

Use `replace` when the branch contains validated corrections. Use `append` when the branch only adds new information and you want to preserve main's existing state.
