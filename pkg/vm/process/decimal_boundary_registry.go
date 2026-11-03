// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

// decimal_boundary_registry.go is the process-side carrier of the
// statement-local decimal boundary registry (issue #30938, design
// docs/design/20261029-decimal-multiplication-storage-boundary.md, decision
// log entries 49-58 and 64-66). One registry belongs to one planning
// generation on the statement's compile-time process and carries:
//
//   - the decision ledger: content-keyed records of the binder's decimal
//     coercive decisions (keys and payloads are authored and consumed by the
//     planner; the carrier stores them opaquely as string -> any);
//   - the decision barrier: fold-observable content signatures of the
//     recorded decisions marked widened-multiplication provenance-bearing,
//     consulted by both constant-fold engines' recursive entries so the
//     ledger's pre-coercion witness structure survives every post-recording
//     fold;
//   - the prepared-CTAS claim publication: a nonce-keyed, read-once slot
//     handing one completed prepared-CTAS generation's ledger copy to the
//     PrepareStmt that stores the plan that generation built.
//
// The registry is consulted during planning only; it is never serialized,
// never persisted and never transmitted (remote pipelines build their own
// processes from the shipped finished plan).

// DecimalBoundaryRegistry is the planner-owned statement registry riding the
// compile-time process for one planning generation. The planner owns the key
// encoding and the payload shape; the carrier keeps both opaque.
type DecimalBoundaryRegistry struct {
	// depth marks nested generation entries sharing this registry: the
	// outermost entry installs with depth one, a nested entry increments it
	// and every release decrements, so only the outermost release restores.
	depth int
	// preparedCTAS flags that this generation authored a CTAS DDL plan
	// carrying a Query, so the outermost release stashes the completed
	// ledger into the process-side claim slot.
	preparedCTAS bool
	// decisions is the content-keyed decision ledger. Values are
	// planner-owned payloads, immutable once recorded; the prepared-CTAS
	// stash shallow-copies this map and ownership moves with the claim.
	decisions map[string]any
	// barrier holds the fold-observable content signatures of the recorded
	// decisions flagged widened-multiplication provenance-bearing. Both
	// constant-fold engines test membership at their recursive entry and
	// leave a matched subtree unfolded.
	barrier map[string]struct{}
}

// DecimalBoundaryClaimSlot is the nonce-keyed read-once publication of one
// completed prepared-CTAS generation's decision ledger. A slot becomes stale
// the moment any generation installs after the stash, and an unclaimed slot
// is discarded by the next generation's fresh install.
type DecimalBoundaryClaimSlot struct {
	// Nonce is the installing generation's identity at stash time.
	Nonce uint64
	// Ledger is the shallow copy of the generation's completed decision
	// ledger claimed by the PrepareStmt construction.
	Ledger map[string]any
}

// EnterDecimalBoundaryGeneration begins one planning generation on proc and
// returns its release. Every caller registers the release with defer so the
// field lifecycle holds on success, error and panic unwind alike.
//
// With no active generation the entry saves the field's exact prior value,
// installs a fresh empty registry, bumps the generation nonce and discards
// any unclaimed prepared-CTAS claim slot, so consecutive statements on the
// reused session process each plan against an empty registry and a failed
// build leaves nothing for the retry or the next statement. A nested entry
// (in-build view binding, the optimizer's inner BuildPlan whose fold rules
// then consult the recorded barrier, the shared SELECT funnel under the DDL
// and view owners) increments the depth and shares the active registry
// instead of overwriting it. A nil proc (test mocks) is a no-op whose
// release does nothing.
//
// The release decrements the depth on every return path and, at depth zero,
// publishes the completed ledger of a prepared-CTAS-flagged generation into
// the claim slot before restoring the saved prior field value.
func EnterDecimalBoundaryGeneration(proc *Process) func() {
	if proc == nil {
		return func() {}
	}
	if active := proc.decimalBoundaryRegistry; active != nil {
		active.depth++
		return func() { releaseDecimalBoundaryGeneration(proc, active) }
	}
	installed := &DecimalBoundaryRegistry{
		depth:     1,
		decisions: make(map[string]any),
		barrier:   make(map[string]struct{}),
	}
	proc.decimalBoundarySaved = proc.decimalBoundaryRegistry
	proc.decimalBoundaryRegistry = installed
	proc.decimalBoundaryNonce++
	proc.decimalBoundaryClaim = nil
	return func() { releaseDecimalBoundaryGeneration(proc, installed) }
}

// releaseDecimalBoundaryGeneration unwinds one entry of the entered
// generation. The outermost release of a prepared-CTAS-flagged generation
// stashes a copy of the completed statement ledger into the read-once claim
// slot keyed by the current generation nonce, then restores the registry
// field's saved prior value. A release against a registry that is no longer
// installed (unbalanced caller) does nothing instead of corrupting a later
// generation.
func releaseDecimalBoundaryGeneration(proc *Process, entered *DecimalBoundaryRegistry) {
	if proc == nil || entered == nil || proc.decimalBoundaryRegistry != entered {
		return
	}
	entered.depth--
	if entered.depth > 0 {
		return
	}
	if entered.preparedCTAS {
		stash := make(map[string]any, len(entered.decisions))
		for key, entry := range entered.decisions {
			stash[key] = entry
		}
		proc.decimalBoundaryClaim = &DecimalBoundaryClaimSlot{
			Nonce:  proc.decimalBoundaryNonce,
			Ledger: stash,
		}
	}
	proc.decimalBoundaryRegistry = proc.decimalBoundarySaved
	proc.decimalBoundarySaved = nil
}

// DecimalBoundaryGenerationDepth reports the entered nesting depth of the
// active generation on proc (zero when no generation is active). The planner
// lifecycle tests assert the depth discipline through it.
func (proc *Process) DecimalBoundaryGenerationDepth() int {
	if proc == nil || proc.decimalBoundaryRegistry == nil {
		return 0
	}
	return proc.decimalBoundaryRegistry.depth
}

// RecordDecimalBoundaryDecision appends one coercive-decision record to the
// active generation's ledger. ledgerKey is the decision's content signature;
// when the record carries widened-multiplication provenance, barrierSig —
// the decision node's fold-observable signature — joins the decision barrier
// so both fold engines preserve the decision's operand structure. A repeat
// recording of the identical content key (a copy resolving its origin's
// entry, or a retried family binding recording its retried decision)
// replaces the payload with the identical observable content; distinct
// decisions never share a key because the key encodes every observed
// component. Without an active generation the recording is dropped: a
// planner path that records outside a generation has no ledger to serve and
// the boundary evaluation conservatively degrades to its declared-envelope
// miss behavior.
func (proc *Process) RecordDecimalBoundaryDecision(ledgerKey string, entry any, barrierSig string, provenance bool) {
	if proc == nil || proc.decimalBoundaryRegistry == nil {
		return
	}
	registry := proc.decimalBoundaryRegistry
	registry.decisions[ledgerKey] = entry
	if provenance && barrierSig != "" {
		registry.barrier[barrierSig] = struct{}{}
	}
}

// DecimalBoundaryDecision returns the recorded payload for one content key.
func (proc *Process) DecimalBoundaryDecision(ledgerKey string) (any, bool) {
	if proc == nil || proc.decimalBoundaryRegistry == nil {
		return nil, false
	}
	entry, ok := proc.decimalBoundaryRegistry.decisions[ledgerKey]
	return entry, ok
}

// DecimalBoundaryDecisions exposes the active generation's live ledger for
// planner iteration (pass-entry capture, coverage probes). Callers must not
// mutate the returned map. It returns nil without an active generation.
func (proc *Process) DecimalBoundaryDecisions() map[string]any {
	if proc == nil || proc.decimalBoundaryRegistry == nil {
		return nil
	}
	return proc.decimalBoundaryRegistry.decisions
}

// DecimalBoundaryBarrierContains reports whether barrierSig matches a
// recorded provenance-bearing decision of the active generation. A statement
// whose recorded decisions carry no provenance flag — and every statement on
// a process without an active generation — answers from the empty set, so
// the fold engines' per-entry cost stays one O(1) membership check.
func (proc *Process) DecimalBoundaryBarrierContains(barrierSig string) bool {
	if proc == nil || proc.decimalBoundaryRegistry == nil ||
		len(proc.decimalBoundaryRegistry.barrier) == 0 {
		return false
	}
	_, hit := proc.decimalBoundaryRegistry.barrier[barrierSig]
	return hit
}

// DecimalBoundaryBarrierActive reports whether the active generation's
// barrier holds any signature, letting the fold engines skip the signature
// computation itself — not only the membership test — for every statement
// whose recorded decisions carry no provenance flag.
func (proc *Process) DecimalBoundaryBarrierActive() bool {
	return proc != nil && proc.decimalBoundaryRegistry != nil &&
		len(proc.decimalBoundaryRegistry.barrier) > 0
}

// MarkDecimalBoundaryPreparedCTAS flags that the active generation built a
// CTAS DDL plan carrying a Query, so its outermost release publishes the
// completed ledger into the claim slot.
func (proc *Process) MarkDecimalBoundaryPreparedCTAS() {
	if proc == nil || proc.decimalBoundaryRegistry == nil {
		return
	}
	proc.decimalBoundaryRegistry.preparedCTAS = true
}

// ClaimDecimalBoundaryEvidence performs the read-once claim of the published
// prepared-CTAS ledger. The claim succeeds only while the slot's nonce still
// equals the process's current generation nonce — no generation has
// installed since the stash — and it clears the slot, so every successful
// claim owns the sole copy. A stale or absent slot reports false and leaves
// the discard to the next fresh install.
func (proc *Process) ClaimDecimalBoundaryEvidence() (map[string]any, bool) {
	if proc == nil || proc.decimalBoundaryClaim == nil {
		return nil, false
	}
	if proc.decimalBoundaryClaim.Nonce != proc.decimalBoundaryNonce {
		return nil, false
	}
	ledger := proc.decimalBoundaryClaim.Ledger
	proc.decimalBoundaryClaim = nil
	return ledger, true
}

// DecimalBoundaryGenerationNonce exposes the process's generation counter
// for the cached-generation reuse witness's nonce-identity assertions.
func (proc *Process) DecimalBoundaryGenerationNonce() uint64 {
	if proc == nil {
		return 0
	}
	return proc.decimalBoundaryNonce
}

// HasUnclaimedDecimalBoundaryEvidence reports whether a prepared-CTAS
// publication waits unclaimed on the process.
func (proc *Process) HasUnclaimedDecimalBoundaryEvidence() bool {
	return proc != nil && proc.decimalBoundaryClaim != nil
}
