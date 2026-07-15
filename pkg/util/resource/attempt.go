// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package resource

import "sync"

// AttemptState is the coordinator-owned lifecycle state.
type AttemptState uint8

const (
	AttemptOpen AttemptState = iota
	AttemptClosing
	AttemptSealed
)

type slotState uint8

const (
	slotPlanned slotState = iota
	slotDispatched
	slotTerminal
	slotMissing
)

// PublishResult classifies rejected terminal reports without allocating errors
// in the remote completion path.
type PublishResult uint8

const (
	PublishAccepted PublishResult = iota
	PublishStaleGeneration
	PublishInvalidSlot
	PublishDuplicate
	PublishAfterSeal
	PublishInvalidState
)

// Attempt reduces immutable terminal reports under one small lock. Hot-path
// producer updates never touch this object.
type Attempt struct {
	mu sync.Mutex

	generation uint64
	state      AttemptState
	fragments  []slotState
	memory     []slotState
	summary    AttemptSummary
}

// NewAttempt pre-registers dense fragment and memory-domain slots.
func NewAttempt(generation uint64, fragmentSlots, memoryDomainSlots int) *Attempt {
	if fragmentSlots < 0 {
		fragmentSlots = 0
	}
	if memoryDomainSlots < 0 {
		memoryDomainSlots = 0
	}
	return &Attempt{
		generation: generation,
		fragments:  make([]slotState, fragmentSlots),
		memory:     make([]slotState, memoryDomainSlots),
	}
}

// Generation returns the immutable retry generation.
func (a *Attempt) Generation() uint64 { return a.generation }

// MarkFragmentDispatched records that a terminal report is now required.
func (a *Attempt) MarkFragmentDispatched(slot int) PublishResult {
	return a.markDispatched(a.fragments, slot)
}

// MarkMemoryDomainDispatched records that an isolated MPool began attributable
// work and must publish a terminal summary.
func (a *Attempt) MarkMemoryDomainDispatched(slot int) PublishResult {
	return a.markDispatched(a.memory, slot)
}

func (a *Attempt) markDispatched(slots []slotState, slot int) PublishResult {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state == AttemptSealed {
		return PublishAfterSeal
	}
	if a.state != AttemptOpen {
		return PublishInvalidState
	}
	if slot < 0 || slot >= len(slots) {
		return PublishInvalidSlot
	}
	if slots[slot] != slotPlanned {
		return PublishDuplicate
	}
	slots[slot] = slotDispatched
	return PublishAccepted
}

// MarkFragmentSendFailed closes a slot that never became remotely owned. Such
// a slot contributes zero and is not missing data.
func (a *Attempt) MarkFragmentSendFailed(slot int) PublishResult {
	return a.markUndispatchedTerminal(a.fragments, slot)
}

// MarkMemoryDomainUnused closes a registered domain that never began work.
func (a *Attempt) MarkMemoryDomainUnused(slot int) PublishResult {
	return a.markUndispatchedTerminal(a.memory, slot)
}

func (a *Attempt) markUndispatchedTerminal(slots []slotState, slot int) PublishResult {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state == AttemptSealed {
		return PublishAfterSeal
	}
	if slot < 0 || slot >= len(slots) {
		return PublishInvalidSlot
	}
	if slots[slot] != slotPlanned {
		return PublishDuplicate
	}
	slots[slot] = slotTerminal
	return PublishAccepted
}

// PublishFragment merges one terminal delta exactly once.
func (a *Attempt) PublishFragment(generation uint64, slot int, delta Delta) PublishResult {
	a.mu.Lock()
	defer a.mu.Unlock()
	if generation != a.generation {
		return PublishStaleGeneration
	}
	if a.state == AttemptSealed {
		return PublishAfterSeal
	}
	if slot < 0 || slot >= len(a.fragments) {
		return PublishInvalidSlot
	}
	if a.fragments[slot] == slotTerminal || a.fragments[slot] == slotMissing {
		return PublishDuplicate
	}
	if a.fragments[slot] != slotDispatched {
		return PublishInvalidState
	}
	a.summary.Quality |= delta.Quality | MergeUsage(&a.summary.Usage, delta.Usage)
	a.fragments[slot] = slotTerminal
	return PublishAccepted
}

// PublishMemoryDomain merges one terminal isolated-MPool summary exactly once.
func (a *Attempt) PublishMemoryDomain(
	generation uint64,
	slot int,
	domain MemoryDomainSummary,
) PublishResult {
	a.mu.Lock()
	defer a.mu.Unlock()
	if generation != a.generation {
		return PublishStaleGeneration
	}
	if a.state == AttemptSealed {
		return PublishAfterSeal
	}
	if slot < 0 || slot >= len(a.memory) {
		return PublishInvalidSlot
	}
	if a.memory[slot] == slotTerminal || a.memory[slot] == slotMissing {
		return PublishDuplicate
	}
	if a.memory[slot] != slotDispatched {
		return PublishInvalidState
	}
	a.summary.Quality |= MergeMemoryDomain(&a.summary.Memory, domain)
	a.memory[slot] = slotTerminal
	return PublishAccepted
}

// AddLocal merges the quiescent attempt-local recorder before object release.
func (a *Attempt) AddLocal(delta Delta) PublishResult {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state == AttemptSealed {
		return PublishAfterSeal
	}
	a.summary.Quality |= delta.Quality | MergeUsage(&a.summary.Usage, delta.Usage)
	return PublishAccepted
}

// BeginClosing rejects new dispatch while allowing already-dispatched terminal
// reports to publish.
func (a *Attempt) BeginClosing() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != AttemptOpen {
		return false
	}
	a.state = AttemptClosing
	return true
}

// Seal marks dispatched non-terminal slots missing, closes undispatched slots
// as zero work, and returns an immutable summary. Repeated calls are idempotent.
func (a *Attempt) Seal(wallNS uint64, outcome Outcome) AttemptSummary {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state == AttemptSealed {
		return a.summary
	}
	if a.state == AttemptOpen {
		a.summary.Quality |= QualityInvariantFailure
		a.state = AttemptClosing
	}
	for i := range a.fragments {
		switch a.fragments[i] {
		case slotDispatched:
			a.fragments[i] = slotMissing
			a.summary.MissingFragmentCount, a.summary.Quality = addChecked(
				a.summary.MissingFragmentCount, 1,
				a.summary.Quality|QualityPartial|QualityMissingFragment)
		case slotPlanned:
			a.fragments[i] = slotTerminal
		}
	}
	for i := range a.memory {
		switch a.memory[i] {
		case slotDispatched:
			a.memory[i] = slotMissing
			a.summary.MissingMemoryDomainCount, a.summary.Quality = addChecked(
				a.summary.MissingMemoryDomainCount, 1,
				a.summary.Quality|QualityPartial|QualityMissingMemoryDomain)
		case slotPlanned:
			a.memory[i] = slotTerminal
		}
	}
	a.summary.WallNS = wallNS
	a.summary.Outcome = outcome
	a.state = AttemptSealed
	return a.summary
}

// State returns a synchronized lifecycle snapshot.
func (a *Attempt) State() AttemptState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state
}
