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

import (
	"sort"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// SequenceObligation records issued-value tracking for one transaction-local
// sequence generation within one transaction. A generation is
// transaction-local when it exists only inside the caller's current
// transaction (on this base: created by an explicit-transaction CLONE or
// snapshot restore). Its allocations run inside the caller transaction, so a
// statement-level rollback (RollbackLastStatement) can rewind the workspace
// counter while values were already emitted to the client. The obligation
// remembers the issued values of the current CYCLE pass so a later allocation
// in the same transaction repairs the position forward instead of
// re-issuing, and so the pre-commit stamp can persist a safe position.
//
// PassStart/Floor are magnitudes in the sequence's numeric domain (stored as
// bits; interpret as int64 unless Unsigned). Minus marks descending
// direction. PassStart is the first value emitted in the current pass; Floor
// is the extreme issued value of the current pass (ascending: max;
// descending: min).
type SequenceObligation struct {
	RelID     uint64
	Db        string
	Table     string
	PassStart uint64
	Floor     uint64
	Unsigned  bool
	Minus     bool
}

// IssuedContains reports whether candidate (same numeric domain) lies inside
// the pass-issued range [PassStart, Floor] ([Floor, PassStart] descending).
// The range is a superset of the actually issued values; treating a value as
// issued only ever produces additional gaps, never a re-issue.
func (o SequenceObligation) IssuedContains(candidate uint64) bool {
	if o.Unsigned {
		if o.Minus {
			return candidate <= o.PassStart && candidate >= o.Floor
		}
		return candidate >= o.PassStart && candidate <= o.Floor
	}
	c, start, floor := int64(candidate), int64(o.PassStart), int64(o.Floor)
	if o.Minus {
		return c <= start && c >= floor
	}
	return c >= start && c <= floor
}

// Extend widens the pass-issued range to include bits, an emission the
// surviving workspace position may no longer reflect (a statement rewind
// reclaimed the counter writes of an already-observed emission). The
// frontier never retreats: a candidate strictly ahead of the floor in
// allocation order advances the floor; a candidate at or behind the pass
// start pulls the start back; a candidate already inside the range changes
// nothing. Lowering the floor here would collapse the envelope (PassStart
// ahead of the floor protects nothing), letting a later allocation re-issue
// an already-observed value and letting the pre-commit stamp persist the
// regressed position.
func (o *SequenceObligation) Extend(bits uint64) {
	if o.Minus {
		// Descending: allocation order runs toward smaller magnitudes.
		if numericDomainAfter(o.Unsigned, o.Floor, bits) {
			o.Floor = bits
		} else if numericDomainAfter(o.Unsigned, bits, o.PassStart) {
			o.PassStart = bits
		}
		return
	}
	if numericDomainAfter(o.Unsigned, bits, o.Floor) {
		o.Floor = bits
	} else if numericDomainAfter(o.Unsigned, o.PassStart, bits) {
		o.PassStart = bits
	}
}

// UnsafeToPreserve reports the preserve-or-restore stamp decision: the
// surviving commit-time position W (last=bits, called) is unsafe for the
// final pass's issued values when it sits behind the floor in allocation
// order, or equals the floor while still uncalled (the rewind cases). Any
// position at or beyond the floor is preserved unchanged.
func (o SequenceObligation) UnsafeToPreserve(wBits uint64, called bool) bool {
	if called && wBits == o.Floor {
		return false
	}
	if o.Unsigned {
		if o.Minus {
			return wBits > o.Floor || (wBits == o.Floor && !called)
		}
		return wBits < o.Floor || (wBits == o.Floor && !called)
	}
	w, floor := int64(wBits), int64(o.Floor)
	if o.Minus {
		return w > floor || (w == floor && !called)
	}
	return w < floor || (w == floor && !called)
}

// SequenceObligationSet is the session-owned store of per-(txn, identity)
// obligations. Entries are keyed by relation id. The set is bounded by the
// number of transaction-local identities allocated from in one transaction
// and is terminally cleaned at transaction end (consumed by the pre-commit
// stamp or discarded on rollback).
//
// staged holds boundary invalidations whose statement has not finished yet.
// Only a *successful* SETVAL/restart clears an obligation (J2): the function
// executes mid-statement, and when that statement later fails its workspace
// writes are rewound, so the boundary never persisted and the removal must
// roll back with it. StageRemoval hides the entry from enforcement and
// snapshot/stamp processing exactly as a removal does; statement success
// applies the staged removals, statement failure restores them (envelope-
// merged with any post-SETVAL obligations the same failed statement raised,
// so issued values of either side stay protected), and transaction rollback
// or session teardown discards both.
type SequenceObligationSet struct {
	mu      sync.Mutex
	entries map[uint64]SequenceObligation
	staged  map[uint64]SequenceObligation
}

func NewSequenceObligationSet() *SequenceObligationSet {
	return &SequenceObligationSet{entries: make(map[uint64]SequenceObligation)}
}

// Get returns the obligation for relID. A nil set reports no obligation.
func (s *SequenceObligationSet) Get(relID uint64) (SequenceObligation, bool) {
	if s == nil {
		return SequenceObligation{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ob, ok := s.entries[relID]
	return ob, ok
}

// Raise upserts entry at allocation time, before the row is emitted, so the
// record stands independently of the statement's later fate. A nil set
// cannot record the obligation; failing loudly keeps issue-once intact on
// paths that have no session-owned store.
func (s *SequenceObligationSet) Raise(entry SequenceObligation) error {
	if s == nil {
		return moerr.NewInternalErrorNoCtxf(
			"sequence obligation store is unavailable for relation %d", entry.RelID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		s.entries = make(map[uint64]SequenceObligation)
	}
	s.entries[entry.RelID] = entry
	return nil
}

// Remove drops the obligation for relID (consumed stamp; retired lifecycle).
func (s *SequenceObligationSet) Remove(relID uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, relID)
}

// mergeObligationEnvelope combines two same-identity entries into one
// (pass-start, floor) span covering both ranges. The envelope is a superset
// of the issued values of either side; like IssuedContains itself, the extra
// coverage only ever produces additional gaps, never a re-issue. The
// identity's numeric domain and direction are fixed per generation, so the
// direction fields are taken from a.
//
// Comparisons run in the identity's numeric domain, exactly as IssuedContains
// and UnsafeToPreserve do: PassStart/Floor are stored as bits, and a signed
// domain must compare as int64. A raw uint64 comparison would order every
// negative value above every non-negative one, collapsing e.g. the merge of
// [-10,-10] and [60,60] into PassStart=60, Floor=-10 — an envelope that
// protects neither side and lets a workspace rewind re-issue 60 values.
func mergeObligationEnvelope(a, b SequenceObligation) SequenceObligation {
	// aheadOf(x, y) compares magnitudes in the identity's numeric domain.
	if a.Minus {
		if numericDomainAfter(a.Unsigned, b.PassStart, a.PassStart) {
			a.PassStart = b.PassStart
		}
		if numericDomainAfter(a.Unsigned, a.Floor, b.Floor) {
			a.Floor = b.Floor
		}
	} else {
		if numericDomainAfter(a.Unsigned, a.PassStart, b.PassStart) {
			a.PassStart = b.PassStart
		}
		if numericDomainAfter(a.Unsigned, b.Floor, a.Floor) {
			a.Floor = b.Floor
		}
	}
	return a
}

// numericDomainAfter reports whether x is strictly greater than y in the
// sequence's numeric domain: bits are magnitudes only after the domain's
// comparison, so a signed domain compares as int64.
func numericDomainAfter(unsigned bool, x, y uint64) bool {
	if unsigned {
		return x > y
	}
	return int64(x) > int64(y)
}

// StageRemoval hides the obligation of relID behind a pending SETVAL
// boundary: enforcement and the pre-commit stamp treat it as removed, while
// the removed entry is kept for a possible statement failure. Staging a
// second boundary for the same identity envelope-merges into the staged
// entry, so one statement cannot lose the protection of either side.
func (s *SequenceObligationSet) StageRemoval(relID uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[relID]
	if !ok {
		return
	}
	delete(s.entries, relID)
	if s.staged == nil {
		s.staged = make(map[uint64]SequenceObligation)
	}
	if prev, hold := s.staged[relID]; hold {
		s.staged[relID] = mergeObligationEnvelope(prev, entry)
	} else {
		s.staged[relID] = entry
	}
}

// ApplyStaged makes the staged boundary invalidations permanent. The
// statement that executed the SETVAL/restart succeeded, so its reposition
// persisted in the workspace and the I4 deliberate boundary holds.
func (s *SequenceObligationSet) ApplyStaged() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.staged)
}

// RestoreStaged returns staged entries to enforcement: the statement that
// executed the SETVAL/restart failed, its workspace writes were rewound, and
// the boundary never persisted. A same-statement post-SETVAL obligation
// (raised before the failure, client-observed emission) is envelope-merged
// into the restored entry rather than displaced, so issued values on both
// sides of the failed boundary stay protected.
func (s *SequenceObligationSet) RestoreStaged() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for relID, entry := range s.staged {
		if live, ok := s.entries[relID]; ok {
			s.entries[relID] = mergeObligationEnvelope(entry, live)
		} else {
			if s.entries == nil {
				s.entries = make(map[uint64]SequenceObligation)
			}
			s.entries[relID] = entry
		}
	}
	clear(s.staged)
}

// Snapshot returns all obligations ordered by relation id for deterministic
// pre-commit processing.
func (s *SequenceObligationSet) Snapshot() []SequenceObligation {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SequenceObligation, 0, len(s.entries))
	for _, ob := range s.entries {
		out = append(out, ob)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RelID < out[j].RelID })
	return out
}

// Clear discards every obligation (transaction rollback, session teardown).
func (s *SequenceObligationSet) Clear() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[uint64]SequenceObligation)
	clear(s.staged)
}

// GetSequenceObligationSet returns the obligation set this process shares
// with its owning session, or nil when the process has none (background or
// internal executor processes that cannot host transaction-local
// allocations).
func (proc *Process) GetSequenceObligationSet() *SequenceObligationSet {
	if proc == nil || proc.Base == nil {
		return nil
	}
	return proc.Base.SessionInfo.SeqObligations
}
