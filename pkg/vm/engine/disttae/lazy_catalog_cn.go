// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type catalogReadyState int

const (
	accountInactive catalogReadyState = iota
	accountCatchingUp
	accountReadyDraining
	accountReady
)

type accountCatalogEntry struct {
	state   catalogReadyState
	readyTS timestamp.Timestamp
}

type activationResponseKey struct {
	accountID uint32
	seq       uint64
}

// lazyCatalogCNState holds per-account catalog readiness state for a CN.
// All fields are guarded by mu unless noted otherwise.
type lazyCatalogCNState struct {
	mu sync.RWMutex

	enabled atomic.Bool

	accounts map[uint32]*accountCatalogEntry

	// pendingSeq maps accountID → seq for in-flight activation requests.
	pendingSeq map[uint32]uint64

	// accountDCA holds delayed cache-apply closures for accounts in
	// catching_up state. Flushed after full replay.
	accountDCA map[uint32][]func()

	// wantedAccounts records which accounts were ready before a reconnect,
	// so the reconnect subscribe can restore them in one batch.
	wantedAccounts map[uint32]struct{}

	// pendingActivationResponses delivers activation responses from the
	// receive loop to the waiting ActivateTenantCatalog caller.
	pendingActivationResponses map[activationResponseKey]chan *logtail.ActivateAccountForCatalogResponse

	// seqCounter allocates monotonically increasing seq values.
	seqCounter atomic.Uint64

	// catchingUpCount is a fast-path counter. Positive means at least one
	// account is in catching_up state, so consumeEntry should check
	// per-account DCA. It is updated while holding mu, then published through
	// the atomic for cheap read-side checks.
	catchingUpCount atomic.Int32

	// inflightActivations prevents duplicate concurrent activations for the
	// same account. Maps accountID → *inflightActivation.
	inflightActivations sync.Map

	// activationHistory keeps a bounded recent history of structured
	// activation/startup/reconnect events for debug introspection.
	activationHistory []DebugCatalogActivationEvent
}

type inflightActivation struct {
	done chan struct{}
	err  error
}

func newLazyCatalogCNState() *lazyCatalogCNState {
	return &lazyCatalogCNState{
		accounts:                   make(map[uint32]*accountCatalogEntry),
		pendingSeq:                 make(map[uint32]uint64),
		accountDCA:                 make(map[uint32][]func()),
		wantedAccounts:             make(map[uint32]struct{}),
		pendingActivationResponses: make(map[activationResponseKey]chan *logtail.ActivateAccountForCatalogResponse),
	}
}

func (s *lazyCatalogCNState) enable() {
	s.enabled.Store(true)
}

func (s *lazyCatalogCNState) isEnabled() bool {
	return s.enabled.Load()
}

func (s *lazyCatalogCNState) isAccountReady(accountID uint32) bool {
	if !s.enabled.Load() {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.accounts[accountID]
	return ok && e.state == accountReady
}

func (s *lazyCatalogCNState) getAccountReadyTS(accountID uint32) (timestamp.Timestamp, bool) {
	if !s.enabled.Load() {
		return timestamp.Timestamp{}, true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.accounts[accountID]
	if !ok || e.state != accountReady {
		return timestamp.Timestamp{}, false
	}
	return e.readyTS, true
}

// beginCatchingUp transitions the account to catching_up and returns a new seq.
// The caller should only call this under singleflight protection.
func (s *lazyCatalogCNState) beginCatchingUp(accountID uint32) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	seq := s.seqCounter.Add(1)
	old := s.accounts[accountID]
	if old == nil || old.state != accountCatchingUp {
		s.catchingUpCount.Add(1)
	}
	s.accounts[accountID] = &accountCatalogEntry{state: accountCatchingUp}
	s.pendingSeq[accountID] = seq
	s.accountDCA[accountID] = nil
	return seq
}

// setAccountReady transitions the account to ready, sets its readyTS, and
// records it in wantedAccounts for reconnect survival. Startup/reconnect use
// this directly because they do not rely on per-account DCA.
func (s *lazyCatalogCNState) setAccountReady(accountID uint32, readyTS timestamp.Timestamp) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.accounts[accountID]
	if e == nil {
		e = &accountCatalogEntry{}
		s.accounts[accountID] = e
	}
	if e.state == accountCatchingUp {
		s.catchingUpCount.Add(-1)
	}
	e.state = accountReady
	e.readyTS = readyTS
	delete(s.pendingSeq, accountID)
	s.wantedAccounts[accountID] = struct{}{}
}

// delayAccountCacheApply queues f for later flush if the account is currently
// catching_up. Returns true if delayed, false if the caller should apply now.
func (s *lazyCatalogCNState) delayAccountCacheApply(accountID uint32, f func()) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.accounts[accountID]
	if !ok || e.state != accountCatchingUp {
		return false
	}
	s.accountDCA[accountID] = append(s.accountDCA[accountID], f)
	return true
}

// beginAccountReadyTransition detaches any delayed cache-apply closures and
// moves the account out of catching_up, but does not publish readyTS yet. The
// caller must finish the transition under catalogCacheMu after the detached
// closures have run.
func (s *lazyCatalogCNState) beginAccountReadyTransition(accountID uint32) []func() {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.accounts[accountID]
	if e == nil {
		e = &accountCatalogEntry{}
		s.accounts[accountID] = e
	}
	if e.state == accountCatchingUp {
		s.catchingUpCount.Add(-1)
	}
	e.state = accountReadyDraining
	delete(s.pendingSeq, accountID)

	fns := s.accountDCA[accountID]
	delete(s.accountDCA, accountID)
	return fns
}

func (s *lazyCatalogCNState) finishAccountReady(accountID uint32, readyTS timestamp.Timestamp) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.accounts[accountID]
	if e == nil {
		e = &accountCatalogEntry{}
		s.accounts[accountID] = e
	}
	e.state = accountReady
	e.readyTS = readyTS
	s.wantedAccounts[accountID] = struct{}{}
}

// cleanupFailedActivation reverts a catching_up account to inactive if the seq
// still matches. Returns true if cleanup was performed.
func (s *lazyCatalogCNState) cleanupFailedActivation(accountID uint32, seq uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.pendingSeq[accountID]
	if !ok || current != seq {
		return false
	}

	delete(s.pendingSeq, accountID)
	delete(s.pendingActivationResponses, activationResponseKey{accountID: accountID, seq: seq})
	s.accountDCA[accountID] = nil

	e := s.accounts[accountID]
	if e != nil && e.state == accountCatchingUp {
		e.state = accountInactive
		s.catchingUpCount.Add(-1)
	}
	return true
}

// matchesPendingSeq checks whether a pending activation seq matches.
func (s *lazyCatalogCNState) matchesPendingSeq(accountID uint32, seq uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pendingSeq[accountID] == seq
}

// snapshotWantedAccounts returns a copy of the wantedAccounts set, suitable
// for passing to subSysTables during reconnect.
func (s *lazyCatalogCNState) snapshotWantedAccounts() []uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]uint32, 0, len(s.wantedAccounts))
	for id := range s.wantedAccounts {
		result = append(result, id)
	}
	return result
}

// collectWantedAccounts snapshots all currently ready accounts into
// wantedAccounts. Called before reconnect.
func (s *lazyCatalogCNState) collectWantedAccounts() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.wantedAccounts = make(map[uint32]struct{})
	for id, e := range s.accounts {
		if e.state == accountReady {
			s.wantedAccounts[id] = struct{}{}
		}
	}
}

// resetAllStates clears all per-account state. Called on reconnect after
// wantedAccounts has been collected.
func (s *lazyCatalogCNState) resetAllStates() {
	s.mu.Lock()
	pendingResponses := make([]chan *logtail.ActivateAccountForCatalogResponse, 0, len(s.pendingActivationResponses))
	for _, ch := range s.pendingActivationResponses {
		pendingResponses = append(pendingResponses, ch)
	}

	s.accounts = make(map[uint32]*accountCatalogEntry)
	s.pendingSeq = make(map[uint32]uint64)
	s.accountDCA = make(map[uint32][]func())
	s.pendingActivationResponses = make(map[activationResponseKey]chan *logtail.ActivateAccountForCatalogResponse)
	s.catchingUpCount.Store(0)
	s.inflightActivations = sync.Map{}
	s.mu.Unlock()

	for _, ch := range pendingResponses {
		select {
		case ch <- nil:
		default:
		}
	}
}

// registerPendingResponse creates a buffered channel for delivering the
// activation response from the receive loop to the ActivateTenantCatalog
// caller.
func (s *lazyCatalogCNState) registerPendingResponse(
	accountID uint32,
	seq uint64,
) chan *logtail.ActivateAccountForCatalogResponse {
	ch := make(chan *logtail.ActivateAccountForCatalogResponse, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingActivationResponses[activationResponseKey{accountID: accountID, seq: seq}] = ch
	return ch
}

// deliverActivationResponse sends the activation response to the waiting
// caller. Returns false if no caller is waiting.
func (s *lazyCatalogCNState) deliverActivationResponse(resp *logtail.ActivateAccountForCatalogResponse) bool {
	key := activationResponseKey{accountID: resp.GetAccountId(), seq: resp.GetSeq()}
	s.mu.Lock()
	ch, ok := s.pendingActivationResponses[key]
	if ok {
		delete(s.pendingActivationResponses, key)
	}
	s.mu.Unlock()
	if !ok {
		return false
	}
	ch <- resp
	return true
}

func (s *lazyCatalogCNState) hasCatchingUp() bool {
	return s.catchingUpCount.Load() > 0
}

func isLazyCatalogTableID(tableID uint64) bool {
	return catalog.IsLazyCatalogTableID(tableID)
}

// shouldDelayCatalogCacheApplyEntry routes a pushed in-memory catalog entry by
// account. This intentionally relies on the current lazy-catalog assumption
// that pushed entry batches for the three catalog tables belong to a single
// account; subscribe snapshots are filtered separately on the TN side.
func (s *lazyCatalogCNState) shouldDelayCatalogCacheApplyEntry(
	entry api.Entry,
) (uint32, bool, error) {
	if !s.isEnabled() || !isLazyCatalogTableID(entry.TableId) {
		return 0, false, nil
	}

	accountID, ok, err := accountIDFromCatalogCacheApplyEntry(entry)
	if err != nil || !ok {
		return 0, false, err
	}
	return accountID, s.isAccountCatchingUp(accountID), nil
}

func (s *lazyCatalogCNState) isAccountCatchingUp(accountID uint32) bool {
	if s.catchingUpCount.Load() == 0 {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry := s.accounts[accountID]
	return entry != nil && entry.state == accountCatchingUp
}

func accountIDFromCatalogCacheApplyEntry(entry api.Entry) (uint32, bool, error) {
	return catalog.LazyCatalogEntryAccountID(entry)
}
