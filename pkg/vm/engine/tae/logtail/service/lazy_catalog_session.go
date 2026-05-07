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

package service

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
)

type lazyCatalogAllowedAccounts struct {
	accounts map[uint32]struct{}
}

func newLazyCatalogAllowedAccounts(accountIDs ...uint32) *lazyCatalogAllowedAccounts {
	allowed := &lazyCatalogAllowedAccounts{
		accounts: make(map[uint32]struct{}, len(accountIDs)),
	}
	for _, accountID := range accountIDs {
		allowed.accounts[accountID] = struct{}{}
	}
	return allowed
}

func (s *lazyCatalogAllowedAccounts) contains(accountID uint32) bool {
	if s == nil {
		return false
	}
	_, ok := s.accounts[accountID]
	return ok
}

// lazyCatalogFilterState keeps the per-session tenant filter state for the three
// catalog system tables. The whole struct is guarded by Session.mu.
type lazyCatalogFilterState struct {
	enabled bool

	// activeAccounts are already allowed in steady-state push.
	activeAccounts map[uint32]struct{}
	// activeAccountsSnapshot is rebuilt only when the active set changes so the
	// publish fast path can read a stable view without cloning on every push.
	activeAccountsSnapshot atomic.Pointer[lazyCatalogAllowedAccounts]
	// activatingSeqByAccount belongs to the later tn-activation-sender step.
	// The tn-filter refactor keeps the state slot here, but does not manipulate it yet.
	activatingSeqByAccount map[uint32]uint64
}

func (s *lazyCatalogFilterState) ensureActiveAccounts() {
	if s.activeAccounts == nil {
		s.activeAccounts = make(map[uint32]struct{})
	}
}

func (s *lazyCatalogFilterState) configure(initialActiveAccounts []uint32) {
	s.enabled = true
	s.ensureActiveAccounts()
	for _, accountID := range initialActiveAccounts {
		s.activeAccounts[accountID] = struct{}{}
	}
	s.storeActiveAccountsSnapshot()
}

func (s *lazyCatalogFilterState) storeActiveAccountsSnapshot() {
	snapshot := &lazyCatalogAllowedAccounts{
		accounts: make(map[uint32]struct{}, len(s.activeAccounts)),
	}
	for accountID := range s.activeAccounts {
		snapshot.accounts[accountID] = struct{}{}
	}
	s.activeAccountsSnapshot.Store(snapshot)
}

// beginActivation records that an activation is in progress for the given
// account with the given seq. Returns false if the session is not in lazy
// catalog mode.
func (s *lazyCatalogFilterState) beginActivation(accountID uint32, seq uint64) bool {
	if !s.enabled {
		return false
	}
	if s.activatingSeqByAccount == nil {
		s.activatingSeqByAccount = make(map[uint32]uint64)
	}
	s.activatingSeqByAccount[accountID] = seq
	return true
}

// completeActivation finalises an activation: verifies the seq still matches,
// adds the account to activeAccounts, and removes the pending entry. Returns
// false if a newer seq has superseded this one (stale activation).
func (s *lazyCatalogFilterState) completeActivation(accountID uint32, seq uint64) bool {
	if !s.enabled {
		return false
	}
	current, ok := s.activatingSeqByAccount[accountID]
	if !ok || current != seq {
		return false
	}
	delete(s.activatingSeqByAccount, accountID)
	s.ensureActiveAccounts()
	s.activeAccounts[accountID] = struct{}{}
	s.storeActiveAccountsSnapshot()
	return true
}

func (s *lazyCatalogFilterState) abortActivation(accountID uint32, seq uint64) bool {
	if !s.enabled {
		return false
	}
	current, ok := s.activatingSeqByAccount[accountID]
	if !ok || current != seq {
		return false
	}
	delete(s.activatingSeqByAccount, accountID)
	return true
}

func (ss *Session) configureLazyCatalogSubscription(req *logtail.SubscribeRequest) error {
	if !isLazyCatalogSubscribe(req) {
		return nil
	}
	if !isLazyCatalogTableID(req.GetTable()) {
		return moerr.NewNotSupportedNoCtxf(
			"lazy catalog subscribe only supports mo_database/mo_tables/mo_columns, got %v",
			req.GetTable(),
		)
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.lazyCatalog.configure(req.GetInitialActiveAccounts())
	return nil
}

func (ss *Session) snapshotLazyCatalogActiveAccountsForFilter() (*lazyCatalogAllowedAccounts, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	if !ss.lazyCatalog.enabled {
		return nil, false
	}
	return ss.lazyCatalog.activeAccountsSnapshot.Load(), true
}

func (ss *Session) lazyCatalogSubscribeAccountsForFilter(
	req *logtail.SubscribeRequest,
) (*lazyCatalogAllowedAccounts, bool) {
	if !isLazyCatalogSubscribe(req) {
		return nil, false
	}
	return ss.snapshotLazyCatalogActiveAccountsForFilter()
}

func (ss *Session) beginLazyCatalogActivation(accountID uint32, seq uint64) bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.lazyCatalog.beginActivation(accountID, seq)
}

func (ss *Session) completeLazyCatalogActivation(accountID uint32, seq uint64) bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.lazyCatalog.completeActivation(accountID, seq)
}

func (ss *Session) abortLazyCatalogActivation(accountID uint32, seq uint64) bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.lazyCatalog.abortActivation(accountID, seq)
}

func (ss *Session) prepareLazyCatalogPublishWrapsFromIndex(
	wraps []wrapLogtail,
	firstLazyIndex int,
) ([]wrapLogtail, error) {
	// Hot path fast path: non-catalog events should return before taking locks or
	// cloning account state.
	if firstLazyIndex < 0 {
		return wraps, nil
	}

	allowedAccounts, ok := ss.snapshotLazyCatalogActiveAccountsForFilter()
	if !ok {
		return wraps, nil
	}
	return rewriteLazyCatalogPublishWraps(wraps, firstLazyIndex, allowedAccounts)
}

// rewriteLazyCatalogPublishWraps assumes firstLazyIndex already points at the
// first lazy-catalog table in wraps, so the caller can keep the hot-path scan
// outside the rewrite loop.
func rewriteLazyCatalogPublishWraps(
	wraps []wrapLogtail,
	firstLazyIndex int,
	allowedAccounts *lazyCatalogAllowedAccounts,
) ([]wrapLogtail, error) {
	var filtered []wrapLogtail
	for idx := firstLazyIndex; idx < len(wraps); idx++ {
		wrap := wraps[idx]
		if !isLazyCatalogTableID(wrap.tail.Table) {
			if filtered != nil {
				filtered = append(filtered, wrap)
			}
			continue
		}

		filteredTail, changed, err := filterLazyCatalogPublishRowsInTail(wrap.tail, allowedAccounts)
		if err != nil {
			return nil, err
		}
		if filtered == nil {
			if !changed {
				continue
			}
			filtered = make([]wrapLogtail, 0, len(wraps))
			filtered = append(filtered, wraps[:idx]...)
		}
		if isEmptyLogtail(filteredTail) {
			continue
		}
		filtered = append(filtered, wrapLogtail{id: wrap.id, tail: filteredTail})
	}
	if filtered == nil {
		return wraps, nil
	}
	return filtered, nil
}

// --- lazy catalog scope helpers ---

func isLazyCatalogTableID(table *api.TableID) bool {
	return table != nil && catalog.IsLazyCatalogTableID(table.TbId)
}

func isLazyCatalogSubscribe(req *logtail.SubscribeRequest) bool {
	return req != nil && req.GetLazyCatalog()
}

func isEmptyLogtail(tail logtail.TableLogtail) bool {
	return tail.CkpLocation == "" && len(tail.Commands) == 0
}
