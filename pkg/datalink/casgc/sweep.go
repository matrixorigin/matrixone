// Copyright 2024 Matrix Origin
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

package casgc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

// snapshotRef identifies a live snapshot to re-scan.
type snapshotRef struct {
	Name string
	TS   int64
}

// accountRefs provides the reference data needed to sweep one account.
type accountRefs interface {
	rowScanner // scanColumn(ctx, ref, snapshotHint) ([]string, error)
	// datalinkColumns lists the account's datalink-typed columns (live schema).
	datalinkColumns(ctx context.Context) ([]columnRef, error)
	// liveSnapshots lists the account's currently-live snapshots.
	liveSnapshots(ctx context.Context) ([]snapshotRef, error)
}

// sweepEnv is the data-access boundary the Sweeper depends on.
type sweepEnv interface {
	// listAccountIDs returns all live account ids (sys view).
	listAccountIDs(ctx context.Context) ([]uint32, error)
	// refsForAccount returns the reference source scoped to one account.
	refsForAccount(ctx context.Context, accountID uint32) (accountRefs, error)
}

// Sweeper reclaims unreferenced datalink CAS blobs per account using a
// reference-aware, two-pass mark-and-delete grace window. Because the file
// service does not expose object mtime, the grace period is implemented with
// in-process memory state: a blob is deleted only after it has stayed an orphan
// across passes for at least cfg.GraceWindow.
type Sweeper struct {
	fs    fileservice.FileService // SHARED file service holding the CAS
	cfg   Config
	env   sweepEnv
	nowFn func() time.Time // injectable clock for tests

	mu      sync.Mutex
	pending map[uint32]map[string]time.Time // accountID -> hash -> first-seen-as-orphan
}

// NewSweeper builds a Sweeper over the SHARED file service holding the CAS and
// the given reference environment. cfg defaults are filled via Adjust.
func NewSweeper(fs fileservice.FileService, cfg Config, env sweepEnv) *Sweeper {
	cfg.Adjust()
	return &Sweeper{
		fs:      fs,
		cfg:     cfg,
		env:     env,
		nowFn:   time.Now,
		pending: make(map[uint32]map[string]time.Time),
	}
}

// SweepAccount runs one sweep pass for a single account and returns the number
// of blobs deleted in this pass. It is candidate-driven: snapshots are only
// scanned when there is at least one orphan candidate. A blob is deleted only
// after it has remained an orphan for at least cfg.GraceWindow across passes.
func (s *Sweeper) SweepAccount(ctx context.Context, accountID uint32) (deleted int, err error) {
	refs, err := s.env.refsForAccount(ctx, accountID)
	if err != nil {
		return 0, err
	}
	cols, err := refs.datalinkColumns(ctx)
	if err != nil {
		return 0, err
	}
	// Hashes referenced by the current (live) data.
	currentLive, err := collectHashesFromColumns(ctx, refs, cols, "")
	if err != nil {
		return 0, err
	}

	// Candidates: blobs present in the CAS but not referenced by current data.
	candidates := make(map[string]struct{})
	for ent, listErr := range datalink.CASListAccount(ctx, s.fs, accountID) {
		if listErr != nil {
			return 0, listErr
		}
		if _, live := currentLive[ent.Hash]; !live {
			candidates[ent.Hash] = struct{}{}
		}
	}

	// Confirm candidates against live snapshots. Candidate-driven: when there
	// are no candidates, snapshots are never scanned.
	orphans := candidates
	if len(candidates) > 0 {
		snaps, snapErr := refs.liveSnapshots(ctx)
		if snapErr != nil {
			return 0, snapErr
		}
		for _, snap := range snaps {
			if len(orphans) == 0 {
				break
			}
			snapHashes, scanErr := collectHashesFromColumns(
				ctx, refs, cols, "{snapshot = '"+snap.Name+"'}")
			if scanErr != nil {
				return 0, scanErr
			}
			for h := range snapHashes {
				delete(orphans, h)
			}
		}
	}

	// Two-pass grace: mark on first sighting, delete once the grace window has
	// elapsed for a continuously-orphan blob.
	s.mu.Lock()
	defer s.mu.Unlock()

	pacc := s.pending[accountID]
	if pacc == nil {
		pacc = make(map[string]time.Time)
		s.pending[accountID] = pacc
	}
	now := s.nowFn()

	for h := range orphans {
		seen, ok := pacc[h]
		if !ok {
			pacc[h] = now
			continue
		}
		if now.Sub(seen) >= s.cfg.GraceWindow {
			if delErr := datalink.CASDelete(ctx, s.fs, accountID, h); delErr != nil {
				// Return partial progress along with the error so the caller can
				// log it; pending state for h is left intact for a later retry.
				s.prune(accountID, pacc, orphans)
				return deleted, delErr
			}
			delete(pacc, h)
			deleted++
		}
	}

	// Prune entries that are no longer orphans (re-referenced or deleted) so the
	// pending map cannot leak.
	s.prune(accountID, pacc, orphans)
	return deleted, nil
}

// prune removes pending entries for hashes that are no longer orphans, and
// drops the account's pending map entirely once it is empty. Caller holds s.mu.
func (s *Sweeper) prune(accountID uint32, pacc map[string]time.Time, orphans map[string]struct{}) {
	for h := range pacc {
		if _, ok := orphans[h]; !ok {
			delete(pacc, h)
		}
	}
	if len(pacc) == 0 {
		delete(s.pending, accountID)
	}
}

// SweepAll sweeps every live account. A per-account failure is logged and does
// not abort the run; remaining accounts are still swept. It returns the total
// number of blobs deleted across all accounts and a joined error aggregating
// every per-account failure (nil if all accounts succeeded).
func (s *Sweeper) SweepAll(ctx context.Context) (deleted int, err error) {
	ids, err := s.env.listAccountIDs(ctx)
	if err != nil {
		return 0, err
	}
	var errs []error
	for _, id := range ids {
		n, accErr := s.SweepAccount(ctx, id)
		deleted += n
		if accErr != nil {
			logutil.Errorf("casgc: sweep account %d failed: %v", id, accErr)
			errs = append(errs, accErr)
		}
	}
	return deleted, errors.Join(errs...)
}
