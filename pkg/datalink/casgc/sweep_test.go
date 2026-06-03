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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

const testAccountID = uint32(7)

func newTestCASFS(t *testing.T) fileservice.FileService {
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	return fs
}

// datalinkURL builds a pinned datalink URL string carrying the given contenthash.
func datalinkURL(hash string) string {
	return fmt.Sprintf("mo://bucket/path?contenthash=%s", hash)
}

// fakeRefs fakes accountRefs for one account. It records calls so tests can
// assert candidate-driven behavior.
type fakeRefs struct {
	cols []columnRef
	// liveValues are the raw column values returned for snapshotHint=="".
	liveValues []string
	// snapValues maps a snapshot name to the raw column values returned for it.
	snapValues map[string][]string
	snaps      []snapshotRef

	// error injection
	colsErr error
	scanErr error
	snapErr error

	// call recording
	scanCalls         int
	liveSnapshotCalls int
	snapScanHints     []string
}

func (f *fakeRefs) datalinkColumns(ctx context.Context) ([]columnRef, error) {
	if f.colsErr != nil {
		return nil, f.colsErr
	}
	return f.cols, nil
}

func (f *fakeRefs) liveSnapshots(ctx context.Context) ([]snapshotRef, error) {
	f.liveSnapshotCalls++
	if f.snapErr != nil {
		return nil, f.snapErr
	}
	return f.snaps, nil
}

func (f *fakeRefs) scanColumn(ctx context.Context, ref columnRef, snapshotHint string) ([]string, error) {
	f.scanCalls++
	if f.scanErr != nil {
		return nil, f.scanErr
	}
	if snapshotHint == "" {
		return f.liveValues, nil
	}
	f.snapScanHints = append(f.snapScanHints, snapshotHint)
	// snapValues is keyed by snapshot name; recover it from the hint.
	for name, vals := range f.snapValues {
		if snapshotHint == "{snapshot = '"+name+"'}" {
			return vals, nil
		}
	}
	return nil, nil
}

// fakeEnv fakes sweepEnv with a fixed set of accounts.
type fakeEnv struct {
	ids       []uint32
	refs      map[uint32]*fakeRefs
	refsErr   map[uint32]error
	idsErr    error
	refsCalls []uint32
}

func (e *fakeEnv) listAccountIDs(ctx context.Context) ([]uint32, error) {
	if e.idsErr != nil {
		return nil, e.idsErr
	}
	return e.ids, nil
}

func (e *fakeEnv) refsForAccount(ctx context.Context, accountID uint32) (accountRefs, error) {
	e.refsCalls = append(e.refsCalls, accountID)
	if err, ok := e.refsErr[accountID]; ok && err != nil {
		return nil, err
	}
	return e.refs[accountID], nil
}

func newSweeper(t *testing.T, fs fileservice.FileService, env sweepEnv) (*Sweeper, *fakeClock) {
	cfg := Config{Interval: time.Hour, GraceWindow: 24 * time.Hour}
	s := NewSweeper(fs, cfg, env)
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	s.nowFn = clk.Now
	return s, clk
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

// 1. Unreferenced blob deleted only after grace.
func TestSweepAccount_UnreferencedDeletedAfterGrace(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	hash, err := datalink.CASPut(ctx, fs, testAccountID, []byte("orphan blob"))
	require.NoError(t, err)

	refs := &fakeRefs{cols: []columnRef{{DBName: "db", TableName: "t", ColName: "c"}}}
	env := &fakeEnv{ids: []uint32{testAccountID}, refs: map[uint32]*fakeRefs{testAccountID: refs}}
	s, clk := newSweeper(t, fs, env)

	// First pass: only marks, deletes nothing.
	deleted, err := s.SweepAccount(ctx, testAccountID)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)
	ok, err := datalink.CASExists(ctx, fs, testAccountID, hash)
	require.NoError(t, err)
	require.True(t, ok)

	// Advance past grace and sweep again: now deleted.
	clk.advance(24 * time.Hour)
	deleted, err = s.SweepAccount(ctx, testAccountID)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	ok, err = datalink.CASExists(ctx, fs, testAccountID, hash)
	require.NoError(t, err)
	require.False(t, ok)
}

// 2. Referenced blob never deleted.
func TestSweepAccount_ReferencedNeverDeleted(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	hash, err := datalink.CASPut(ctx, fs, testAccountID, []byte("live blob"))
	require.NoError(t, err)

	refs := &fakeRefs{
		cols:       []columnRef{{DBName: "db", TableName: "t", ColName: "c"}},
		liveValues: []string{datalinkURL(hash)},
	}
	env := &fakeEnv{ids: []uint32{testAccountID}, refs: map[uint32]*fakeRefs{testAccountID: refs}}
	s, clk := newSweeper(t, fs, env)

	for i := 0; i < 3; i++ {
		deleted, err := s.SweepAccount(ctx, testAccountID)
		require.NoError(t, err)
		require.Equal(t, 0, deleted)
		clk.advance(48 * time.Hour)
	}
	ok, err := datalink.CASExists(ctx, fs, testAccountID, hash)
	require.NoError(t, err)
	require.True(t, ok)

	// Referenced => never a candidate => snapshots never scanned.
	require.Equal(t, 0, refs.liveSnapshotCalls)
}

// 3. Snapshot keeps blob; and with zero candidates snapshots are not scanned.
func TestSweepAccount_SnapshotKeepsBlob(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	hash, err := datalink.CASPut(ctx, fs, testAccountID, []byte("snapshotted blob"))
	require.NoError(t, err)

	refs := &fakeRefs{
		cols:       []columnRef{{DBName: "db", TableName: "t", ColName: "c"}},
		liveValues: nil, // absent from current data
		snaps:      []snapshotRef{{Name: "snap1", TS: 1}},
		snapValues: map[string][]string{"snap1": {datalinkURL(hash)}},
	}
	env := &fakeEnv{ids: []uint32{testAccountID}, refs: map[uint32]*fakeRefs{testAccountID: refs}}
	s, clk := newSweeper(t, fs, env)

	for i := 0; i < 3; i++ {
		deleted, err := s.SweepAccount(ctx, testAccountID)
		require.NoError(t, err)
		require.Equal(t, 0, deleted)
		clk.advance(48 * time.Hour)
	}
	ok, err := datalink.CASExists(ctx, fs, testAccountID, hash)
	require.NoError(t, err)
	require.True(t, ok)
	// There WAS a candidate, so snapshots must have been consulted.
	require.Greater(t, refs.liveSnapshotCalls, 0)
	require.NotEmpty(t, refs.snapScanHints)
}

// 3b. Candidate-driven: zero candidates => snapshots never scanned.
func TestSweepAccount_NoCandidatesSkipsSnapshots(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t) // empty CAS namespace

	refs := &fakeRefs{
		cols:  []columnRef{{DBName: "db", TableName: "t", ColName: "c"}},
		snaps: []snapshotRef{{Name: "snap1", TS: 1}},
	}
	env := &fakeEnv{ids: []uint32{testAccountID}, refs: map[uint32]*fakeRefs{testAccountID: refs}}
	s, _ := newSweeper(t, fs, env)

	deleted, err := s.SweepAccount(ctx, testAccountID)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)
	require.Equal(t, 0, refs.liveSnapshotCalls)
	require.Empty(t, refs.snapScanHints)
}

// 4. Re-reference resets the grace timer.
func TestSweepAccount_ReReferenceResetsGrace(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	hash, err := datalink.CASPut(ctx, fs, testAccountID, []byte("flapping blob"))
	require.NoError(t, err)

	refs := &fakeRefs{cols: []columnRef{{DBName: "db", TableName: "t", ColName: "c"}}}
	env := &fakeEnv{ids: []uint32{testAccountID}, refs: map[uint32]*fakeRefs{testAccountID: refs}}
	s, clk := newSweeper(t, fs, env)

	// Pass 1: orphan, marked.
	deleted, err := s.SweepAccount(ctx, testAccountID)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)

	// Before grace elapses, the blob is referenced again -> pruned from pending.
	clk.advance(1 * time.Hour)
	refs.liveValues = []string{datalinkURL(hash)}
	deleted, err = s.SweepAccount(ctx, testAccountID)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)

	// Pending must no longer track the hash (timer reset).
	s.mu.Lock()
	_, tracked := s.pending[testAccountID][hash]
	s.mu.Unlock()
	require.False(t, tracked)

	// Even well past the original grace, a still-referenced blob survives.
	clk.advance(48 * time.Hour)
	deleted, err = s.SweepAccount(ctx, testAccountID)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)
	ok, err := datalink.CASExists(ctx, fs, testAccountID, hash)
	require.NoError(t, err)
	require.True(t, ok)

	// Now it becomes orphan again: timer restarts from this pass.
	refs.liveValues = nil
	deleted, err = s.SweepAccount(ctx, testAccountID) // re-mark
	require.NoError(t, err)
	require.Equal(t, 0, deleted)

	// Just under grace: still not deleted.
	clk.advance(23 * time.Hour)
	deleted, err = s.SweepAccount(ctx, testAccountID)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)
	ok, err = datalink.CASExists(ctx, fs, testAccountID, hash)
	require.NoError(t, err)
	require.True(t, ok)

	// Cross grace from the restart: deleted.
	clk.advance(2 * time.Hour)
	deleted, err = s.SweepAccount(ctx, testAccountID)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	ok, err = datalink.CASExists(ctx, fs, testAccountID, hash)
	require.NoError(t, err)
	require.False(t, ok)
}

// 5. SweepAll continues past a failing account.
func TestSweepAll_ContinuesPastFailingAccount(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)

	const acctBad = uint32(1)
	const acctGood = uint32(2)

	// acctGood has one orphan blob already past grace once we advance.
	hash, err := datalink.CASPut(ctx, fs, acctGood, []byte("good orphan"))
	require.NoError(t, err)

	goodRefs := &fakeRefs{cols: []columnRef{{DBName: "db", TableName: "t", ColName: "c"}}}
	env := &fakeEnv{
		ids:     []uint32{acctBad, acctGood},
		refs:    map[uint32]*fakeRefs{acctGood: goodRefs},
		refsErr: map[uint32]error{acctBad: fmt.Errorf("boom: refs unavailable")},
	}
	s, clk := newSweeper(t, fs, env)

	// First SweepAll: bad account errors (logged), good account marks its orphan.
	deleted, err := s.SweepAll(ctx)
	require.Error(t, err) // joined error surfaces the bad account
	require.Equal(t, 0, deleted)
	// The good account was still visited.
	require.Contains(t, env.refsCalls, acctGood)

	// Advance past grace; the good account's blob should now be deleted despite
	// the bad account continuing to fail.
	clk.advance(24 * time.Hour)
	deleted, err = s.SweepAll(ctx)
	require.Error(t, err)
	require.Equal(t, 1, deleted)
	ok, err := datalink.CASExists(ctx, fs, acctGood, hash)
	require.NoError(t, err)
	require.False(t, ok)
}
