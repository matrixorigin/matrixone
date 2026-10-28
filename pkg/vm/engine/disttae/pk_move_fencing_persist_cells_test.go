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

package disttae

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

const pkMoveFencingCellTimeout = 30 * time.Second

// The D185(B.2)/(H.10)(j)/(k) persistence cells and the review-P3(c)
// staging-spill schedule share one fabric: the file drives the REAL
// dumpBatchLocked path to mint REAL durable object names into the shared
// fs (witness-PRECONDITION: a run failing to mint a name FAILS as vacuous),
// then runs the REAL Transaction.Rollback GC against either an open or a
// closed engine. The certification's load-bearing claim is exactly the
// partition these subtests assert:
//
//	LEG 2: real names + rollback with the engine OPEN   -> gcFiles consumes
//	       the names successfully, panic ABSENT.
//	LEG 3: real names + rollback with the engine CLOSED -> the closed-engine
//	       rejection, the Rollback PANIC lands (fail-loud discriminator),
//	       and the durable object SURVIVES (the residue the certificate
//	       exists to prevent).
//	(k):   ZERO names + rollback with the engine CLOSED -> the zero-name
//	       fast path precedes the closed-engine checks, panic ABSENT.
//
// The production ordering that places a persisted lane's rollback behind
// teardown (the sync unlock blocked past closeStoreEngine, released by
// lockService.Close) is realized here directly by ordering the rollback
// after dynamicCtx.closed; the unlock-chain trace remains the live (h.2)
// cell. The deterministic KNOWN post-preparation rejection of the legs is
// realized by minting through the commit-time dump and then ROLLING BACK
// instead of committing — the GC outcome the legs discriminate does not
// depend on the rejection's classification.

// mintedWorkspace holds a fabric transaction whose commit-time dump has
// written real durable objects.
type mintedWorkspace struct {
	t     *testing.T
	txn   *Transaction
	names []string
	pool  *ants.Pool
}

// mintWorkspaceNames drives dumpBatchLocked(ctx, -1, -1) — the commit-time
// branch, dumpAll=true — over one workspace entry of the given type and
// returns the REAL object names carried by the resulting writeFile entries.
// With insertEntryMaxCount=1 one staged row satisfies the :1394 delete gate
// (ARM B) / makes the insert gate admit (ARM A); the zero
// commitWorkspaceThreshold keeps the insert size check open.
func mintWorkspaceNames(t *testing.T, typ int) *mintedWorkspace {
	t.Helper()
	ctx := context.Background()
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	require.NotNil(t, tbl)
	tbl.tableDef.Cols[0].Typ = pbplan.Type{Id: int32(types.T_int64)}
	txn.tnStores = []DNStore{{}}
	txn.batchSelectList = make(map[*batch.Batch][]int64)
	txn.tablesInVain = make(map[uint64]int)
	txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	txn.engine.config.insertEntryMaxCount = 1
	txn.engine.config.commitWorkspaceThreshold = 0
	txn.commitWorkspaceThreshold = 0

	switch typ {
	case INSERT:
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   7,
			tableId:      42,
			databaseName: "db",
			tableName:    "tbl",
			bat:          newInsertBatchWithRowIDForTest(t, txn.proc, []int64{7}),
		})
	case DELETE:
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          DELETE,
			accountId:    1,
			databaseId:   7,
			tableId:      42,
			databaseName: "db",
			tableName:    "tbl",
			bat:          newDeleteBatchForTest(t, txn.proc, []int64{7}),
		})
	default:
		t.Fatalf("unsupported workspace type %d", typ)
	}

	// Engine-open GC intake: shared fs (the storage the dump writes) and a
	// live GC pool.
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	txn.engine.fs = fs
	txn.engine.gcPool = pool
	t.Cleanup(pool.Release)

	txn.Lock()
	err = txn.dumpBatchLocked(ctx, -1, -1)
	txn.Unlock()
	require.NoError(t, err, "the commit-time dump must succeed (preparation)")

	// WITNESS FIRST: collect the names the dump minted and assert every one
	// is durable/readable BEFORE any rollback or oracle runs.
	var names []string
	witnessed := false
	for i := range txn.writes {
		e := &txn.writes[i]
		if e.bat == nil || e.fileName == "" || e.typ == SOFT_DELETE_OBJECT {
			continue
		}
		witnessed = true
		var vecIdx int
		if e.typ == DELETE {
			vecIdx = 0 // [object_stats, pk]
		} else {
			vecIdx = 1 // [%!%mo__meta_loc, object_stats]
		}
		require.Greater(t, len(e.bat.Vecs), vecIdx)
		vec := e.bat.Vecs[vecIdx]
		for j := 0; j < vec.Length(); j++ {
			ss := objectio.ObjectStats(vec.GetBytesAt(j))
			names = append(names, ss.ObjectName().String())
		}
	}
	require.True(t, witnessed,
		"VACUOUS: the commit-time dump must leave a writeFile entry with a non-empty fileName")
	require.NotEmpty(t, names,
		"VACUOUS: the writeFile entry must carry object stats")
	for _, name := range names {
		_, err := fs.StatFile(ctx, name)
		require.NoError(t, err,
			"the minted object must be durable and readable before the cell proceeds: %s", name)
	}
	return &mintedWorkspace{t: t, txn: txn, names: names, pool: pool}
}

// barrierGC waits for every GC submission queued so far to finish.
func (m *mintedWorkspace) barrierGC() {
	m.t.Helper()
	barrier := make(chan struct{})
	require.NoError(m.t, m.pool.Submit(func() { close(barrier) }))
	select {
	case <-barrier:
	case <-time.After(pkMoveFencingCellTimeout):
		m.t.Fatal("GC pool barrier timed out")
	}
}

func (m *mintedWorkspace) statAllPresent() {
	m.t.Helper()
	fs := m.txn.engine.fs
	for _, name := range m.names {
		_, err := fs.StatFile(context.Background(), name)
		require.NoError(m.t, err, "object %s must still exist", name)
	}
}

func (m *mintedWorkspace) statAllAbsent() {
	m.t.Helper()
	fs := m.txn.engine.fs
	for _, name := range m.names {
		_, err := fs.StatFile(context.Background(), name)
		require.Error(m.t, err, "engine-open cleanup must delete %s", name)
	}
}

// rollbackPanic sandbox-runs Transaction.Rollback and returns the recovered
// panic value (nil when no panic landed).
func rollbackPanic(txn *Transaction) (panicked any) {
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		_ = txn.Rollback(context.Background())
	}()
	select {
	case panicked = <-done:
	case <-time.After(pkMoveFencingCellTimeout):
		panicked = "ROLLBACK-HUNG"
	}
	return panicked
}

// TestDataBranchFencingPersistLeg2EngineOpenGC is (H.10)(j) LEG 2: real
// durable names minted by the commit-time dump are consumed by the REAL
// rollback GC with the engine OPEN — the premise the C-KR residue argument
// needs — with the :4068 panic oracle ABSENT. ARM A (one-row insert) and
// ARM B (one-tombstone, insertEntryMaxCount=1) twins.
func TestDataBranchFencingPersistLeg2EngineOpenGC(t *testing.T) {
	for _, arm := range []struct {
		name string
		typ  int
	}{
		{"ARM_A_insert", INSERT},
		{"ARM_B_tombstone", DELETE},
	} {
		t.Run(arm.name, func(t *testing.T) {
			m := mintWorkspaceNames(t, arm.typ)
			require.False(t, m.txn.engine.dynamicCtx.closed.Load(),
				"LEG 2 runs with the engine open")
			require.Nil(t, rollbackPanic(m.txn),
				"engine-open real-name rollback must not panic")
			m.barrierGC()
			m.statAllAbsent()
		})
	}
}

// TestDataBranchFencingPersistLeg3PostTeardownPanic is (H.10)(j) LEG 3: the
// same witness-first names, but the rollback runs with the engine CLOSED —
// the closed-engine rejection fires inside gcFiles and the Rollback panic
// lands (fail-loud), with the durable objects SURVIVING as the residue the
// certification denies by construction on every certified lane.
func TestDataBranchFencingPersistLeg3PostTeardownPanic(t *testing.T) {
	for _, arm := range []struct {
		name string
		typ  int
	}{
		{"ARM_A_insert", INSERT},
		{"ARM_B_tombstone", DELETE},
	} {
		t.Run(arm.name, func(t *testing.T) {
			m := mintWorkspaceNames(t, arm.typ)
			m.statAllPresent() // residue baseline: real content exists
			m.txn.engine.dynamicCtx.closed.Store(true)
			p := rollbackPanic(m.txn)
			require.NotNil(t, p, "post-teardown real-name rollback must panic (fail-loud)")
			require.Contains(t, p.(string), "gc objects generated by CN failed",
				"the :4068 oracle is the rollback GC failure, value %v", p)

			// The residue survives the refused cleanup; reclaim it with the
			// engine re-opened so the fabric leaves nothing behind.
			m.statAllPresent()
			m.txn.engine.dynamicCtx.closed.Store(false)
			require.Nil(t, rollbackPanic(m.txn))
			m.barrierGC()
			m.statAllAbsent()
		})
	}
}

// TestDataBranchFencingPersistZeroNamePostTeardown is (H.10)(k)'s zero-name
// arm: a certificate-clean workspace (in-memory only, no minted names) rolls
// back with the engine CLOSED and takes the zero-name fast path BEFORE the
// closed-engine checks — panic ABSENT, the discriminating oracle against
// LEG 3's real-name panic.
func TestDataBranchFencingPersistZeroNamePostTeardown(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	txn.tnStores = []DNStore{{}}
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer pool.Release()
	txn.engine.fs = fs
	txn.engine.gcPool = pool

	// Unflushed in-memory workspace: no dump runs, so no name can exist.
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   7,
		tableId:      42,
		databaseName: "db",
		tableName:    "tbl",
		bat:          newInsertBatchWithRowIDForTest(t, txn.proc, []int64{7}),
	})
	txn.engine.dynamicCtx.closed.Store(true)
	require.Nil(t, rollbackPanic(txn),
		"the zero-name fast path must precede the closed-engine checks")
}

// TestPkMoveStagingInsertSpillRealNames is the review-P3(c) staging-time
// INSERT spill schedule, pinning the exception the fencing certificate does
// NOT rely on (review P3(a)): the design's "names mint only at commit" is
// restricted to TOMBSTONES — the delete gate fires only on the offset<0
// (commit) branch — while the statement-boundary INSERT path (dumpAll=false)
// spills live INSERT batches to durable objects PRE-commit. The certificate's
// floors argument (driver-exact tiny staged shapes far under every threshold)
// remains what keeps persistence closed on the fencing lanes. This cell
// witnesses that a staging-time INSERT spill mints REAL names and that the
// engine-open GC consumes exactly those names.
//
// Witness-PRECONDITION discipline: a run failing to mint a real name FAILS
// the cell as vacuous rather than passing silently.
func TestPkMoveStagingInsertSpillRealNames(t *testing.T) {
	ctx := context.Background()
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	require.NotNil(t, tbl)
	tbl.tableDef.Cols[0].Typ = pbplan.Type{Id: int32(types.T_int64)}
	txn.tnStores = []DNStore{{}}
	txn.batchSelectList = make(map[*batch.Batch][]int64)
	txn.tablesInVain = make(map[uint64]int)
	txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

	// Engine-open GC intake: shared fs (same storage the dump writes) + pool.
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer pool.Release()
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)
	txn.engine.fs = fs
	txn.engine.gcPool = pool

	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   7,
		tableId:      42,
		databaseName: "db",
		tableName:    "tbl",
		bat:          newInsertBatchWithRowIDForTest(t, txn.proc, []int64{1, 2, 3}),
	})

	// Statement boundary (offset >= 0 => dumpAll=false, the STAGING path):
	// zero writeWorkspaceThreshold makes any staged content exceed the size
	// gate; the zero extraWorkspaceThreshold keeps the quota path silent.
	txn.writeWorkspaceThreshold = 0
	txn.Lock()
	err = txn.dumpBatchLocked(ctx, 0, -1)
	txn.Unlock()
	require.NoError(t, err)

	// WITNESS FIRST: the spill minted a real durable object name, carried by
	// a writeFile INSERT entry; the object must be readable from fs.
	var spilledNames []string
	witness := false
	for i := range txn.writes {
		if txn.writes[i].typ != INSERT || txn.writes[i].fileName == "" {
			continue
		}
		witness = true
		require.GreaterOrEqual(t, len(txn.writes[i].bat.Vecs), 2)
		vec := txn.writes[i].bat.Vecs[1] // [%!%mo__meta_loc, object_stats]
		for j := 0; j < vec.Length(); j++ {
			ss := objectio.ObjectStats(vec.GetBytesAt(j))
			spilledNames = append(spilledNames, ss.ObjectName().String())
		}
	}
	require.True(t, witness,
		"VACUOUS: staging spill must mint a writeFile INSERT entry with non-empty fileName")
	require.NotEmpty(t, spilledNames,
		"VACUOUS: witness found but no object stats carried")
	for _, name := range spilledNames {
		_, err := fs.StatFile(ctx, name)
		require.NoError(t, err,
			"spilled object must be durable/readable before cleanup: %s", name)
	}

	// ENGINE-OPEN CLEANUP: the GC path consumes exactly the witnessed names
	// while the engine is open (dynamicCtx.closed unset, gcPool live).
	require.False(t, txn.engine.dynamicCtx.closed.Load())
	require.NoError(t, txn.gcObjsByIdxRange(0, len(txn.writes)-1, cloneGCTxnRollback))

	// Deletion is async on the pool: barrier behind the GC submissions.
	barrier := make(chan struct{})
	require.NoError(t, pool.Submit(func() { close(barrier) }))
	select {
	case <-barrier:
	case <-time.After(pkMoveFencingCellTimeout):
		t.Fatal("GC pool barrier timed out")
	}
	for _, name := range spilledNames {
		_, err := fs.StatFile(ctx, name)
		require.Error(t, err,
			"engine-open cleanup must delete the witnessed object: %s", name)
	}

	for i := range txn.writes {
		if txn.writes[i].bat != nil {
			txn.writes[i].bat.Clean(txn.proc.Mp())
		}
	}
}
