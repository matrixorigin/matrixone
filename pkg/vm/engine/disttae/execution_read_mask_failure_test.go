// Copyright 2026 MatrixOne
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"sync"
)

// The pre-PR review P2 allocation-failure members for the flushed collector:
// a rowid append that fails and a PK append that fail (vecappend protocol
// errors) must surface through the collector's shared failure contract and
// STOP the drain immediately — never loop through every tombstone row after
// the mask can no longer be complete, and never leave a truncated,
// length-divergent delete set that a caller could mistake for the full set
// (the exact shape whose downstream guards reject an incomplete mask).
func TestCollectFlushedMaskIntentsAllocationFailuresPropagate(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := proc.Ctx
	mp := proc.GetMPool()

	// Real flushed tombstone rows through the production writer, exactly
	// like the readback member above.
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 100; i++ {
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], types.RandomRowid(), false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
	}
	bat.SetRowCount(100)
	defer bat.CleanOnlyData()

	fs, err := fileservice.Get[fileservice.FileService](
		proc.Base.FileService, defines.SharedFileServiceName)
	require.NoError(t, err)
	writer := colexec.NewCNS3TombstoneWriterForService(
		"execread-mask-failure", mp, fs, types.T_int32.ToType(), -1)
	require.NoError(t, writer.Write(ctx, bat))
	stats, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, stats)
	defer writer.Close()

	op, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	op.AddWorkspace(&Transaction{engine: &Engine{fs: fs}})
	_, tbl := newExecutionReadTestTable(t, op)

	// The shared production vecappend protocol (computeExecutionReadTombstoneMask's
	// appendRaw closure): rowid first, then the PK union, one shared error slot.
	for _, tc := range []struct {
		name  string
		rowid uint64
		pk    uint64
	}{
		{name: "rowid append failure", rowid: 32, pk: 1 << 20},
		{name: "pk append failure", rowid: 1 << 20, pk: 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rowidRegistry, err := mpool.NewAllocationAccountRegistry(1, 8)
			require.NoError(t, err)
			rowidAccount, err := rowidRegistry.Open(tc.rowid)
			require.NoError(t, err)
			rowidSelection, err := vector.NewAllocationAccountSelection(
				rowidAccount, mpool.AllocationOwnerDependent, 1, 2, 3, 4)
			require.NoError(t, err)

			pkRegistry, err := mpool.NewAllocationAccountRegistry(1, 8)
			require.NoError(t, err)
			pkAccount, err := pkRegistry.Open(tc.pk)
			require.NoError(t, err)
			pkSelection, err := vector.NewAllocationAccountSelection(
				pkAccount, mpool.AllocationOwnerDependent, 1, 2, 3, 4)
			require.NoError(t, err)

			rawRowids := vector.NewOffHeapVecWithType(types.T_Rowid.ToType())
			require.NoError(t, rawRowids.SetAllocationAccount(rowidSelection))
			rawPKs := vector.NewOffHeapVecWithType(types.T_int32.ToType())
			require.NoError(t, rawPKs.SetAllocationAccount(pkSelection))

			var collectErr error
			appendCalls := 0
			appendRaw := func(rowid types.Rowid, pkColumn *vector.Vector, j int64) {
				if collectErr != nil {
					return
				}
				appendCalls++
				if collectErr = vector.AppendFixed[types.Rowid](rawRowids, rowid, false, mp); collectErr != nil {
					return
				}
				collectErr = rawPKs.UnionOne(pkColumn, j, mp)
			}

			err = collectFlushedMaskIntents(ctx, tbl, stats, types.T_int32.ToType(),
				appendRaw, func() bool { return collectErr != nil })
			require.NoError(t, err,
				"the collector reports the shared callback failure through the contract; the caller propagates it")
			require.Error(t, collectErr,
				"the failing vecappend stage surfaced a real error in the shared slot")
			require.Less(t, appendCalls, 100,
				"the collector stops draining once the mask can no longer be complete (the writer persisted 100 rows)")

			if tc.pk == 32 {
				// The pk member: the last attempted row appended its rowid but
				// the pk union failed — the lengths diverge by exactly that one
				// half-written pair, the truncated/inconsistent set the
				// caller's new guards must reject instead of mapping a prefix;
				// every earlier completed pair stays paired.
				require.Equal(t, rawPKs.Length()+1, rawRowids.Length(),
					"a failed pk union leaves the delete set length-divergent")
			} else {
				// The rowid member: the failing rowid append is atomic — the
				// half pair never enters the set, so every collected row
				// stays a complete (rowid, pk) pair.
				require.Equal(t, rawPKs.Length(), rawRowids.Length(),
					"a failed rowid append adds nothing to the delete set")
			}

			// Release the vectors BEFORE the account seals: the free path is
			// what returns every charge (the finalized-vector protocol).
			rawRowids.Free(mp)
			rawPKs.Free(mp)
			snapshot := rowidAccount.Seal()
			require.Zero(t, snapshot.Used)
			snapshot = pkAccount.Seal()
			require.Zero(t, snapshot.Used)
			_, err = rowidRegistry.Finalize(rowidAccount)
			require.NoError(t, err)
			_, err = pkRegistry.Finalize(pkAccount)
			require.NoError(t, err)
		})
	}
}

// The collector's hard-error channel on the production reader: a tombstone
// object whose location does not resolve FAILS the reader loudly (FastLoadObjectMeta),
// never silently producing an incomplete readback — the same-error contract
// the in-memory arm already had.
func TestCollectFlushedMaskIntentsReadErrorPropagates(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := proc.Ctx

	fs, err := fileservice.Get[fileservice.FileService](
		proc.Base.FileService, defines.SharedFileServiceName)
	require.NoError(t, err)
	op, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	op.AddWorkspace(&Transaction{engine: &Engine{fs: fs}})
	_, tbl := newExecutionReadTestTable(t, op)

	// A fabricated stats entry whose ObjectLocation cannot resolve to any
	// object in the shared store.
	id := types.NewObjectid()
	bogus := objectio.NewObjectStatsWithObjectID(&id, false, true, true)

	appendCalls := 0
	err = collectFlushedMaskIntents(ctx, tbl, []objectio.ObjectStats{*bogus}, types.T_int32.ToType(),
		func(rowid types.Rowid, pkColumn *vector.Vector, j int64) {
			appendCalls++
		},
		func() bool { return false })
	require.Error(t, err,
		"an unreadable flushed tombstone location fails the collection loudly")
	require.Zero(t, appendCalls,
		"no row of an unreadable tombstone may enter the delete set")
}

// The BINDING-level failure contract (pre-PR review round 3, P1): a mask
// computation failure reaching BindExecutionReadOp propagates UNCHANGED — an
// unreadable flushed tombstone is a real I/O failure, not an excluded
// relation kind, so the binding must not downgrade it to
// ExecutionReadUnsupportedError (the shape whose caller silently degraded
// the statement to snapshot semantics). The compile binding caller treats
// any non-unsupported error as a terminal statement failure (G4).
func TestBindExecutionReadOpMaskComputationFailurePropagatesUnchanged(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := proc.Ctx

	fs, err := fileservice.Get[fileservice.FileService](
		proc.Base.FileService, defines.SharedFileServiceName)
	require.NoError(t, err)

	// A fabricated stats entry whose ObjectLocation cannot resolve to any
	// object in the shared store — the hard reader error channel.
	id := types.NewObjectid()
	bogus := objectio.NewObjectStatsWithObjectID(&id, false, true, true)
	list := new(sync.Map)
	list.Store(*bogus, nil)

	op, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	op.TxnRef().SnapshotTS = timestamp.Timestamp{PhysicalTime: 1000, LogicalTime: 1}
	// A FLUSHED delete entry whose marshalled ObjectStats cannot resolve: the
	// mask enumerates flushed tombstones from the owning DELETE entry, and
	// the bogus location fails the production reader loudly inside the BIND.
	statsVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(statsVec, bogus[:], false, proc.Mp()))
	statsBat := batch.NewWithSize(1)
	statsBat.Vecs[0] = statsVec
	statsBat.SetRowCount(1)
	op.AddWorkspace(&Transaction{
		engine: &Engine{fs: fs},
		cn_flushed_s3_tombstone_object_stats_list: list,
		proc:   proc,
		writes: []Entry{{typ: DELETE, tableId: 3, fileName: "bogus", bat: statsBat}},
	})
	_, origin := newExecutionReadTestTable(t, op)
	// The mask's nonsmoke gates: a real (snapshot, frontier) window and a
	// locked-row primary key, so the flushed-tombstone arm actually runs.
	origin.primaryIdx = 0
	origin.tableDef.Cols = []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}}, {Name: "v", Typ: plan.Type{Id: int32(types.T_int32)}}}
	origin.proc.Store(proc)
	handle := &txnTableDelegate{origin: origin, reusableHandle: true}

	carrier := client.ApplyExecutionReadTS(op, timestamp.Timestamp{PhysicalTime: 2000, LogicalTime: 9})
	bindErr := handle.BindExecutionReadOp(carrier)
	require.Error(t, bindErr,
		"an unreadable tombstone in the mask computation fails the binding")
	require.False(t, engine.IsExecutionReadUnsupported(bindErr),
		"a mask computation failure is a REAL failure: it must never masquerade as the soft unsupported class")
	require.Same(t, origin, handle.origin,
		"the refused handle keeps its original binding")

	// The soft class stays reserved for excluded relation kinds: a fresh
	// handle on a CLEAN transaction (no flushed entry) binds normally.
	op2, closeFn2 := client.NewTestTxnOperator(ctx)
	defer closeFn2()
	op2.TxnRef().SnapshotTS = timestamp.Timestamp{PhysicalTime: 1000, LogicalTime: 1}
	op2.AddWorkspace(&Transaction{
		engine: &Engine{fs: fs},
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
		proc: proc,
	})
	_, origin2 := newExecutionReadTestTable(t, op2)
	origin2.primaryIdx = 0
	origin2.tableDef.Cols = []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}}, {Name: "v", Typ: plan.Type{Id: int32(types.T_int32)}}}
	origin2.proc.Store(proc)
	handle2 := &txnTableDelegate{origin: origin2, reusableHandle: true}
	require.NoError(t, handle2.BindExecutionReadOp(
		client.ApplyExecutionReadTS(op2, timestamp.Timestamp{PhysicalTime: 2000, LogicalTime: 9})))
}
