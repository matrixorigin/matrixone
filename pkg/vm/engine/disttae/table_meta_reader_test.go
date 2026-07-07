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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestTableMetaReaderRecordsCloneObjectOwnership(t *testing.T) {
	tbl := newTxnTableForTest()
	txn := tbl.getTxn()
	txn.BindTxnOp(tbl.db.op)
	txn.engine.cloneTxnCache = newCloneTxnCache()
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 2)
	reader := &TableMetaReader{table: tbl}
	reader.addCloneSharedFile(&stats[0], cloneObjectFromCommittedState)
	reader.addCloneSharedFile(&stats[1], cloneObjectFromTxnWorkspace)

	txnID := txn.op.Txn().ID
	require.True(t, txn.engine.cloneTxnCache.IsSharedFile(txnID, stats[0].ObjectName().String()))
	require.False(t, txn.engine.cloneTxnCache.IsSharedFile(txnID, stats[1].ObjectName().String()))
	require.True(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txnID, stats[1].ObjectName().String()))
}

func TestCloneTxnIntermediateGCKeepsTxnLocalSharedObjects(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	txn := &Transaction{
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op: txnOp,
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 2)
	committedSourceName := stats[0].ObjectName().String()
	txnLocalName := stats[1].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, committedSourceName))
	require.NoError(t, writeObjectToFS(ctx, fs, txnLocalName))

	txn.engine.cloneTxnCache.AddSharedFile(txn.op.Txn().ID, committedSourceName)
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, txnLocalName)
	mp := mpool.MustNewZero()
	bat := cloneObjectStatsBatchForTest(t, mp, stats...)
	defer func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	txn.writes = append(txn.writes, Entry{
		typ:      INSERT,
		fileName: "clone-meta",
		bat:      bat,
	})

	require.NoError(t, txn.GCObjsByIdxRange(0, 0))
	require.True(t, objectExistsInFS(ctx, fs, committedSourceName))
	require.True(t, objectExistsInFS(ctx, fs, txnLocalName))
}

func TestCloneTxnRollbackGCDeletesTxnLocalSharedObjects(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	txn := &Transaction{
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op: txnOp,
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 2)
	committedSourceName := stats[0].ObjectName().String()
	txnLocalName := stats[1].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, committedSourceName))
	require.NoError(t, writeObjectToFS(ctx, fs, txnLocalName))

	txn.engine.cloneTxnCache.AddSharedFile(txn.op.Txn().ID, committedSourceName)
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, txnLocalName)
	mp := mpool.MustNewZero()
	bat := cloneObjectStatsBatchForTest(t, mp, stats...)
	defer func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	txn.writes = append(txn.writes, Entry{
		typ:      INSERT,
		fileName: "clone-meta",
		bat:      bat,
	})

	require.NoError(t, txn.gcObjsByIdxRange(0, 0, cloneGCTxnRollback))
	require.Eventually(t, func() bool {
		return objectExistsInFS(ctx, fs, committedSourceName) &&
			!objectExistsInFS(ctx, fs, txnLocalName)
	}, time.Second, 10*time.Millisecond)
}

func TestCloneTxnCompactionGCDeletesUnreferencedTxnLocalSharedObject(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	txn := &Transaction{
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op: txnOp,
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, name)

	mp := mpool.MustNewZero()
	bat := cloneObjectStatsBatchForTest(t, mp, stats...)
	txn.writes = append(txn.writes, Entry{
		typ:      INSERT,
		fileName: name,
		bat:      bat,
	})
	txn.writes[0].bat.Clean(mp)
	txn.writes[0].bat = nil
	require.Zero(t, mp.CurrNB())

	txn.unprotectUnreferencedTxnLocalSharedFilesLocked(stats, nil)
	require.False(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, name))
	require.NoError(t, txn.GCObjsByStats(stats...))
	require.Eventually(t, func() bool {
		return !objectExistsInFS(ctx, fs, name)
	}, time.Second, 10*time.Millisecond)
}

func TestCloneTxnCompactionGCKeepsLiveTxnLocalSharedObject(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	txn := &Transaction{
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op: txnOp,
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, name)

	mp := mpool.MustNewZero()
	liveBat := cloneObjectStatsBatchForTest(t, mp, stats...)
	defer func() {
		liveBat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	txn.writes = append(txn.writes, Entry{
		typ:      INSERT,
		fileName: name,
		bat:      liveBat,
	})

	txn.unprotectUnreferencedTxnLocalSharedFilesLocked(stats, nil)
	require.True(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, name))
	require.NoError(t, txn.GCObjsByStats(stats...))
	require.True(t, objectExistsInFS(ctx, fs, name))
}

func TestCloneTxnTablesInVainGCDeletesUnreferencedTxnLocalSharedObject(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc: proc,
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op:              txnOp,
		tablesInVain:    map[uint64]int{42: 1},
		deletedBlocks:   &deletedBlocks{offsets: map[types.Blockid][]int64{}},
		batchSelectList: make(map[*batch.Batch][]int64),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, name)

	txn.writes = append(txn.writes, Entry{
		typ:      INSERT,
		tableId:  42,
		fileName: name,
		bat:      cloneObjectStatsBatchForTest(t, proc.Mp(), stats...),
	})

	require.NoError(t, txn.mergeTxnWorkspaceLocked(ctx))
	require.Nil(t, txn.writes[0].bat)
	require.False(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, name))
	require.Eventually(t, func() bool {
		return !objectExistsInFS(ctx, fs, name)
	}, time.Second, 10*time.Millisecond)
}

func TestCloneTxnTablesInVainGCKeepsLiveTxnLocalSharedObject(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc: proc,
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op:              txnOp,
		tablesInVain:    map[uint64]int{42: 1},
		deletedBlocks:   &deletedBlocks{offsets: map[types.Blockid][]int64{}},
		batchSelectList: make(map[*batch.Batch][]int64),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, name)

	liveBat := cloneObjectStatsBatchForTest(t, proc.Mp(), stats...)
	txn.writes = append(txn.writes,
		Entry{
			typ:      INSERT,
			tableId:  42,
			fileName: name,
			bat:      cloneObjectStatsBatchForTest(t, proc.Mp(), stats...),
		},
		Entry{
			typ:      INSERT,
			tableId:  43,
			fileName: name,
			bat:      liveBat,
		},
	)

	require.NoError(t, txn.mergeTxnWorkspaceLocked(ctx))
	require.Nil(t, txn.writes[0].bat)
	require.NotNil(t, txn.writes[1].bat)
	require.True(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, name))
	require.True(t, objectExistsInFS(ctx, fs, name))

	liveBat.Clean(proc.Mp())
}

func TestCloneTxnUnknownCommitPreservesRollbackCleanup(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()

	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc: proc,
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op:              txnOp,
		tablesInVain:    make(map[uint64]int),
		deletedBlocks:   &deletedBlocks{offsets: map[types.Blockid][]int64{}},
		batchSelectList: make(map[*batch.Batch][]int64),
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, name)
	txn.writes = append(txn.writes, Entry{
		typ:      INSERT,
		tableId:  42,
		fileName: name,
		bat:      cloneObjectStatsBatchForTest(t, proc.Mp(), stats...),
	})

	txn.FinalizeCommitWithUnknownResult(ctx)
	require.NotNil(t, txn.writes[0].bat)
	require.True(t, txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, name))
	require.NoError(t, txn.gcObjsByIdxRange(0, 0, cloneGCTxnRollback))
	require.Eventually(t, func() bool {
		return !objectExistsInFS(ctx, fs, name)
	}, time.Second, 10*time.Millisecond)
	txn.writes[0].bat.Clean(proc.Mp())
	txn.writes[0].bat = nil
}

func cloneObjectStatsBatchForTest(
	t *testing.T,
	mp *mpool.MPool,
	statsList ...objectio.ObjectStats,
) *batch.Batch {
	bat := batch.New([]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})
	bat.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
	bat.SetVector(1, vector.NewVec(types.T_varchar.ToType()))
	for i := range statsList {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("block-info"), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], statsList[i].Marshal(), false, mp))
	}
	bat.SetRowCount(len(statsList))
	return bat
}
