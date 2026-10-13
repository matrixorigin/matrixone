// Copyright 2022 Matrix Origin
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
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

var lifecycleTaskID atomic.Uint64

func (tbl *txnTable) getSortKeyPosAndSortKeyIsPK() (int, bool) {
	sortKeyPos := -1
	sortKeyIsPK := false
	if tbl.primaryIdx >= 0 && tbl.tableDef.Cols[tbl.primaryIdx].Name != catalog.FakePrimaryKeyColName {
		if tbl.clusterByIdx < 0 {
			sortKeyPos = tbl.primaryIdx
			sortKeyIsPK = true
		} else {
			panic(fmt.Sprintf("bad schema pk %v, ck %v", tbl.primaryIdx, tbl.clusterByIdx))
		}
	} else if tbl.clusterByIdx >= 0 {
		sortKeyPos = tbl.clusterByIdx
		sortKeyIsPK = false
	}
	return sortKeyPos, sortKeyIsPK
}

// lifecycleRewriteTask adapts one exact Lifecycle source to the shared
// mergesort producer. It is not an ordinary CN merge task or commit path.
type lifecycleRewriteTask struct {
	taskId uint64 // only unique in a process
	host   *txnTable
	// txn
	snapshot types.TS // start ts, fixed
	ds       engine.DataSource
	mp       *mpool.MPool

	// schema
	colattrs    []string // no rowid column
	sortkeyPos  int      // (composite) primary key, cluster by etc. -1 meas no sort key
	sortkeyIsPK bool

	// targets
	targets []objectio.ObjectStats

	// commit things
	commitEntry   *api.MergeCommitEntry
	transferTable *mergesort.TransferTable

	// auxiliaries
	fs fileservice.FileService

	blkCnts  []int
	blkIters []*objectio.StatsBlkIter

	targetObjSize uint32

	segmentID *objectio.Segmentid
	num       uint16

	arena *objectio.WriteArena

	// Lifecycle installs the read budget before scanning so a physical Block is
	// rejected before BlockDataReadNoCopy when its metadata estimate exceeds
	// the release-certified memory envelope.
	lifecycleReadBudget *lifecycleBlockReadBudget
}

type lifecycleBlockReadBudget struct {
	maxBytes uint64
	metas    []objectio.ObjectDataMeta
	next     []uint32
}

func newLifecycleRewriteTask(
	ctx context.Context,
	tbl *txnTable,
	snapshot types.TS,
	sortkeyPos int,
	sortkeyIsPK bool,
	targets []objectio.ObjectStats,
	targetObjSize uint32,
) (*lifecycleRewriteTask, error) {
	if len(targets) != 1 {
		return nil, moerr.NewInvalidInput(ctx, "Lifecycle reader/rewrite requires one source Object")
	}

	part, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}

	relData := readutil.NewBlockListRelationData(1,
		readutil.WithPartitionState(part))

	source, err := tbl.buildLocalDataSource(
		ctx,
		0,
		relData,
		engine.Policy_CheckAll,
		engine.GeneralLocalDataSource)
	if err != nil {
		return nil, err
	}

	attrs := make([]string, 0, len(tbl.seqnums))
	for i := range len(tbl.tableDef.Cols) - 1 {
		attrs = append(attrs, tbl.tableDef.Cols[i].Name)
	}

	proc := tbl.proc.Load()
	fs := proc.Base.FileService

	blkCnts := make([]int, len(targets))
	blkIters := make([]*objectio.StatsBlkIter, len(targets))
	for i, objStats := range targets {
		blkCnts[i] = int(objStats.BlkCnt())

		loc := objStats.ObjectLocation()
		meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
		if err != nil {
			return nil, err
		}

		blkIters[i] = objectio.NewStatsBlkIter(&objStats, meta.MustDataMeta())
	}

	var arena *objectio.WriteArena
	if targetObjSize > 0 {
		arena = objectio.GetArena(objectio.ArenaLarge)
	}

	return &lifecycleRewriteTask{
		taskId:        lifecycleTaskID.Add(1),
		host:          tbl,
		snapshot:      snapshot,
		ds:            source,
		mp:            proc.GetMPool(),
		colattrs:      attrs,
		sortkeyPos:    sortkeyPos,
		sortkeyIsPK:   sortkeyIsPK,
		targets:       targets,
		fs:            fs,
		blkCnts:       blkCnts,
		blkIters:      blkIters,
		targetObjSize: targetObjSize,
		segmentID:     objectio.NewSegmentid(),
		arena:         arena,
	}, nil
}

func (t *lifecycleRewriteTask) HasBigDelEvent() bool {
	return false
}

func (t *lifecycleRewriteTask) TaskSourceNote() string {
	return ""
}

func (t *lifecycleRewriteTask) Name() string {
	return fmt.Sprintf("[Lifecycle-%d]%d-%s", t.taskId, t.host.tableId, t.host.tableName)
}

func (t *lifecycleRewriteTask) DoTransfer() bool {
	return true
}
func (t *lifecycleRewriteTask) GetObjectCnt() int {
	return len(t.targets)
}

func (t *lifecycleRewriteTask) GetBlkCnts() []int {
	return t.blkCnts
}

func (t *lifecycleRewriteTask) GetAccBlkCnts() []int {
	accCnt := make([]int, 0, len(t.targets))
	acc := 0
	for _, objInfo := range t.targets {
		accCnt = append(accCnt, acc)
		acc += int(objInfo.BlkCnt())
	}
	return accCnt
}

func (t *lifecycleRewriteTask) GetBlockMaxRows() uint32 {
	return objectio.BlockMaxRows
}

func (t *lifecycleRewriteTask) GetObjectMaxBlocks() uint16 {
	return options.DefaultBlocksPerObject
}

func (t *lifecycleRewriteTask) GetTargetObjSize() uint32 {
	return t.targetObjSize
}

func (t *lifecycleRewriteTask) GetSortKeyType() types.Type {
	if t.sortkeyPos >= 0 {
		return t.host.typs[t.sortkeyPos]
	}
	return types.Type{}
}

func (t *lifecycleRewriteTask) LoadNextBatch(ctx context.Context, objIdx uint32, _ *batch.Batch) (*batch.Batch, *nulls.Nulls, func(), error) {
	iter := t.blkIters[objIdx]
	if iter.Next() {
		blk := iter.Entry()
		if err := t.admitLifecycleBlockRead(objIdx, &blk); err != nil {
			return nil, nil, nil, err
		}
		// update delta location
		obj := t.targets[objIdx]
		blk.SetFlagByObjStats(&obj)
		return t.readblock(ctx, &blk)
	}
	return nil, nil, nil, mergesort.ErrNoMoreBlocks
}

func (t *lifecycleRewriteTask) configureLifecycleBlockReadBudget(
	ctx context.Context,
	maxBytes uint64,
) error {
	if maxBytes == 0 {
		return moerr.NewInvalidInput(
			ctx,
			"Lifecycle certified Block read limit must be positive",
		)
	}
	metas := make([]objectio.ObjectDataMeta, len(t.targets))
	for index := range t.targets {
		location := t.targets[index].ObjectLocation()
		meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, t.fs)
		if err != nil {
			return err
		}
		dataMeta := meta.MustDataMeta()
		if dataMeta.IsEmpty() {
			return moerr.NewInvalidInput(
				ctx,
				"Lifecycle source Object metadata has no Data blocks",
			)
		}
		metas[index] = dataMeta
	}
	t.lifecycleReadBudget = &lifecycleBlockReadBudget{
		maxBytes: maxBytes,
		metas:    metas,
		next:     make([]uint32, len(t.targets)),
	}
	return nil
}

func (t *lifecycleRewriteTask) admitLifecycleBlockRead(
	objIdx uint32,
	blockInfo *objectio.BlockInfo,
) error {
	budget := t.lifecycleReadBudget
	if budget == nil {
		return nil
	}
	if int(objIdx) >= len(budget.metas) {
		return moerr.NewInvalidInputNoCtx(
			"Lifecycle Block read Object index is out of range",
		)
	}
	blockOrdinal := budget.next[objIdx]
	meta := budget.metas[objIdx]
	if blockOrdinal >= meta.BlockCount() {
		return moerr.NewInvalidInputNoCtx(
			"Lifecycle Block read ordinal is out of range",
		)
	}
	expectedObjectID := t.targets[objIdx].ObjectName().ObjectId()
	if blockInfo == nil ||
		*blockInfo.BlockID.Object() != *expectedObjectID ||
		blockInfo.BlockID.Sequence() != uint16(blockOrdinal) {
		return moerr.NewInvalidInputNoCtx(
			"Lifecycle exact reader Block identity/order changed",
		)
	}
	block := meta.GetBlockMeta(blockOrdinal)
	var sourceLogicalBytes uint64
	for _, seqnum := range t.host.seqnums {
		origin := uint64(block.MustGetColumn(seqnum).Location().OriginSize())
		if sourceLogicalBytes > ^uint64(0)-origin {
			return moerr.NewInvalidInputNoCtx(
				"Lifecycle Block read metadata size overflow",
			)
		}
		sourceLogicalBytes += origin
	}
	if err := validateLifecycleBlockReadPeak(
		sourceLogicalBytes,
		budget.maxBytes,
	); err != nil {
		return err
	}
	budget.next[objIdx]++
	return nil
}

func (t *lifecycleRewriteTask) GetCommitEntry() *api.MergeCommitEntry {
	if t.commitEntry == nil {
		return t.prepareCommitEntry()
	}
	return t.commitEntry
}

func (t *lifecycleRewriteTask) SetTransferTable(tt *mergesort.TransferTable) {
	t.transferTable = tt
}

// impl DisposableVecPool
func (t *lifecycleRewriteTask) GetVector(typ *types.Type) (*vector.Vector, func()) {
	v := vector.NewOffHeapVecWithType(*typ)
	return v, func() { v.Free(t.mp) }
}

func (t *lifecycleRewriteTask) GetMPool() *mpool.MPool {
	return t.mp
}

func (t *lifecycleRewriteTask) Release() {
	if t.arena != nil {
		t.arena.Reset()
		objectio.PutArena(t.arena)
		t.arena = nil
	}
	if t.transferTable != nil {
		t.transferTable.Release()
		t.transferTable = nil
	}
}

func (t *lifecycleRewriteTask) GetTotalSize() uint64 {
	totalSize := uint64(0)
	for _, obj := range t.targets {
		totalSize += uint64(obj.OriginSize())
	}
	return totalSize
}

func (t *lifecycleRewriteTask) GetTotalRowCnt() uint32 {
	totalRowCnt := uint32(0)
	for _, obj := range t.targets {
		totalRowCnt += obj.Rows()
	}
	return totalRowCnt
}

func (t *lifecycleRewriteTask) prepareCommitEntry() *api.MergeCommitEntry {
	commitEntry := &api.MergeCommitEntry{}
	commitEntry.DbId = t.host.db.databaseId
	commitEntry.TblId = t.host.tableId
	commitEntry.TableName = t.host.tableName
	commitEntry.StartTs = t.snapshot.ToTimestamp()
	for _, o := range t.targets {
		commitEntry.MergedObjs = append(commitEntry.MergedObjs, o.Clone().Marshal())
	}
	t.commitEntry = commitEntry
	// leave mapping to ReadMergeAndWrite
	return commitEntry
}

func (t *lifecycleRewriteTask) PrepareNewWriter() *ioutil.BlockWriter {
	if t.arena != nil {
		t.arena.Reset()
	}
	writer := ioutil.ConstructWriterWithSegmentID(
		t.segmentID,
		t.num,
		t.host.version,
		t.host.seqnums,
		t.sortkeyPos,
		t.sortkeyIsPK,
		false,
		t.fs,
		t.arena,
	)
	t.num++
	return writer // TODO obj.isTombstone
}

// readblock reads block data. there is no rowid column, no ablk
func (t *lifecycleRewriteTask) readblock(ctx context.Context, info *objectio.BlockInfo) (bat *batch.Batch, dels *nulls.Nulls, release func(), err error) {
	// read data
	bat, dels, release, err = blockio.BlockDataReadNoCopy(
		ctx, info, t.ds, t.host.seqnums, t.host.typs,
		t.snapshot, fileservice.SkipAllCache, t.mp, t.fs)
	if err != nil {
		logutil.Infof("read block data failed: %v", err.Error())
		return
	}
	bat.SetAttributes(t.colattrs)
	return
}
