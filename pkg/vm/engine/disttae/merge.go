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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type cnMergeTask struct {
	host *txnTable
	// txn
	snapshot types.TS // start ts, fixed
	state    *logtailreplay.PartitionState
	proc     *process.Process

	// schema
	version     uint32       // version
	colseqnums  []uint16     // no rowid column
	coltypes    []types.Type // no rowid column
	colattrs    []string     // no rowid column
	sortkeyPos  int          // (composite) primary key, cluster by etc. -1 meas no sort key
	sortkeyIsPK bool

	doTransfer bool

	// targets
	targets []logtailreplay.ObjectInfo

	// commit things
	commitEntry *api.MergeCommitEntry

	// auxiliaries
	fs fileservice.FileService

	blkCnts  []int
	blkIters []*StatsBlkIter

	targetObjSize uint32
}

func newCNMergeTask(
	ctx context.Context,
	tbl *txnTable,
	snapshot types.TS,
	state *logtailreplay.PartitionState,
	sortkeyPos int,
	sortkeyIsPK bool,
	targets []logtailreplay.ObjectInfo,
	targetObjSize uint32,
) (*cnMergeTask, error) {
	proc := tbl.proc.Load()
	attrs := make([]string, 0, len(tbl.seqnums))
	for i := 0; i < len(tbl.tableDef.Cols)-1; i++ {
		attrs = append(attrs, tbl.tableDef.Cols[i].Name)
	}
	fs := proc.FileService

	blkCnts := make([]int, len(targets))
	blkIters := make([]*StatsBlkIter, len(targets))
	for i, objInfo := range targets {
		objInfo := objInfo
		blkCnts[i] = int(objInfo.BlkCnt())

		loc := objInfo.ObjectLocation()
		meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
		if err != nil {
			return nil, err
		}

		blkIters[i] = NewStatsBlkIter(&objInfo.ObjectStats, meta.MustDataMeta())
	}

	return &cnMergeTask{
		host:        tbl,
		snapshot:    snapshot,
		state:       state,
		proc:        proc,
		version:     tbl.version,
		colseqnums:  tbl.seqnums,
		coltypes:    tbl.typs,
		colattrs:    attrs,
		sortkeyPos:  sortkeyPos,
		sortkeyIsPK: sortkeyIsPK,
		targets:     targets,
		fs:          fs,
		blkCnts:     blkCnts,
		blkIters:    blkIters,

		targetObjSize: targetObjSize,
		doTransfer:    !strings.Contains(tbl.comment, catalog.MO_COMMENT_NO_DEL_HINT),
	}, nil
}

func (t *cnMergeTask) DoTransfer() bool {
	return t.doTransfer
}
func (t *cnMergeTask) GetObjectCnt() int {
	return len(t.targets)
}

func (t *cnMergeTask) GetBlkCnts() []int {
	return t.blkCnts
}

func (t *cnMergeTask) GetAccBlkCnts() []int {
	accCnt := make([]int, 0, len(t.targets))
	acc := 0
	for _, objInfo := range t.targets {
		accCnt = append(accCnt, acc)
		acc += int(objInfo.BlkCnt())
	}
	return accCnt
}

func (t *cnMergeTask) GetBlockMaxRows() uint32 {
	return options.DefaultBlockMaxRows
}

func (t *cnMergeTask) GetObjectMaxBlocks() uint16 {
	return options.DefaultBlocksPerObject
}

func (t *cnMergeTask) GetTargetObjSize() uint32 {
	return t.targetObjSize
}

func (t *cnMergeTask) GetSortKeyType() types.Type {
	if t.sortkeyPos >= 0 {
		return t.coltypes[t.sortkeyPos]
	}
	return types.Type{}
}

func (t *cnMergeTask) LoadNextBatch(ctx context.Context, objIdx uint32) (*batch.Batch, *nulls.Nulls, func(), error) {
	iter := t.blkIters[objIdx]
	if iter.Next() {
		blk := iter.Entry()
		// update delta location
		obj := t.targets[objIdx]
		blk.Sorted = obj.Sorted
		blk.EntryState = obj.EntryState
		blk.CommitTs = obj.CommitTS
		if obj.HasDeltaLoc {
			deltaLoc, commitTs, ok := t.state.GetBockDeltaLoc(blk.BlockID)
			if ok {
				blk.DeltaLoc = deltaLoc
				blk.CommitTs = commitTs
			}
		}
		return t.readblock(ctx, &blk)
	}
	return nil, nil, nil, mergesort.ErrNoMoreBlocks
}

func (t *cnMergeTask) GetCommitEntry() *api.MergeCommitEntry {
	if t.commitEntry == nil {
		return t.prepareCommitEntry()
	}
	return t.commitEntry
}

// impl DisposableVecPool
func (t *cnMergeTask) GetVector(typ *types.Type) (*vector.Vector, func()) {
	v := t.proc.GetVector(*typ)
	return v, func() { t.proc.PutVector(v) }
}

func (t *cnMergeTask) GetMPool() *mpool.MPool {
	return t.proc.GetMPool()
}

func (t *cnMergeTask) HostHintName() string { return "CN" }

func (t *cnMergeTask) PrepareData(ctx context.Context) ([]*batch.Batch, []*nulls.Nulls, func(), error) {
	r, release, d, e := t.readAllData(ctx)
	return r, d, release, e
}

func (t *cnMergeTask) GetTotalSize() uint32 {
	totalSize := uint32(0)
	for _, obj := range t.targets {
		totalSize += obj.OriginSize()
	}
	return totalSize
}

func (t *cnMergeTask) GetTotalRowCnt() uint32 {
	totalRowCnt := uint32(0)
	for _, obj := range t.targets {
		totalRowCnt += obj.Rows()
	}
	return totalRowCnt
}

func (t *cnMergeTask) prepareCommitEntry() *api.MergeCommitEntry {
	commitEntry := &api.MergeCommitEntry{}
	commitEntry.DbId = t.host.db.databaseId
	commitEntry.TblId = t.host.tableId
	commitEntry.TableName = t.host.tableName
	commitEntry.StartTs = t.snapshot.ToTimestamp()
	for _, o := range t.targets {
		commitEntry.MergedObjs = append(commitEntry.MergedObjs, o.ObjectStats.Clone().Marshal())
	}
	t.commitEntry = commitEntry
	// leave mapping to ReadMergeAndWrite
	return commitEntry
}

func (t *cnMergeTask) PrepareNewWriter() *blockio.BlockWriter {
	return mergesort.GetNewWriter(t.fs, t.version, t.colseqnums, t.sortkeyPos, t.sortkeyIsPK)
}

func (t *cnMergeTask) readAllData(ctx context.Context) ([]*batch.Batch, func(), []*nulls.Nulls, error) {
	var cnt uint32
	for _, t := range t.targets {
		cnt += t.BlkCnt()
	}
	blkBatches := make([]*batch.Batch, 0, cnt)
	blkDels := make([]*nulls.Nulls, 0, cnt)
	releases := make([]func(), 0, cnt)

	release := func() {
		for _, r := range releases {
			r()
		}
	}

	for _, obj := range t.targets {
		loc := obj.ObjectLocation()
		meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, t.fs)
		if err != nil {
			release()
			return nil, nil, nil, err
		}

		// read all blocks data in an object
		var innerErr error
		readBlock := func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
			// update delta location
			blk.Sorted = obj.Sorted
			blk.EntryState = obj.EntryState
			blk.CommitTs = obj.CommitTS
			if obj.HasDeltaLoc {
				deltaLoc, commitTs, ok := t.state.GetBockDeltaLoc(blk.BlockID)
				if ok {
					blk.DeltaLoc = deltaLoc
					blk.CommitTs = commitTs
				}
			}
			bat, delmask, releasef, err := t.readblock(ctx, &blk)
			if err != nil {
				innerErr = err
				return false
			}

			blkBatches = append(blkBatches, bat)
			releases = append(releases, releasef)
			blkDels = append(blkDels, delmask)
			return true
		}
		ForeachBlkInObjStatsList(false, meta.MustDataMeta(), readBlock, obj.ObjectStats)
		// if err is found, bail out
		if innerErr != nil {
			release()
			return nil, nil, nil, innerErr
		}
	}

	return blkBatches, release, blkDels, nil
}

// readblock reads block data. there is no rowid column, no ablk
func (t *cnMergeTask) readblock(ctx context.Context, info *objectio.BlockInfo) (bat *batch.Batch, dels *nulls.Nulls, release func(), err error) {
	// read data
	bat, release, err = blockio.LoadColumns(ctx, t.colseqnums, t.coltypes, t.fs, info.MetaLocation(), t.proc.GetMPool(), fileservice.Policy(0))
	if err != nil {
		return
	}

	// read tombstone on disk
	if !info.DeltaLocation().IsEmpty() {
		obat, byCN, delRelease, err2 := blockio.ReadBlockDelete(ctx, info.DeltaLocation(), t.fs)
		if err2 != nil {
			err = err2
			return
		}
		defer delRelease()
		if byCN {
			dels = blockio.EvalDeleteRowsByTimestampForDeletesPersistedByCN(obat, t.snapshot, info.CommitTs)
		} else {
			dels = blockio.EvalDeleteRowsByTimestamp(obat, t.snapshot, &info.BlockID)
		}
	}

	if dels == nil {
		dels = nulls.NewWithSize(128)
	}
	deltalocDel := dels.Count()
	// read tombstone in memory
	iter := t.state.NewRowsIter(t.snapshot, &info.BlockID, true)
	for iter.Next() {
		entry := iter.Entry()
		_, offset := entry.RowID.Decode()
		dels.Add(uint64(offset))
	}
	iter.Close()
	if dels.Count() > 0 {
		logutil.Infof("mergeblocks read block %v, %d deleted(%d from disk)", info.BlockID.ShortStringEx(), dels.Count(), deltalocDel)
	}

	bat.SetAttributes(t.colattrs)
	return
}
