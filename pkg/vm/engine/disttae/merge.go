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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type CNMergeTask struct {
	ctx  context.Context
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

	// targets
	targets []logtailreplay.ObjectInfo

	// commit things
	commitEntry *api.MergeCommitEntry

	// auxiliaries
	fs fileservice.FileService
}

func NewCNMergeTask(
	ctx context.Context,
	tbl *txnTable,
	snapshot types.TS,
	state *logtailreplay.PartitionState,
	sortkeyPos int,
	sortkeyIsPK bool,
	targets []logtailreplay.ObjectInfo,
) *CNMergeTask {
	proc := tbl.proc.Load()
	attrs := make([]string, 0, len(tbl.seqnums))
	for i := 0; i < len(tbl.tableDef.Cols)-1; i++ {
		attrs = append(attrs, tbl.tableDef.Cols[i].Name)
	}
	fs := proc.FileService
	return &CNMergeTask{
		ctx:         ctx,
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
	}
}

// impl DisposableVecPool
func (t *CNMergeTask) GetVector(typ *types.Type) (*vector.Vector, func()) {
	v := t.proc.GetVector(*typ)
	return v, func() { t.proc.PutVector(v) }
}

func (t *CNMergeTask) GetMPool() *mpool.MPool {
	return t.proc.GetMPool()
}

func (t *CNMergeTask) HostHintName() string { return "CN" }

func (t *CNMergeTask) PrepareData() ([]*batch.Batch, []*nulls.Nulls, func(), error) {
	r, d, e := t.readAllData()
	return r, d, func() {}, e
}

func (t *CNMergeTask) PrepareCommitEntry() *api.MergeCommitEntry {
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

func (t *CNMergeTask) PrepareNewWriterFunc() func() *blockio.BlockWriter {
	return mergesort.GetMustNewWriter(t.fs, t.version, t.colseqnums, t.sortkeyPos, t.sortkeyIsPK)
}

func (t *CNMergeTask) readAllData() ([]*batch.Batch, []*nulls.Nulls, error) {
	var cnt uint32
	for _, t := range t.targets {
		cnt += t.BlkCnt()
	}
	blkBatches := make([]*batch.Batch, 0, cnt)
	blkDels := make([]*nulls.Nulls, 0, cnt)

	for _, obj := range t.targets {
		loc := obj.ObjectLocation()
		meta, err := objectio.FastLoadObjectMeta(t.ctx, &loc, false, t.fs)
		if err != nil {
			return nil, nil, err
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
			bat, delmask, err := t.readblock(&blk)
			if err != nil {
				innerErr = err
				return false
			}

			blkBatches = append(blkBatches, bat)
			blkDels = append(blkDels, delmask)
			return true
		}
		ForeachBlkInObjStatsList(false, meta.MustDataMeta(), readBlock, obj.ObjectStats)
		// if err is found, bail out
		if innerErr != nil {
			return nil, nil, innerErr
		}
	}
	return blkBatches, blkDels, nil
}

// readblock reads block data. there is no rowid column, no ablk
func (t *CNMergeTask) readblock(info *objectio.BlockInfo) (bat *batch.Batch, dels *nulls.Nulls, err error) {
	// read data
	bat, err = blockio.LoadColumns(t.ctx, t.colseqnums, t.coltypes, t.fs, info.MetaLocation(), t.proc.GetMPool(), fileservice.Policy(0))
	if err != nil {
		return
	}

	// read tombstone on disk
	if !info.DeltaLocation().IsEmpty() {
		obat, byCN, err2 := blockio.ReadBlockDelete(t.ctx, info.DeltaLocation(), t.fs)
		if err2 != nil {
			err = err2
			return
		}
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
