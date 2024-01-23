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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type CNMergeTask struct {
	ctx context.Context
	// txn
	snapshot types.TS // start ts, fixed
	state    *logtailreplay.PartitionState
	proc     *process.Process

	// schema
	name        string       // table name
	version     uint32       // version
	colseqnums  []uint16     // no rowid column
	coltypes    []types.Type // no rowid column
	blkMaxRow   int
	sortkeyPos  int // (composite) primary key, cluster by etc. -1 meas no sort key
	sortkeyIsPK bool

	// targets
	targets []logtailreplay.ObjectInfo

	// commit things
	entry *mergesort.MergeCommitEntry

	// auxiliaries
	fs fileservice.FileService
}

// impl DisposableVecPool
func (t *CNMergeTask) GetVector(typ *types.Type) (*vector.Vector, func()) {
	v := t.proc.GetVector(*typ)
	return v, func() { t.proc.PutVector(v) }
}

func (t *CNMergeTask) GetMPool() *mpool.MPool {
	return t.proc.GetMPool()
}

func (t *CNMergeTask) PrepareData() ([]*batch.Batch, []*nulls.Nulls, func(), error) {
	r, d, e := t.readAllData()
	return r, d, func() {}, e
}

func (t *CNMergeTask) PrepareCommitEntry() *mergesort.MergeCommitEntry {
	commitEntry := &mergesort.MergeCommitEntry{}
	commitEntry.Tablename = t.name
	commitEntry.StartTs = t.snapshot
	for _, o := range t.targets {
		commitEntry.MergedObjs = append(commitEntry.MergedObjs, o.ObjectStats)
	}
	t.entry = commitEntry
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
		readBlock := func(blk *objectio.BlockInfo, _ objectio.BlockObject) bool {
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
			bat, delmask, innerErr := t.readblock(blk)
			if innerErr != nil {
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

	// read tombstone in memory
	iter := t.state.NewRowsIter(t.snapshot, &info.BlockID, true)
	for iter.Next() {
		entry := iter.Entry()
		_, offset := entry.RowID.Decode()
		dels.Add(uint64(offset))
	}
	iter.Close()
	return
}
