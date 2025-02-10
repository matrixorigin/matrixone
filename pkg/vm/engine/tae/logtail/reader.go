// Copyright 2021 Matrix Origin
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

package logtail

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

// Reader is a snapshot of all txn prepared between from and to.
// Dirty tables/objects/blocks can be queried based on those txn
type Reader struct {
	from, to types.TS
	table    *TxnTable
}

// Merge all dirty table/object/block into one dirty tree
func (r *Reader) GetDirty() (tree *model.Tree, count int) {
	tree = model.NewTree()
	op := func(row RowT) (moveOn bool) {
		if memo := row.GetMemo(); memo.HasAnyTableDataChanges() {
			row.GetTxnState(true)
			tree.Merge(memo.GetDirty())
		}
		count++
		return true
	}
	r.table.ForeachRowInBetween(r.from, r.to, nil, op)
	return
}

// Merge all dirty table/object/block of **a table** into one tree
func (r *Reader) GetDirtyByTable(
	dbID, id uint64,
) (tree *model.TableTree) {
	tree = model.NewTableTree(dbID, id)
	op := func(row RowT) (moveOn bool) {
		if memo := row.GetMemo(); memo.HasTableDataChanges(id) {
			tree.Merge(memo.GetDirtyTableByID(id))
		}
		return true
	}
	skipFn := func(blk BlockT) bool {
		summary := blk.summary.Load()
		if summary == nil {
			return false
		}
		_, exist := summary.tids[id]
		return !exist
	}
	r.table.ForeachRowInBetween(r.from, r.to, skipFn, op)
	return
}

// TODO: optimize
func (r *Reader) GetMaxLSN() (maxLsn uint64) {
	r.table.ForeachRowInBetween(
		r.from,
		r.to,
		nil,
		func(row RowT) (moveOn bool) {
			lsn := row.GetLSN()
			if lsn > maxLsn {
				maxLsn = lsn
			}
			return true
		})
	return
}

type CheckpointReader struct {
	sid       string
	fs        fileservice.FileService
	meta      map[uint64]*CheckpointMeta
	locations []objectio.Location
	version   uint32
}

func MakeGlobalCheckpointDataReader(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	location objectio.Location,
	version uint32,
) (*CheckpointReader, error) {
	metadata := &CheckpointReader{
		meta:      make(map[uint64]*CheckpointMeta),
		locations: make([]objectio.Location, 0),
		sid:       sid,
		fs:        fs,
		version:   version,
	}
	reader, err := ioutil.NewObjectReader(fs, location)
	if err != nil {
		return nil, err
	}
	var bats []*containers.Batch
	item := checkpointDataReferVersions[CheckpointCurrentVersion][MetaIDX]
	if bats, err = LoadBlkColumnsByMeta(
		version, ctx, item.types, item.attrs, uint16(0), reader, common.DebugAllocator,
	); err != nil {
		return nil, err
	}
	defer func() {
		for _, bat := range bats {
			bat.Close()
		}
	}()
	locations := make(map[string]objectio.Location)
	buildMeta(bats[0], locations, metadata.meta)
	for _, loc := range locations {
		metadata.locations = append(metadata.locations, loc)
	}
	return metadata, nil
}

func GetDataSchema() ([]string, []types.Type) {
	return checkpointDataReferVersions[CheckpointCurrentVersion][ObjectInfoIDX].attrs,
		checkpointDataReferVersions[CheckpointCurrentVersion][ObjectInfoIDX].types
}

func (r *CheckpointReader) LoadBatchData(
	ctx context.Context,
	_ []string, _ *plan.Expr,
	mp *mpool.MPool,
	bat *batch.Batch,
) (bool, error) {
	if len(r.locations) == 0 {
		return true, nil
	}
	key := r.locations[0]
	reader, err := ioutil.NewObjectReader(r.fs, key)
	if err != nil {
		return false, err
	}
	var bats []*containers.Batch
	for idx := range checkpointDataReferVersions[CheckpointCurrentVersion] {
		if uint16(idx) != ObjectInfoIDX &&
			uint16(idx) != TombstoneObjectInfoIDX {
			continue
		}
		item := checkpointDataReferVersions[CheckpointCurrentVersion][idx]
		if bats, err = LoadBlkColumnsByMeta(
			CheckpointCurrentVersion, ctx, item.types, item.attrs, uint16(idx), reader, mp,
		); err != nil {
			return false, err
		}
		for i := range bats {
			cnBat := containers.ToCNBatch(bats[i])
			bat, err = bat.Append(ctx, mp, cnBat)
			bats[i].Close()
			if err != nil {
				logutil.Infof("err is %v", err)
				return false, err
			}
		}
	}
	r.locations = r.locations[1:]
	return false, nil
}

func (r *CheckpointReader) Close() {
	r.meta = nil
	r.locations = nil
}
