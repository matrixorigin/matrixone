// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// CountSealedBlocks counts blocks whose rows require no per-row MVCC check.
// remaining contains precisely the original ranges still owned by the reader.
// A nil remaining means this relation cannot use this optimization.
func CountSealedBlocks(
	ctx context.Context, rel engine.Relation, data engine.RelData, txnOffset int, analyzer process.Analyzer,
) (count int64, remaining engine.RelData, err error) {
	var tbl *txnTable
	switch r := rel.(type) {
	case *txnTable:
		tbl = r
	case *txnTableDelegate:
		if r.combined.is || r.shard.is {
			return 0, nil, nil
		}
		tbl = r.origin
	default:
		return 0, nil, nil
	}
	switch data.(type) {
	case *readutil.ObjListRelData, *readutil.BlockListRelData:
	default:
		return 0, nil, nil
	}
	if tbl.db.op.IsSnapOp() || data.DataCnt() == 0 {
		return 0, nil, nil
	}
	source, err := tbl.buildLocalDataSource(ctx, txnOffset, data, engine.Policy_CheckAll, engine.GeneralLocalDataSource)
	if err != nil {
		return 0, nil, err
	}
	defer source.Close()

	return countSealedBlocks(ctx, data, source, tbl.getTxn().engine.fs, analyzer)
}

func countSealedBlocks(
	ctx context.Context, data engine.RelData, source engine.DataSource, fs fileservice.FileService,
	analyzer process.Analyzer,
) (int64, engine.RelData, error) {
	remaining := data.BuildEmptyRelData(1)
	if err := remaining.AttachTombstones(data.GetTombstones()); err != nil {
		return 0, nil, err
	}
	var (
		total      int64
		lastObject objectio.ObjectId
		meta       objectio.ObjectDataMeta
		haveMeta   bool
	)
	err := engine.ForRangeBlockInfo(0, data.DataCnt(), data, func(blk *objectio.BlockInfo) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		loc := blk.MetaLocation()
		if blk.IsMemBlk() || blk.IsAppendable() || loc.IsEmpty() {
			remaining.AppendBlockInfo(blk)
			return true, nil
		}
		// Range expansion can estimate block sizes from object stats. Read the
		// block header for the exact count, without prefetching ordinary columns.
		if object := loc.ObjectId(); !haveMeta || object != lastObject {
			loaded, err := process.MeasureFilesystemWait(analyzer, func() (objectio.ObjectMeta, error) {
				return objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
			})
			if err != nil {
				return false, err
			}
			meta, _ = loaded.DataMeta()
			lastObject, haveMeta = object, true
		}
		id := uint32(loc.ID())
		if meta.IsEmpty() || id < uint32(meta.BlockHeader().StartID()) ||
			id-uint32(meta.BlockHeader().StartID()) >= meta.BlockCount() {
			remaining.AppendBlockInfo(blk)
			return true, nil
		}
		rows := meta.GetBlockMeta(id).GetRows()
		deleted, err := process.MeasureFilesystemWait(analyzer, func() (objectio.Bitmap, error) {
			return source.GetTombstones(ctx, &blk.BlockID)
		})
		if err != nil {
			return false, err
		}
		total += int64(rows) - int64(deleted.Bitmap().CountRange(0, uint64(rows)))
		deleted.Release()
		return true, nil
	})
	if err != nil {
		return 0, nil, err
	}
	return total, remaining, ctx.Err()
}
