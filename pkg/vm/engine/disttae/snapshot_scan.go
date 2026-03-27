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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"go.uber.org/zap"
)

// ScanSnapshotWithCurrentRanges reads rows at snapshotTS by reusing the current
// relation handle and its current-view ranges, while rebuilding visibility from
// a snapshot partition state. This avoids historical catalog resolution through
// account/db/table names or table-id re-lookup at the target timestamp.
func ScanSnapshotWithCurrentRanges(
	ctx context.Context,
	caller string,
	rel engine.Relation,
	relData engine.RelData,
	snapshotTS types.TS,
	attrs []string,
	colTypes []types.Type,
	filterExpr *plan.Expr,
	mp *mpool.MPool,
	onBatch func(*batch.Batch) error,
) error {
	if len(attrs) == 0 || relData == nil || relData.DataCnt() == 0 {
		return nil
	}
	if len(attrs) != len(colTypes) {
		return moerr.NewInternalErrorNoCtxf(
			"snapshot scan: attrs/colTypes length mismatch, attrs=%d colTypes=%d",
			len(attrs),
			len(colTypes),
		)
	}
	if mp == nil {
		return moerr.NewInternalErrorNoCtx("snapshot scan requires a non-nil mpool")
	}

	tbl, err := unwrapTxnTable(rel)
	if err != nil {
		return err
	}

	eng, ok := tbl.eng.(*Engine)
	if !ok {
		return moerr.NewInternalErrorNoCtxf(
			"snapshot scan requires disttae engine, got %T",
			tbl.eng,
		)
	}

	logutil.Info(
		"SnapshotScan-Start",
		zap.String("caller", caller),
		zap.Uint64("table-id", tbl.tableId),
		zap.String("table-name", tbl.tableName),
		zap.String("db-name", tbl.db.databaseName),
		zap.String("snapshot-ts", snapshotTS.ToString()),
	)

	// Strip the EmptyBlockInfo marker (signals in-memory data) that Ranges()
	// prepends when Policy_CollectCommittedInmemData is set.  RemoteDataSource
	// doesn't understand this marker and would try to open a zero-path file.
	if relData.DataCnt() > 0 {
		blk := relData.GetBlockInfo(0)
		if blk.IsMemBlk() {
			relData = relData.DataSlice(1, relData.DataCnt())
		}
	}
	if relData.DataCnt() == 0 {
		return nil
	}

	tombstones, err := tbl.CollectTombstones(ctx, 0, engine.Policy_CollectAllTombstones)
	if err != nil {
		return err
	}
	if err = relData.AttachTombstones(tombstones); err != nil {
		return err
	}

	source := readutil.NewRemoteDataSource(
		ctx, eng.fs, snapshotTS.ToTimestamp(), relData,
	)

	seqnums := attrsToSeqnums(attrs, tbl.tableDef)
	reader := readutil.SimpleReaderWithDataSource(
		ctx, eng.fs,
		source, snapshotTS.ToTimestamp(),
		readutil.WithColumns(seqnums, colTypes),
	)

	defer reader.Close()

	readBatch := batch.NewWithSize(len(attrs))
	readBatch.SetAttributes(attrs)
	for i := range attrs {
		readBatch.Vecs[i] = vector.NewVec(colTypes[i])
	}
	defer readBatch.Clean(mp)

	for {
		isEnd, err := reader.Read(ctx, attrs, nil, mp, readBatch)
		if err != nil {
			return err
		}
		if isEnd {
			return nil
		}
		if readBatch.RowCount() == 0 {
			continue
		}
		if err := onBatch(readBatch); err != nil {
			return err
		}
	}
}

func unwrapTxnTable(rel engine.Relation) (*txnTable, error) {
	switch tbl := rel.(type) {
	case *txnTable:
		return tbl, nil
	case *txnTableDelegate:
		return tbl.origin, nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf(
			"snapshot scan requires disttae relation, got %T",
			rel,
		)
	}
}

func attrsToSeqnums(attrs []string, tableDef *plan.TableDef) []uint16 {
	seqnums := make([]uint16, len(attrs))
	for i, attr := range attrs {
		col := strings.ToLower(attr)
		if objectio.IsPhysicalAddr(col) {
			seqnums[i] = objectio.SEQNUM_ROWID
		} else if col == objectio.DefaultCommitTS_Attr {
			seqnums[i] = objectio.SEQNUM_COMMITTS
		} else {
			colIdx := tableDef.Name2ColIndex[col]
			colDef := tableDef.Cols[colIdx]
			seqnums[i] = uint16(colDef.Seqnum)
		}
	}
	return seqnums
}
