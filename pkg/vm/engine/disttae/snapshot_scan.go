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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
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
		"SnapshotScan-ResolveSnapshotState-Start",
		zap.String("caller", caller),
		zap.Uint64("table-id", tbl.tableId),
		zap.String("table-name", tbl.tableName),
		zap.String("db-name", tbl.db.databaseName),
		zap.String("snapshot-ts", snapshotTS.ToString()),
	)

	pState, err := eng.getOrCreateSnapPartBy(ctx, tbl, snapshotTS)
	if err != nil {
		logutil.Error(
			"SnapshotScan-ResolveSnapshotState-Error",
			zap.String("caller", caller),
			zap.Uint64("table-id", tbl.tableId),
			zap.String("table-name", tbl.tableName),
			zap.String("db-name", tbl.db.databaseName),
			zap.String("snapshot-ts", snapshotTS.ToString()),
			zap.Error(err),
		)
		return err
	}
	logutil.Info(
		"SnapshotScan-ResolveSnapshotState-Done",
		zap.String("caller", caller),
		zap.Uint64("table-id", tbl.tableId),
		zap.String("table-name", tbl.tableName),
		zap.String("db-name", tbl.db.databaseName),
		zap.String("snapshot-ts", snapshotTS.ToString()),
	)

	source, err := newSnapshotScanDataSource(ctx, tbl, relData, pState, snapshotTS, mp)
	if err != nil {
		return err
	}

	reader, err := readutil.NewReader(
		ctx,
		mp,
		eng.packerPool,
		eng.fs,
		tbl.tableDef,
		snapshotTS.ToTimestamp(),
		filterExpr,
		source,
		0,
		engine.FilterHint{},
	)
	if err != nil {
		return err
	}
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

func newSnapshotScanDataSource(
	ctx context.Context,
	tbl *txnTable,
	relData engine.RelData,
	pState *logtailreplay.PartitionState,
	snapshotTS types.TS,
	mp *mpool.MPool,
) (*LocalDisttaeDataSource, error) {
	if pState == nil {
		return nil, moerr.NewInternalErrorNoCtx("snapshot scan requires a non-nil partition state")
	}

	ranges := relData.GetBlockInfoSlice()
	skipReadMem := true
	if ranges != nil && ranges.Len() > 0 {
		if ranges.Get(0).IsMemBlk() {
			skipReadMem = false
			ranges = ranges.Slice(1, ranges.Len())
		}
	}

	source := &LocalDisttaeDataSource{
		category:        engine.GeneralLocalDataSource,
		extraTombstones: relData.GetTombstones(),
		rangeSlice:      ranges,
		pState:          pState,
		table:           tbl,
		mp:              mp,
		ctx:             ctx,
		fs:              tbl.getTxn().engine.fs,
		snapshotTS:      snapshotTS,
		tombstonePolicy: engine.Policy_CheckCommittedOnly,
	}

	if ranges == nil || ranges.Len() == 0 {
		source.rc.prefetchDisabled = true
	} else {
		source.rc.prefetchDisabled = ranges.Len() < 4
	}

	source.iteratePhase = engine.InMem
	if skipReadMem {
		source.iteratePhase = engine.Persisted
	}
	return source, nil
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
