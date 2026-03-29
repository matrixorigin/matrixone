// Copyright 2025 Matrix Origin
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
	"runtime"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

const (
	snapshotScanMaxParallelism     = 4
	snapshotScanMinBlocksPerReader = 512
)

var (
	snapshotScanSubmitPoolOnce sync.Once
	snapshotScanSubmitPool     *ants.Pool
	snapshotScanSubmitPoolErr  error
)

type snapshotScanReaderConfig struct {
	attrs          []string
	seqnums        []uint16
	colTypes       []types.Type
	blockFilter    objectio.BlockReadFilter
	filterSeqnum   uint16
	filterType     types.Type
	hasBlockFilter bool
}

// ScanSnapshotWithCurrentRanges reads rows at snapshotTS by reusing the current
// relation handle and its current-view ranges.
//
// Serial path (parallelism ≤ 1): Uses LocalDataSource which reads both
// in-memory partition-state rows and persisted S3 blocks, ensuring newly
// created catalog entries that have not been flushed are still visible.
//
// Parallel path (parallelism > 1): Uses RemoteDataSource for disk blocks
// only.  In-memory rows are typically flushed for the large tables that
// trigger parallelism.
func ScanSnapshotWithCurrentRanges(
	ctx context.Context,
	caller string,
	rel engine.Relation,
	relData engine.RelData,
	snapshotTS types.TS,
	attrs []string,
	colTypes []types.Type,
	filterExpr *plan.Expr,
	scanParallelism int,
	mp *mpool.MPool,
	onBatch func(*batch.Batch) error,
) error {
	if len(attrs) == 0 {
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

	pState, err := tbl.getPartitionState(ctx)
	if err != nil {
		return err
	}

	// Strip the EmptyBlockInfo marker that Ranges() may prepend.
	if relData != nil && relData.DataCnt() > 0 {
		blk := relData.GetBlockInfo(0)
		if blk.IsMemBlk() {
			relData = relData.DataSlice(1, relData.DataCnt())
		}
	}

	diskBlockCnt := 0
	if relData != nil {
		diskBlockCnt = relData.DataCnt()
	}

	logutil.Debug(
		"SnapshotScan-Start",
		zap.String("caller", caller),
		zap.Uint64("table-id", tbl.tableId),
		zap.String("table-name", tbl.tableName),
		zap.String("db-name", tbl.db.databaseName),
		zap.String("snapshot-ts", snapshotTS.ToString()),
		zap.Int("disk-block-cnt", diskBlockCnt),
	)

	tombstones, err := tbl.CollectTombstones(ctx, 0, engine.Policy_CollectAllTombstones)
	if err != nil {
		return err
	}

	readerCfg, err := buildSnapshotScanReaderConfig(
		tbl.tableDef,
		attrs,
		colTypes,
		filterExpr,
		mp,
	)
	if err != nil {
		return err
	}
	if readerCfg.hasBlockFilter && readerCfg.blockFilter.Cleanup != nil {
		defer readerCfg.blockFilter.Cleanup()
	}

	actualParallelism := normalizeSnapshotScanParallelism(relData, scanParallelism)
	shards := splitSnapshotScanShards(relData, actualParallelism)

	if len(shards) > 1 {
		// Parallel path: disk-only via RemoteDataSource.
		if err = relData.AttachTombstones(tombstones); err != nil {
			return err
		}
		logutil.Debug(
			"SnapshotScan-Parallel-Start",
			zap.String("caller", caller),
			zap.Uint64("table-id", tbl.tableId),
			zap.String("table-name", tbl.tableName),
			zap.String("snapshot-ts", snapshotTS.ToString()),
			zap.Int("block-cnt", diskBlockCnt),
			zap.Int("parallelism", len(shards)),
		)
		if err = scanSnapshotShardsParallel(
			ctx,
			eng.fs,
			snapshotTS,
			shards,
			readerCfg,
			mp,
			onBatch,
		); err != nil {
			return err
		}
		logutil.Debug(
			"SnapshotScan-Parallel-Done",
			zap.String("caller", caller),
			zap.Uint64("table-id", tbl.tableId),
			zap.String("table-name", tbl.tableName),
			zap.String("snapshot-ts", snapshotTS.ToString()),
			zap.Int("block-cnt", diskBlockCnt),
			zap.Int("parallelism", len(shards)),
		)
		return nil
	}

	// Serial path: LocalDataSource reads in-memory rows + disk blocks.
	return scanSnapshotShardLocal(
		ctx, eng.fs, tbl, pState, tombstones,
		snapshotTS, relData, readerCfg, mp, onBatch,
	)
}

func getSnapshotScanSubmitPool() (*ants.Pool, error) {
	snapshotScanSubmitPoolOnce.Do(func() {
		size := runtime.GOMAXPROCS(0) * snapshotScanMaxParallelism
		if size < snapshotScanMaxParallelism {
			size = snapshotScanMaxParallelism
		}
		snapshotScanSubmitPool, snapshotScanSubmitPoolErr = ants.NewPool(size)
	})
	return snapshotScanSubmitPool, snapshotScanSubmitPoolErr
}

func scanSnapshotShardsParallel(
	ctx context.Context,
	fs fileservice.FileService,
	snapshotTS types.TS,
	shards []engine.RelData,
	cfg snapshotScanReaderConfig,
	mp *mpool.MPool,
	onBatch func(*batch.Batch) error,
) error {
	pool, err := getSnapshotScanSubmitPool()
	if err != nil {
		return err
	}

	parallelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg      sync.WaitGroup
		firstMu sync.Mutex
		first   error
	)

	setError := func(err error) {
		if err == nil {
			return
		}
		firstMu.Lock()
		if first == nil {
			first = err
			cancel()
		}
		firstMu.Unlock()
	}

	for _, shard := range shards {
		if shard == nil || shard.DataCnt() == 0 {
			continue
		}
		shard := shard
		wg.Add(1)
		if err := pool.Submit(func() {
			defer wg.Done()
			if parallelCtx.Err() != nil {
				setError(parallelCtx.Err())
				return
			}
			if err := scanSnapshotShard(
				parallelCtx,
				fs,
				snapshotTS,
				shard,
				cfg,
				mp,
				onBatch,
			); err != nil {
				setError(err)
			}
		}); err != nil {
			wg.Done()
			setError(err)
			break
		}
	}

	wg.Wait()
	return first
}

func normalizeSnapshotScanParallelism(relData engine.RelData, requested int) int {
	if relData == nil {
		return 1
	}
	if requested == 1 {
		return 1
	}
	if requested <= 0 {
		requested = snapshotScanMaxParallelism
	}
	if requested > snapshotScanMaxParallelism {
		requested = snapshotScanMaxParallelism
	}
	if gomax := runtime.GOMAXPROCS(0); requested > gomax {
		requested = gomax
	}
	blockCnt := relData.DataCnt()
	if blockCnt < snapshotScanMinBlocksPerReader*2 {
		return 1
	}
	maxByBlocks := blockCnt / snapshotScanMinBlocksPerReader
	if maxByBlocks < 2 {
		return 1
	}
	if requested > maxByBlocks {
		requested = maxByBlocks
	}
	if requested > blockCnt {
		requested = blockCnt
	}
	if requested < 2 {
		return 1
	}
	return requested
}

func splitSnapshotScanShards(relData engine.RelData, parallelism int) []engine.RelData {
	if relData == nil || parallelism <= 1 || relData.DataCnt() == 0 {
		return []engine.RelData{relData}
	}
	raw := relData.Split(parallelism)
	shards := make([]engine.RelData, 0, len(raw))
	for _, shard := range raw {
		if shard == nil || shard.DataCnt() == 0 {
			continue
		}
		shards = append(shards, shard)
	}
	if len(shards) == 0 {
		return []engine.RelData{relData}
	}
	return shards
}

func scanSnapshotShard(
	ctx context.Context,
	fs fileservice.FileService,
	snapshotTS types.TS,
	relData engine.RelData,
	cfg snapshotScanReaderConfig,
	mp *mpool.MPool,
	onBatch func(*batch.Batch) error,
) error {
	source := readutil.NewRemoteDataSource(
		ctx, fs, snapshotTS.ToTimestamp(), relData,
	)

	readerOpts := []readutil.ReaderOption{
		readutil.WithColumns(cfg.seqnums, cfg.colTypes),
	}
	if cfg.hasBlockFilter {
		readerOpts = append(readerOpts, readutil.WithBlockFilter(cfg.blockFilter, cfg.filterSeqnum, cfg.filterType))
	}
	reader := readutil.SimpleReaderWithDataSource(
		ctx, fs,
		source, snapshotTS.ToTimestamp(),
		readerOpts...,
	)

	defer reader.Close()

	readBatch := batch.NewWithSize(len(cfg.attrs))
	readBatch.SetAttributes(cfg.attrs)
	for i := range cfg.attrs {
		readBatch.Vecs[i] = vector.NewVec(cfg.colTypes[i])
	}
	defer readBatch.Clean(mp)

	for {
		isEnd, err := reader.Read(ctx, cfg.attrs, nil, mp, readBatch)
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

// scanSnapshotShardLocal uses LocalDataSource to read both in-memory
// partition-state rows and persisted S3 blocks in a single serial pass.
// The snapshotTS overrides the transaction's own snapshot so the reader
// applies the correct visibility window.
func scanSnapshotShardLocal(
	ctx context.Context,
	fs fileservice.FileService,
	tbl *txnTable,
	pState *logtailreplay.PartitionState,
	tombstones engine.Tombstoner,
	snapshotTS types.TS,
	relData engine.RelData,
	cfg snapshotScanReaderConfig,
	mp *mpool.MPool,
	onBatch func(*batch.Batch) error,
) error {
	var rangesSlice objectio.BlockInfoSlice
	if relData != nil {
		rangesSlice = relData.GetBlockInfoSlice()
	}

	source, err := NewLocalDataSource(
		ctx, tbl,
		0, // txnOffset: committed data only
		pState,
		rangesSlice,
		tombstones,
		false, // skipReadMem: include in-memory rows
		0,     // tombstonePolicy: apply all
		engine.ShardingRemoteDataSource,
	)
	if err != nil {
		return err
	}
	source.snapshotTS = snapshotTS

	readerOpts := []readutil.ReaderOption{
		readutil.WithColumns(cfg.seqnums, cfg.colTypes),
	}
	if cfg.hasBlockFilter {
		readerOpts = append(readerOpts, readutil.WithBlockFilter(cfg.blockFilter, cfg.filterSeqnum, cfg.filterType))
	}
	reader := readutil.SimpleReaderWithDataSource(
		ctx, fs,
		source, snapshotTS.ToTimestamp(),
		readerOpts...,
	)
	defer reader.Close()

	readBatch := batch.NewWithSize(len(cfg.attrs))
	readBatch.SetAttributes(cfg.attrs)
	for i := range cfg.attrs {
		readBatch.Vecs[i] = vector.NewVec(cfg.colTypes[i])
	}
	defer readBatch.Clean(mp)

	for {
		isEnd, err := reader.Read(ctx, cfg.attrs, nil, mp, readBatch)
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

func buildSnapshotScanReaderConfig(
	tableDef *plan.TableDef,
	attrs []string,
	colTypes []types.Type,
	filterExpr *plan.Expr,
	mp *mpool.MPool,
) (snapshotScanReaderConfig, error) {
	cfg := snapshotScanReaderConfig{
		attrs:    attrs,
		seqnums:  attrsToSeqnums(attrs, tableDef),
		colTypes: colTypes,
	}

	blockFilter, filterSeqnum, filterType, ok, err := buildSnapshotBlockFilter(tableDef, filterExpr, mp)
	if err != nil {
		return snapshotScanReaderConfig{}, err
	}
	if ok {
		cfg.blockFilter = blockFilter
		cfg.filterSeqnum = filterSeqnum
		cfg.filterType = filterType
		cfg.hasBlockFilter = true
	}
	return cfg, nil
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

func buildSnapshotBlockFilter(
	tableDef *plan.TableDef,
	filterExpr *plan.Expr,
	mp *mpool.MPool,
) (objectio.BlockReadFilter, uint16, types.Type, bool, error) {
	if filterExpr == nil || tableDef == nil || tableDef.Pkey == nil || mp == nil {
		return objectio.BlockReadFilter{}, 0, types.Type{}, false, nil
	}

	baseFilter, err := readutil.ConstructBasePKFilter(filterExpr, tableDef, mp)
	if err != nil {
		return objectio.BlockReadFilter{}, 0, types.Type{}, false, err
	}
	if !baseFilter.Valid {
		return objectio.BlockReadFilter{}, 0, types.Type{}, false, nil
	}

	colName := strings.ToLower(tableDef.Pkey.PkeyColName)
	colIdx, ok := tableDef.Name2ColIndex[colName]
	if !ok {
		colIdx, ok = tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	}
	if !ok || colIdx < 0 || int(colIdx) >= len(tableDef.Cols) {
		return objectio.BlockReadFilter{}, 0, types.Type{}, false, moerr.NewInternalErrorNoCtxf(
			"snapshot scan: cannot resolve primary key column %q",
			tableDef.Pkey.PkeyColName,
		)
	}

	blockFilter, err := readutil.ConstructBlockPKFilter(
		catalog.IsFakePkName(tableDef.Pkey.PkeyColName),
		baseFilter,
		nil,
	)
	if err != nil {
		return objectio.BlockReadFilter{}, 0, types.Type{}, false, err
	}
	if !blockFilter.Valid {
		return objectio.BlockReadFilter{}, 0, types.Type{}, false, nil
	}

	pkCol := tableDef.Cols[colIdx]
	pkType := plan2.ExprType2Type(&pkCol.Typ)
	pkType.Width = pkCol.Typ.Width
	pkType.Scale = pkCol.Typ.Scale

	return blockFilter, uint16(pkCol.Seqnum), pkType, true, nil
}
