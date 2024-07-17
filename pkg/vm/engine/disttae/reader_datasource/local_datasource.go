package reader_datasource

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

// local data source

type LocalDataSource struct {
	ranges []*objectio.BlockInfoInProgress
	pState *logtailreplay.PartitionState

	unCommittedS3Deletes    map[types.Rowid]struct{}
	unCommittedInmemDeletes map[types.Rowid]struct{}
	unCommittedInmemInserts []*batch.Batch

	ctx          context.Context
	fs           fileservice.FileService
	cursor       int
	snapshotTS   types.TS
	iteratePhase DataState
}

func NewLocalDataSource(
	ctx context.Context,
	mp *mpool.MPool,
	snapshotTS types.TS,
	fs fileservice.FileService,
	databaseId, tableId uint64,
	ranges []*objectio.BlockInfoInProgress,
	pState *logtailreplay.PartitionState,
	unCommittedS3DeletesBat map[types.Blockid][]*batch.Batch,
	unCommittedInmemWrites []disttae.Entry) (source LocalDataSource, err error) {

	source.fs = fs
	source.ctx = ctx

	source.ranges = ranges
	source.pState = pState
	source.snapshotTS = snapshotTS

	source.unCommittedS3Deletes = make(map[types.Rowid]struct{})
	source.unCommittedInmemDeletes = make(map[types.Rowid]struct{})

	if err = loadAllUncommittedS3Deletes(ctx, mp, fs,
		&source.unCommittedS3Deletes, unCommittedS3DeletesBat); err != nil {
		return source, err
	}

	if err = extractAllInsertAndDeletesFromWorkspace(
		databaseId, tableId, &source.unCommittedInmemInserts,
		&source.unCommittedInmemDeletes, unCommittedInmemWrites); err != nil {
		return source, err
	}

	source.iteratePhase = InMem
	return source, nil
}

func (ls *LocalDataSource) HasTombstones(bid types.Blockid) bool {
	if ls.iteratePhase == InMem {
		return false
	}

	if _, _, ok := ls.pState.GetBockDeltaLoc(bid); ok {
		return true
	}

	return false
}

func (ls *LocalDataSource) ApplyTombstones(rows []types.Rowid) (sel []int64, err error) {
	rowIdsToOffsets := func(rowIds []types.Rowid) (ret []int64) {
		for _, row := range rows {
			_, offset := row.Decode()
			ret = append(ret, int64(offset))
		}
		return ret
	}

	if ls.iteratePhase == InMem || ls.iteratePhase == End {
		return rowIdsToOffsets(rows), nil
	}

	var persistedDeletes *nulls.Nulls

	blockId, _ := rows[0].Decode()
	deltaLoc, commitTS, ok := ls.pState.GetBockDeltaLoc(blockId)
	if ok {
		persistedDeletes, err = loadBlockDeletesByDeltaLoc(ls.ctx, ls.fs, blockId, deltaLoc, ls.snapshotTS, commitTS)
	}

	var left []types.Rowid

	for _, row := range rows {
		if _, ok = ls.unCommittedS3Deletes[row]; ok {
			continue
		}

		if _, ok = ls.unCommittedInmemDeletes[row]; ok {
			continue
		}

		if persistedDeletes != nil {
			_, offset := row.Decode()
			if persistedDeletes.Contains(uint64(offset)) {
				continue
			}
		}

		left = append(left, row)
	}

	return rowIdsToOffsets(left), nil
}

func (ls *LocalDataSource) Next(
	ctx context.Context, cols []string, types []types.Type, seqNums []uint16,
	memFilter disttae.MemPKFilterInProgress, txnOffset int, mp *mpool.MPool, vp engine.VectorPool,
	bat *batch.Batch) (*objectio.BlockInfoInProgress, DataState, error) {

	switch ls.iteratePhase {
	case InMem:
		err := ls.iterateInMemData(ctx, cols, types, seqNums, memFilter, bat, mp, vp)
		return nil, InMem, err

	case Persisted:
		if ls.cursor <= len(ls.ranges) {
			ls.cursor++
			return ls.ranges[ls.cursor-1], Persisted, nil
		}

		ls.iteratePhase = End
		return nil, Persisted, nil

	case End:
		return nil, ls.iteratePhase, nil
	}

	return nil, End, nil
}

func (ls *LocalDataSource) iterateInMemData(
	ctx context.Context, cols []string, colTypes []types.Type,
	seqNums []uint16, memFilter disttae.MemPKFilterInProgress, bat *batch.Batch,
	mp *mpool.MPool, vp engine.VectorPool) error {

	defer func() {
		if bat.RowCount() == 0 {
			ls.iteratePhase = Persisted
		}
	}()

	if bat == nil {
		bat = batch.New(true, cols)
	}

	bat.SetRowCount(0)

	if err := ls.filterInMemUncommittedInserts(mp, bat); err != nil {
		return err
	}

	if bat.RowCount() != 0 {
		return nil
	}

	if err := ls.filterInMemCommittedInserts(colTypes, seqNums, memFilter, mp, bat); err != nil {
		return err
	}

	return nil
}

func (ls *LocalDataSource) filterInMemUncommittedInserts(mp *mpool.MPool, bat *batch.Batch) error {
	if len(ls.unCommittedInmemInserts) == 0 {
		return nil
	}

	insertsBat := ls.unCommittedInmemInserts[0]
	ls.unCommittedInmemInserts = ls.unCommittedInmemInserts[1:]

	rowIDs := vector.MustFixedCol[types.Rowid](insertsBat.Vecs[0])
	for i, vec := range bat.Vecs {
		uf := vector.GetUnionOneFunction(*vec.GetType(), mp)

		for j, k := int64(0), int64(bat.RowCount()); j < k; j++ {
			if _, ok := ls.unCommittedInmemDeletes[rowIDs[j]]; ok {
				continue
			}

			if _, ok := ls.unCommittedS3Deletes[rowIDs[j]]; ok {
				continue
			}

			if err := uf(vec, bat.Vecs[i], j); err != nil {
				return err
			}
		}
	}

	bat.SetRowCount(insertsBat.RowCount())

	return nil
}

func (ls *LocalDataSource) filterInMemCommittedInserts(
	colTypes []types.Type, seqNums []uint16,
	memFilter disttae.MemPKFilterInProgress, mp *mpool.MPool, bat *batch.Batch) error {

	var (
		err          error
		appendedRows uint32
		insIter      logtailreplay.RowsIter
	)

	appendFunctions := make([]func(*vector.Vector, *vector.Vector, int64) error, len(bat.Attrs))
	for i, _ := range bat.Attrs {
		//if name == catalog.Row_ID {
		//	appendFunctions[i] = vector.GetUnionOneFunction(types.T_Rowid.ToType(), mp)
		//} else {
		appendFunctions[i] = vector.GetUnionOneFunction(colTypes[i], mp)
		//}
	}

	if memFilter.Spec.Move == nil {
		insIter = ls.pState.NewRowsIter(memFilter.TS, nil, false)
	} else {
		insIter = ls.pState.NewPrimaryKeyIter(memFilter.TS, memFilter.Spec)
	}

	defer insIter.Close()

	for insIter.Next() && appendedRows < options.DefaultBlockMaxRows {
		entry := insIter.Entry()
		if _, ok := ls.unCommittedInmemDeletes[entry.RowID]; ok {
			continue
		}

		if _, ok := ls.unCommittedS3Deletes[entry.RowID]; ok {
			continue
		}

		for i, name := range bat.Attrs {
			if name == catalog.Row_ID {
				if err = vector.AppendFixed(
					bat.Vecs[i],
					entry.RowID,
					false,
					mp); err != nil {
					return err
				}
			} else {
				idx := 2 /*rowid and commits*/ + seqNums[i]
				if int(idx) >= len(entry.Batch.Vecs) /*add column*/ ||
					entry.Batch.Attrs[idx] == "" /*drop column*/ {
					err = vector.AppendAny(
						bat.Vecs[i],
						nil,
						true,
						mp)
				} else {
					err = appendFunctions[i](
						bat.Vecs[i],
						entry.Batch.Vecs[int(2+seqNums[i])],
						entry.Offset,
					)
				}
				if err != nil {
					return err
				}
			}
		}
		appendedRows++
	}

	bat.SetRowCount(int(appendedRows))

	return nil
}
