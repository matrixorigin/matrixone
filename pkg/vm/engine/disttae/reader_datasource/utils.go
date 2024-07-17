package reader_datasource

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

func loadAllUncommittedS3Deletes(
	ctx context.Context,
	mp *mpool.MPool,
	fs fileservice.FileService,
	dest *map[types.Rowid]struct{},
	unCommittedS3DeletesBat map[types.Blockid][]*batch.Batch) error {

	for _, bats := range unCommittedS3DeletesBat {
		for _, bat := range bats {
			vs, area := vector.MustVarlenaRawData(bat.GetVector(0))

			for i := range vs {
				location, err := blockio.EncodeLocationFromString(vs[i].UnsafeGetString(area))
				if err != nil {
					return err
				}

				rowIdBat, release, err := blockio.LoadColumns(ctx, []uint16{0}, nil, fs, location, mp, fileservice.Policy(0))
				if err != nil {
					release()
					return err
				}

				rowIds := vector.MustFixedCol[types.Rowid](rowIdBat.GetVector(0))
				for _, rowId := range rowIds {
					(*dest)[rowId] = struct{}{}
				}

				release()
			}
		}
	}

	return nil
}

func extractAllInsertAndDeletesFromWorkspace(
	databaseId, tableId uint64,
	destInserts *[]*batch.Batch,
	destDeletes *map[types.Rowid]struct{},
	unCommittedInmemWrites []disttae.Entry) error {

	for _, entry := range unCommittedInmemWrites {
		if entry.DatabaseId() != databaseId || entry.TableId() != tableId {
			continue
		}

		if entry.IsGeneratedByTruncate() {
			continue
		}

		if (entry.Type() == disttae.DELETE || entry.Type() == disttae.DELETE_TXN) && entry.FileName() == "" {
			vs := vector.MustFixedCol[types.Rowid](entry.Bat().GetVector(0))
			for _, v := range vs {
				(*destDeletes)[v] = struct{}{}
			}
		}
	}

	for _, entry := range unCommittedInmemWrites {
		if entry.DatabaseId() != databaseId || entry.TableId() != tableId {
			continue
		}

		if entry.IsGeneratedByTruncate() {
			continue
		}

		if entry.Type() == disttae.INSERT || entry.Type() == disttae.INSERT_TXN {
			if entry.Bat() == nil || entry.Bat().IsEmpty() || entry.Bat().Attrs[0] == catalog.BlockMeta_MetaLoc {
				continue
			}

			*destInserts = append(*destInserts, entry.Bat())
		}
	}

	return nil
}

func loadBlockDeletesByDeltaLoc(
	ctx context.Context, fs fileservice.FileService,
	blockId types.Blockid, deltaLoc objectio.ObjectLocation,
	snapshotTS, blockCommitTS types.TS) (deleteMask *nulls.Nulls, err error) {

	var (
		rows             *nulls.Nulls
		bisect           time.Duration
		release          func()
		persistedByCN    bool
		persistedDeletes *batch.Batch
	)

	location := objectio.Location(deltaLoc[:])

	if !location.IsEmpty() {
		t1 := time.Now()

		if persistedDeletes, persistedByCN, release, err = blockio.ReadBlockDelete(ctx, location, fs); err != nil {
			return nil, err
		}
		defer release()

		readCost := time.Since(t1)

		if persistedByCN {
			rows = blockio.EvalDeleteRowsByTimestampForDeletesPersistedByCN(persistedDeletes, snapshotTS, blockCommitTS)
		} else {
			t2 := time.Now()
			rows = blockio.EvalDeleteRowsByTimestamp(persistedDeletes, snapshotTS, &blockId)
			bisect = time.Since(t2)
		}

		if rows != nil {
			deleteMask = rows
		}

		readTotal := time.Since(t1)
		blockio.RecordReadDel(readTotal, readCost, bisect)
	}

	return deleteMask, nil
}
