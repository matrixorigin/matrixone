package reader_datasource

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

type DataState uint8

const (
	InMem DataState = iota
	Persisted
	End
)

type DataSource interface {
	Next(ctx context.Context, cols []string, types []types.Type, seqNums []uint16,
		memFilter disttae.MemPKFilterInProgress, txnOffset int, mp *mpool.MPool,
		vp engine.VectorPool, bat *batch.Batch) (*objectio.BlockInfoInProgress, DataState, error)

	HasTombstones(bid types.Blockid) bool

	// ApplyTombstones Apply tombstones into rows.
	ApplyTombstones(rows []types.Rowid) ([]int64, error)
}
