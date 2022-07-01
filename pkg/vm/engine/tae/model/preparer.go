package model

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func PrepareHiddenData(typ types.Type, prefix []byte, startRow, length uint32) (col containers.Vector, err error) {
	col = containers.MakeVector(typ, false)
	buf := make([]byte, 16)
	offsetBuf := make([]byte, 4)
	for i := uint32(0); i < length; i++ {
		EncodeHiddenKeyWithPrefix(buf, prefix, offsetBuf, startRow+i)
		col.Append(types.DecodeFixed[types.Decimal128](buf))
	}
	return
}

type PreparedCompactedBlockData struct {
	Columns *containers.Batch
	SortKey containers.Vector
}

func NewPreparedCompactedBlockData() *PreparedCompactedBlockData {
	return &PreparedCompactedBlockData{}
}

func (preparer *PreparedCompactedBlockData) Close() {
	if preparer.Columns != nil {
		preparer.Columns.Close()
	}
	preparer.Columns.Close()
	if preparer.SortKey != nil {
		preparer.SortKey.Close()
	}
}
