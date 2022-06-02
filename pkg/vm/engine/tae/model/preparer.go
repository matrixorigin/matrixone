package model

import (
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

func PrepareHiddenData(typ types.Type, prefix []byte, startRow, length uint32) (col *movec.Vector, closer func(), err error) {
	bufSize := uint64(typ.Size) * uint64(length)
	n := common.GPool.Alloc(bufSize)
	offsetBuf := make([]byte, 4)
	pos := 0
	for i := uint32(0); i < length; i++ {
		copy(n.Buf[pos:], prefix)
		binary.BigEndian.PutUint32(offsetBuf, startRow+i)
		copy(n.Buf[pos+12:], offsetBuf)
		pos += 16
	}

	col = movec.New(typ)
	payload := encoding.DecodeDecimal128Slice(n.Buf[:pos])
	movec.SetCol(col, payload)
	closer = func() {
		common.GPool.Free(n)
	}
	return
}
