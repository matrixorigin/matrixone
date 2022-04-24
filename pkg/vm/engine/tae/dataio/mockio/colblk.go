package mockio

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
)

type columnBlock struct {
	common.RefHelper
	block   *blockFile
	ts      uint64
	indexes []*indexFile
	updates *updatesFile
	data    *dataFile
}

func newColumnBlock(block *blockFile, indexCnt int) *columnBlock {
	cb := &columnBlock{
		block:   block,
		indexes: make([]*indexFile, indexCnt),
	}
	for i, _ := range cb.indexes {
		cb.indexes[i] = newIndex(cb)
	}
	cb.updates = newUpdates(cb)
	cb.data = newData(cb)
	cb.OnZeroCB = cb.close
	cb.Ref()
	return cb
}

func (cb *columnBlock) WriteTS(ts uint64) (err error) {
	cb.ts = ts
	return
}

func (cb *columnBlock) WriteData(buf []byte) (err error) {
	_, err = cb.data.Write(buf)
	return
}

func (cb *columnBlock) WriteUpdates(buf []byte) (err error) {
	_, err = cb.updates.Write(buf)
	return
}

func (cb *columnBlock) WriteIndex(idx int, buf []byte) (err error) {
	if idx >= len(cb.indexes) {
		err = file.ErrInvalidParam
		return
	}
	vfile := cb.indexes[idx]
	_, err = vfile.Write(buf)
	return
}

func (cb *columnBlock) ReadTS() uint64 { return cb.ts }

func (cb *columnBlock) ReadData(buf []byte) (err error) {
	_, err = cb.data.Read(buf)
	return
}

func (cb *columnBlock) ReadUpdates(buf []byte) (err error) {
	_, err = cb.updates.Read(buf)
	return
}

func (cb *columnBlock) ReadIndex(idx int, buf []byte) (err error) {
	if idx >= len(cb.indexes) {
		err = file.ErrInvalidParam
		return
	}
	vfile := cb.indexes[idx]
	_, err = vfile.Read(buf)
	return
}

func (cb *columnBlock) GetDataFileStat() (stat common.FileInfo) {
	return cb.data.stat
}

func (cb *columnBlock) OpenIndexFile(idx int) (vfile common.IRWFile, err error) {
	if idx >= len(cb.indexes) {
		err = file.ErrInvalidParam
		return
	}
	vfile = cb.indexes[idx]
	vfile.Ref()
	return
}

func (cb *columnBlock) OpenUpdateFile() (vfile common.IRWFile, err error) {
	cb.updates.Ref()
	vfile = cb.data
	return
}

func (cb *columnBlock) OpenDataFile() (vfile common.IRWFile, err error) {
	cb.data.Ref()
	vfile = cb.data
	return
}

func (cb *columnBlock) Close() error {
	cb.Unref()
	// cb.data.Unref()
	// cb.updates.Unref()
	// for _, index := range cb.indexes {
	// 	index.Unref()
	// }
	return nil
}

func (cb *columnBlock) close() {
	cb.Destory()
}

func (cb *columnBlock) Destory() {
	logutil.Infof("Destoring Block %d Col @ TS %d", cb.block.id, cb.ts)
}
