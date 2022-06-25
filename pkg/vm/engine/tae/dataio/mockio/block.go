// Copyright 2021 Matrix Origin
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

package mockio

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type blockFile struct {
	common.RefHelper
	seg       file.Segment
	rows      uint32
	id        uint64
	ts        uint64
	columns   []*columnBlock
	deletes   *deletesFile
	indexMeta *dataFile
}

func (bf *blockFile) GetDeletesFileStat() common.FileInfo {
	panic(any("implement me"))
}

func newBlock(id uint64, seg file.Segment, colCnt int, indexCnt map[int]int) *blockFile {
	bf := &blockFile{
		seg:     seg,
		id:      id,
		columns: make([]*columnBlock, colCnt),
	}
	bf.deletes = newDeletes(bf)
	bf.indexMeta = newData(nil)
	bf.OnZeroCB = bf.close
	for i := range bf.columns {
		cnt := 0
		if indexCnt != nil {
			cnt = indexCnt[i]
		}
		bf.columns[i] = newColumnBlock(bf, cnt)
	}
	bf.Ref()
	return bf
}

func (bf *blockFile) Fingerprint() *common.ID {
	return &common.ID{
		BlockID: bf.id,
	}
}

func (bf *blockFile) close() {
	_ = bf.Close()
	_ = bf.Destroy()
}

func (bf *blockFile) WriteRows(rows uint32) (err error) {
	bf.rows = rows
	return nil
}

func (bf *blockFile) ReadRows() uint32 {
	return bf.rows
}

func (bf *blockFile) WriteTS(ts uint64) (err error) {
	bf.ts = ts
	return
}

func (bf *blockFile) ReadTS() (ts uint64, err error) {
	ts = bf.ts
	return
}

func (bf *blockFile) WriteDeletes(buf []byte) (err error) {
	_, err = bf.deletes.Write(buf)
	return
}

func (bf *blockFile) ReadDeletes(buf []byte) (err error) {
	_, err = bf.deletes.Read(buf)
	return
}

func (bf *blockFile) WriteIndexMeta(buf []byte) (err error) {
	_, err = bf.indexMeta.Write(buf)
	return
}

func (bf *blockFile) LoadIndexMeta() (any, error) {
	size := bf.indexMeta.Stat().Size()
	buf := make([]byte, size)
	_, err := bf.indexMeta.Read(buf)
	if err != nil {
		return nil, err
	}
	indices := indexwrapper.NewEmptyIndicesMeta()
	if err = indices.Unmarshal(buf); err != nil {
		return nil, err
	}
	return indices, nil
}

func (bf *blockFile) OpenColumn(colIdx int) (colBlk file.ColumnBlock, err error) {
	if colIdx >= len(bf.columns) {
		err = file.ErrInvalidParam
		return
	}
	bf.columns[colIdx].Ref()
	colBlk = bf.columns[colIdx]
	return
}

func (bf *blockFile) Close() error {
	return nil
}

func (bf *blockFile) Destroy() error {
	// logutil.Infof("Destroying Blk %d @ TS %d", bf.id, bf.ts)
	for _, cb := range bf.columns {
		cb.Unref()
	}
	bf.columns = nil
	bf.deletes = nil
	bf.indexMeta = nil
	if bf.seg != nil {
		bf.seg.RemoveBlock(bf.id)
	}
	return nil
}

func (bf *blockFile) Sync() error { return nil }

func (bf *blockFile) LoadIBatch(colTypes []types.Type, maxRow uint32) (bat batch.IBatch, err error) {
	attrs := make([]int, len(bf.columns))
	vecs := make([]vector.IVector, len(attrs))
	var f common.IRWFile
	for i, colBlk := range bf.columns {
		if f, err = colBlk.OpenDataFile(); err != nil {
			return
		}
		defer f.Unref()
		size := f.Stat().Size()
		buf := make([]byte, size)
		if _, err = f.Read(buf); err != nil {
			return
		}
		vec := vector.NewVector(colTypes[i], uint64(maxRow))
		if err = vec.Unmarshal(buf); err != nil {
			return
		}
		vecs[i] = vec
		attrs[i] = i
	}
	bat, err = batch.NewBatch(attrs, vecs)
	return
}

func (bf *blockFile) LoadBatch(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	opts *containers.Options) (bat *containers.Batch, err error) {
	bat = containers.NewBatch()
	var f common.IRWFile
	for i, colBlk := range bf.columns {
		if f, err = colBlk.OpenDataFile(); err != nil {
			return
		}
		defer f.Unref()
		vec := containers.MakeVector(colTypes[i], nullables[i], opts)
		bat.AddVector(colNames[i], vec)
		if f.Stat().Size() > 0 {
			if err = vec.ReadFromFile(f, nil); err != nil {
				return
			}
		}
	}
	return
}

func (bf *blockFile) WriteColumnVec(ts uint64, colIdx int, vec containers.Vector) (err error) {
	panic("not implemented")
}

func (bf *blockFile) WriteBatch(bat *containers.Batch, ts uint64) (err error) {
	panic("not implemented")
}

func (bf *blockFile) WriteSnapshot(
	bat *containers.Batch,
	ts uint64,
	masks map[uint16]*roaring.Bitmap,
	vals map[uint16]map[uint32]any,
	deletes *roaring.Bitmap) (err error) {
	panic("not implemented")
}

func (bf *blockFile) LoadDeletes() (mask *roaring.Bitmap, err error) { panic("implement me") }
func (bf *blockFile) LoadUpdates() (map[uint16]*roaring.Bitmap, map[uint16]map[uint32]any) {
	panic("implement me")
}
