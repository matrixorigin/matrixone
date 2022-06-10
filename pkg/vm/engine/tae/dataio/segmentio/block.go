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

package segmentio

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/compress"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
)

type blockFile struct {
	common.RefHelper
	seg       *segmentFile
	rows      uint32
	id        uint64
	ts        uint64
	columns   []*columnBlock
	deletes   *deletesFile
	indexMeta *dataFile
	destroy   sync.Mutex
}

func newBlock(id uint64, seg *segmentFile, colCnt int, indexCnt map[int]int) *blockFile {
	bf := &blockFile{
		seg:     seg,
		id:      id,
		columns: make([]*columnBlock, colCnt),
	}
	bf.deletes = newDeletes(bf)
	bf.deletes.file = make([]*DriverFile, 1)
	bf.deletes.file[0] = bf.seg.GetSegmentFile().NewBlockFile(
		fmt.Sprintf("%d_%d.del", colCnt, bf.id))
	bf.indexMeta = newIndex(&columnBlock{block: bf}).dataFile
	bf.indexMeta.file = make([]*DriverFile, 1)
	bf.indexMeta.file[0] = bf.seg.GetSegmentFile().NewBlockFile(
		fmt.Sprintf("%d_%d.idx", colCnt, bf.id))
	bf.indexMeta.file[0].snode.algo = compress.None
	bf.OnZeroCB = bf.close
	for i := range bf.columns {
		cnt := 0
		if indexCnt != nil {
			cnt = indexCnt[i]
		}
		bf.columns[i] = newColumnBlock(bf, cnt, i)
	}
	bf.Ref()
	return bf
}

func replayBlock(id uint64, seg *segmentFile, colCnt int, indexCnt map[int]int) *blockFile {
	bf := &blockFile{
		seg:     seg,
		id:      id,
		columns: make([]*columnBlock, colCnt),
	}
	bf.deletes = newDeletes(bf)
	bf.deletes.file = make([]*DriverFile, 1)
	bf.indexMeta = newIndex(&columnBlock{block: bf}).dataFile
	bf.indexMeta.file = make([]*DriverFile, 1)
	bf.OnZeroCB = bf.close
	for i := range bf.columns {
		cnt := 0
		if indexCnt != nil {
			cnt = indexCnt[i]
		}
		bf.columns[i] = openColumnBlock(bf, cnt, i)
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
	bf.Close()
	err := bf.Destroy()
	if err != nil {
		panic(any("Destroy error"))
	}
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
	if bf.deletes.file != nil {
		bf.deletes.mutex.Lock()
		defer bf.deletes.mutex.Unlock()
		bf.deletes.file = append(bf.deletes.file,
			bf.seg.GetSegmentFile().NewBlockFile(fmt.Sprintf("%d_%d_%d.del", len(bf.columns), bf.id, ts)))
	}
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
	bf.destroy.Lock()
	defer bf.destroy.Unlock()
	if bf.columns == nil {
		return nil
	}
	for _, cb := range bf.columns {
		cb.Unref()
	}
	bf.columns = nil
	if bf.deletes.file[0] != nil {
		bf.deletes.file[0].driver.ReleaseFile(bf.deletes.file[0])
		bf.deletes = nil
	}
	if bf.indexMeta.file[0] != nil {
		bf.indexMeta.file[0].driver.ReleaseFile(bf.indexMeta.file[0])
		bf.indexMeta = nil
	}
	if bf.seg != nil {
		bf.seg.RemoveBlock(bf.id)
	}
	return nil
}

func (bf *blockFile) Sync() error { return bf.seg.GetSegmentFile().Sync() }

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
		node := common.GPool.Alloc(uint64(size))
		defer common.GPool.Free(node)
		buf := node.Buf[:size]
		if _, err = f.Read(buf); err != nil {
			return
		}
		vec := vector.NewVector(colTypes[i], uint64(maxRow))
		if colBlk.data.stat.CompressAlgo() == compress.Lz4 {
			decompress := make([]byte, colBlk.data.stat.OriginSize())
			decompress, err = compress.Decompress(buf, decompress, compress.Lz4)
			if err != nil {
				return nil, err
			}
			if len(decompress) != int(colBlk.data.stat.OriginSize()) {
				panic(any(fmt.Sprintf("invalid decompressed size: %d, %d is expected",
					len(decompress), colBlk.data.stat.OriginSize())))
			}
			if err = vec.Unmarshal(decompress); err != nil {
				return
			}
		} else {
			if err = vec.Unmarshal(buf); err != nil {
				return
			}
		}
		vec.ResetReadonly()
		vecs[i] = vec
		attrs[i] = i
	}
	bat, err = batch.NewBatch(attrs, vecs)
	return
}

func (bf *blockFile) LoadBatch(attrs []string, colTypes []types.Type) (bat *gbat.Batch, err error) {
	bat = gbat.New(true, attrs)
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
		vec := gvec.New(colTypes[i])
		if colBlk.data.stat.CompressAlgo() == compress.Lz4 {
			decompress := make([]byte, colBlk.data.stat.OriginSize())
			decompress, err = compress.Decompress(buf, decompress, compress.Lz4)
			if err != nil {
				return nil, err
			}
			if len(decompress) != int(colBlk.data.stat.OriginSize()) {
				panic(any(fmt.Sprintf("invalid decompressed size: %d, %d is expected",
					len(decompress), colBlk.data.stat.OriginSize())))
			}
			if err = vec.Read(decompress); err != nil {
				return
			}
		} else {
			if err = vec.Read(buf); err != nil {
				return
			}
		}
		bat.Vecs[i] = vec
	}
	return
}

func (bf *blockFile) WriteColumnVec(ts uint64, colIdx int, vec *gvec.Vector) (err error) {
	cb, err := bf.OpenColumn(colIdx)
	if err != nil {
		return err
	}
	defer cb.Close()
	err = cb.WriteTS(ts)
	buf, err := vec.Show()
	if err != nil {
		return err
	}
	err = cb.WriteData(buf)
	return
}

func (bf *blockFile) WriteBatch(bat *gbat.Batch, ts uint64) (err error) {
	if err = bf.WriteTS(ts); err != nil {
		return
	}
	if err = bf.WriteRows(uint32(gvec.Length(bat.Vecs[0]))); err != nil {
		return
	}
	for colIdx := range bat.Attrs {
		if err = bf.WriteColumnVec(ts, colIdx, bat.Vecs[colIdx]); err != nil {
			return
		}
	}
	return
}

func (bf *blockFile) WriteIBatch(bat batch.IBatch, ts uint64, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]any, deletes *roaring.Bitmap) (err error) {
	attrs := bat.GetAttrs()
	var w bytes.Buffer
	if deletes != nil {
		if _, err = deletes.WriteTo(&w); err != nil {
			return
		}
	}
	if err = bf.WriteTS(ts); err != nil {
		return err
	}
	if err = bf.WriteRows(uint32(bat.Length())); err != nil {
		return err
	}
	for _, colIdx := range attrs {
		cb, err := bf.OpenColumn(colIdx)
		if err != nil {
			return err
		}
		defer cb.Close()
		err = cb.WriteTS(ts)
		if err != nil {
			return err
		}
		vec, err := bat.GetVectorByAttr(colIdx)
		if err != nil {
			return err
		}
		updates := vals[uint16(colIdx)]
		if updates != nil {
			w.Reset()
			mask := masks[uint16(colIdx)]
			if _, err = mask.WriteTo(&w); err != nil {
				return err
			}
			col := gvec.New(vec.GetDataType())
			it := mask.Iterator()
			for it.HasNext() {
				row := it.Next()
				v := updates[row]
				compute.AppendValue(col, v)
			}
			buf, err := col.Show()
			if err != nil {
				return err
			}
			w.Write(buf)
			if err = cb.WriteUpdates(w.Bytes()); err != nil {
				return err
			}
		}
		w.Reset()
		buf, err := vec.Marshal()
		if err != nil {
			return err
		}
		if err = cb.WriteData(buf); err != nil {
			return err
		}
	}
	return
}
