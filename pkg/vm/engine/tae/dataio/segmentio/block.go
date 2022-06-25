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
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/encoding"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/compress"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/adaptors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
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
	bf.indexMeta = newIndex(&columnBlock{block: bf}).dataFile
	bf.indexMeta.file = append(bf.indexMeta.file, bf.seg.GetSegmentFile().NewBlockFile(
		fmt.Sprintf("%d_%d.idx", colCnt, bf.id)))
	bf.indexMeta.file[0].SetCols(uint32(colCnt))
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

func (bf *blockFile) AddColumn(col int) {
	colCnt := len(bf.columns)
	if col > colCnt {
		for i := colCnt; i < col; i++ {
			bf.columns = append(bf.columns, getColumnBlock(i, bf))
		}
	}
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
	bf.deletes.SetFile(
		bf.seg.GetSegmentFile().NewBlockFile(fmt.Sprintf("%d_%d_%d.del", len(bf.columns), bf.id, ts)),
		uint32(len(bf.columns)),
		uint32(len(bf.columns[0].indexes)))
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

func (bf *blockFile) GetDeletesFileStat() (stat common.FileInfo) {
	return bf.deletes.Stat()
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
	for _, del := range bf.deletes.file {
		if del == nil {
			continue
		}
		del.Unref()
	}
	bf.deletes = nil
	for _, idx := range bf.indexMeta.file {
		if idx == nil {
			continue
		}
		idx.Unref()
	}
	bf.indexMeta = nil
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
		size := f.Stat().Size()
		if size == 0 {
			continue
		}
		buf := make([]byte, size)
		if _, err = f.Read(buf); err != nil {
			return
		}
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
			r := bytes.NewBuffer(decompress)
			if _, err = vec.ReadFrom(r); err != nil {
				return
			}
		} else {
			r := bytes.NewBuffer(buf)
			if _, err = vec.ReadFrom(r); err != nil {
				return
			}
		}
		bat.Vecs[i] = vec
	}
	return
}

func (bf *blockFile) WriteColumnVec(ts uint64, colIdx int, vec containers.Vector) (err error) {
	cb, err := bf.OpenColumn(colIdx)
	if err != nil {
		return err
	}
	defer cb.Close()
	err = cb.WriteTS(ts)
	w := adaptors.NewBuffer(nil)
	defer w.Close()
	if _, err = vec.WriteTo(w); err != nil {
		return
	}
	err = cb.WriteData(w.Bytes())
	return
}

func (bf *blockFile) WriteBatch(bat *containers.Batch, ts uint64) (err error) {
	if err = bf.WriteTS(ts); err != nil {
		return
	}
	if err = bf.WriteRows(uint32(bat.Length())); err != nil {
		return
	}
	for colIdx := range bat.Attrs {
		if err = bf.WriteColumnVec(ts, colIdx, bat.Vecs[colIdx]); err != nil {
			return
		}
	}
	return
}

func (bf *blockFile) WriteSnapshot(
	bat *containers.Batch,
	ts uint64,
	masks map[uint16]*roaring.Bitmap,
	vals map[uint16]map[uint32]any,
	deletes *roaring.Bitmap) (err error) {
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
	buffer := adaptors.NewBuffer(nil)
	defer buffer.Close()
	for colIdx := range bat.Attrs {
		cb, err := bf.OpenColumn(colIdx)
		if err != nil {
			return err
		}
		defer cb.Close()
		err = cb.WriteTS(ts)
		if err != nil {
			return err
		}
		vec := bat.Vecs[colIdx]
		updates := vals[uint16(colIdx)]
		if updates != nil {
			buffer.Reset()
			mask := masks[uint16(colIdx)]
			buf, err := mask.ToBytes()
			if err != nil {
				return err
			}
			if err = binary.Write(buffer, binary.BigEndian, uint32(len(buf))); err != nil {
				return err
			}
			if _, err = mask.WriteTo(buffer); err != nil {
				return err
			}
			col := containers.MakeVector(vec.GetType(), true)
			it := mask.Iterator()
			for it.HasNext() {
				row := it.Next()
				v := updates[row]
				col.Append(v)
			}
			if _, err = col.WriteTo(buffer); err != nil {
				return err
			}
			if err = cb.WriteUpdates(buffer.Bytes()); err != nil {
				return err
			}
		}
		buffer.Reset()
		if _, err = vec.WriteTo(buffer); err != nil {
			return err
		}
		if err = cb.WriteData(buffer.Bytes()); err != nil {
			return err
		}
	}
	return
}

func (bf *blockFile) LoadDeletes() (mask *roaring.Bitmap, err error) {
	stats := bf.deletes.Stat()
	if stats.Size() == 0 {
		return
	}
	size := stats.Size()
	osize := stats.OriginSize()
	dnode := common.GPool.Alloc(uint64(size))
	defer common.GPool.Free(dnode)
	if _, err = bf.deletes.Read(dnode.Buf[:size]); err != nil {
		return
	}
	node := common.GPool.Alloc(uint64(osize))
	defer common.GPool.Free(node)

	if _, err = compress.Decompress(dnode.Buf[:size], node.Buf[:osize], compress.Lz4); err != nil {
		return
	}
	mask = roaring.New()
	err = mask.UnmarshalBinary(node.Buf[:osize])
	return
}

func (bf *blockFile) LoadUpdates() (masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]any) {
	for i, cb := range bf.columns {
		uf, err := cb.OpenUpdateFile()
		if err != nil {
			panic(err)
		}
		osize := uf.Stat().OriginSize()
		if osize == 0 {
			continue
		}
		size := uf.Stat().Size()
		node := common.GPool.Alloc(uint64(size))
		defer common.GPool.Free(node)
		buf := node.Buf[:size]
		if _, err = uf.Read(buf); err != nil {
			panic(err)
		}
		onode := common.GPool.Alloc(uint64(osize))
		defer common.GPool.Free(onode)
		obuf := onode.Buf[:osize]
		obuf, err = compress.Decompress(buf, obuf, compress.Lz4)
		if err != nil {
			panic(err)
		}
		maskLen := binary.BigEndian.Uint32(obuf[:4])
		obuf = obuf[4:]
		mask := roaring.New()
		if err = mask.UnmarshalBinary(obuf[:maskLen]); err != nil {
			panic(err)
		}
		obuf = obuf[maskLen:]
		typ := encoding.DecodeType(obuf[:encoding.TypeSize])
		vec := gvec.New(typ)
		if err = vec.Read(obuf); err != nil {
			panic(err)
		}
		val := make(map[uint32]any)
		it := mask.Iterator()
		pos := 0
		for it.HasNext() {
			row := it.Next()
			v := compute.GetValue(vec, uint32(pos))
			val[row] = v
			pos++
		}
		if masks == nil {
			masks = make(map[uint16]*roaring.Bitmap)
			vals = make(map[uint16]map[uint32]any)
		}
		vals[uint16(i)] = val
		masks[uint16(i)] = mask
	}
	return
}
