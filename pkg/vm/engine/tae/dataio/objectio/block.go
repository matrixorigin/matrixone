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

package objectio

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/adaptors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type blockFile struct {
	common.RefHelper
	seg     *segmentFile
	rows    uint32
	id      *common.ID
	ts      uint64
	columns []*columnBlock
	writer  *Writer
	reader  *Reader
}

func newBlock(id uint64, seg *segmentFile, colCnt int, indexCnt map[int]int) *blockFile {
	blockID := &common.ID{
		SegmentID: seg.id.SegmentID,
		BlockID:   id,
	}
	bf := &blockFile{
		seg:     seg,
		id:      blockID,
		columns: make([]*columnBlock, colCnt),
		writer:  NewWriter(seg.fs),
		reader:  NewReader(seg.fs),
	}
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

func (bf *blockFile) Fingerprint() *common.ID {
	return bf.id
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
	return
}

func (bf *blockFile) ReadTS() (ts uint64, err error) {
	ts = bf.ts
	return
}

func (bf *blockFile) WriteDeletes(buf []byte) (err error) {
	return bf.writer.WriteDeletes(bf.ts, bf.id, buf)
}

func (bf *blockFile) ReadDeletes(buf []byte) (err error) {
	return
}

func (bf *blockFile) GetDeletesFileStat() (stat common.FileInfo) {
	return nil
}

func (bf *blockFile) WriteIndexMeta(buf []byte) (err error) {
	return bf.writer.WriteIndexMeta(bf.id, buf)
}

func (bf *blockFile) LoadIndexMeta() (any, error) {
	return bf.reader.LoadIndexMeta(bf.id)
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
	return nil
}

func (bf *blockFile) Sync() error { return nil }

func (bf *blockFile) LoadBatch(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	opts *containers.Options) (bat *containers.Batch, err error) {
	return bf.reader.LoadABlkColumns(colTypes, colNames, nullables, opts)
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

func (bf *blockFile) PrepareUpdates(
	tool *containers.CodecTool,
	buffer *bytes.Buffer,
	typ types.Type,
	nullable bool,
	mask *roaring.Bitmap,
	vals map[uint32]any) (err error) {
	_, _ = tool.Write(types.EncodeType(typ))
	_, _ = tool.Write(types.EncodeFixed(nullable))
	buf, err := mask.ToBytes()
	if err != nil {
		return
	}
	_, _ = tool.Write(buf)
	tmp := containers.MakeVector(typ, nullable)
	defer tmp.Close()
	it := mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		v := vals[row]
		tmp.Append(v)
	}
	if _, err = tmp.WriteTo(buffer); err != nil {
		return
	}
	_, _ = tool.Write(buffer.Bytes())
	return
}

func (bf *blockFile) WriteSnapshot(
	bat *containers.Batch,
	ts uint64,
	masks map[uint16]*roaring.Bitmap,
	vals map[uint16]map[uint32]any,
	deletes *roaring.Bitmap) (err error) {
	if err = bf.WriteTS(ts); err != nil {
		return err
	}
	if err = bf.WriteRows(uint32(bat.Length())); err != nil {
		return err
	}

	buffer := new(bytes.Buffer)
	if deletes != nil {
		if _, err = deletes.WriteTo(buffer); err != nil {
			return
		}
		if err = bf.WriteDeletes(buffer.Bytes()); err != nil {
			return
		}
	}
	tool := containers.NewCodecTool()
	defer tool.Close()
	for colIdx := range bat.Attrs {
		var cb file.ColumnBlock
		cb, err = bf.OpenColumn(colIdx)
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
			var uf common.IRWFile
			uf, err = cb.OpenUpdateFile()
			if err != nil {
				panic(err)
			}
			defer uf.Unref()
			mask := masks[uint16(colIdx)]
			buffer.Reset()
			if err = bf.PrepareUpdates(tool, buffer, vec.GetType(), vec.Nullable(), mask, updates); err != nil {
				return
			}
			buffer.Reset()
			if _, err = tool.WriteTo(buffer); err != nil {
				return
			}
			if _, err = uf.Write(buffer.Bytes()); err != nil {
				return
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
	return bf.reader.LoadDeletes(bf.id)
}

func (bf *blockFile) LoadUpdates() (masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]any) {
	return bf.reader.LoadUpdates()
}
