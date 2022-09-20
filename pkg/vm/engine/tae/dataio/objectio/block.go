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
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"strconv"
	"strings"
)

type blockFile struct {
	common.RefHelper
	seg     *segmentFile
	rows    uint32
	id      *common.ID
	meta    objectio.BlockObject
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

func (bf *blockFile) WriteBatch(bat *containers.Batch, ts types.TS) (err error) {
	bf.writer = NewWriter(bf.seg.fs, bf.id)
	block, err := bf.writer.WriteBlock(bat)
	bf.meta = block
	return err
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

func (bf *blockFile) GetMeta(metaLoc string) objectio.BlockObject {
	if bf.meta != nil {
		return bf.meta
	}
	info := strings.Split(metaLoc, ":")
	name := info[0]
	location := strings.Split(info[1], "_")
	offset, err := strconv.ParseUint(location[0], 10, 32)
	if err != nil {
		panic(any(err))
	}
	size, err := strconv.ParseUint(location[1], 10, 32)
	if err != nil {
		panic(any(err))
	}
	osize, err := strconv.ParseUint(location[2], 10, 32)
	if err != nil {
		panic(any(err))
	}
	extent := objectio.NewExtent(uint32(offset), uint32(size), uint32(osize))
	if bf.reader == nil {
		bf.reader = NewReader(bf.seg.fs, bf, name)
	}
	block, err := bf.reader.ReadMeta(extent)
	if err != nil {
		panic(any(err))
	}
	bf.meta = block
	return bf.meta
}

func (bf *blockFile) WriteTS(ts types.TS) (err error) {
	return
}

func (bf *blockFile) ReadTS() (ts types.TS, err error) {
	return
}

func (bf *blockFile) WriteDeletes(buf []byte) (err error) {
	return
}

func (bf *blockFile) ReadDeletes(buf []byte) (err error) {
	return
}

func (bf *blockFile) GetDeletesFileStat() (stat common.FileInfo) {
	return nil
}

func (bf *blockFile) WriteIndexMeta(buf []byte) (err error) {
	return
}

func (bf *blockFile) LoadIndexMeta() (any, error) {
	return nil, nil
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

func (bf *blockFile) Sync() error {
	_, err := bf.writer.Sync()
	return err
}

func (bf *blockFile) LoadBatch(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	opts *containers.Options) (bat *containers.Batch, err error) {
	if bf.reader == nil {
		bat = containers.NewBatch()

		for i, _ := range bf.columns {
			vec := containers.MakeVector(colTypes[i], nullables[i], opts)
			bat.AddVector(colNames[i], vec)
			if bf.meta == nil {
				continue
			}
		}
		return bat, err
	}
	return bf.reader.LoadBlkColumns(colTypes, colNames, nullables, opts)
}

func (bf *blockFile) WriteColumnVec(_ types.TS, colIdx int, vec containers.Vector) (err error) {
	return
}

func (bf *blockFile) LoadDeletes() (mask *roaring.Bitmap, err error) {
	return bf.reader.LoadDeletes(bf.id)
}

func (bf *blockFile) LoadUpdates() (masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]any) {
	return
}
