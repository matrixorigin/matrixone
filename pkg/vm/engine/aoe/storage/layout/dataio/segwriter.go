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

package dataio

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"

	"github.com/pierrec/lz4"
)

const (
	headerSize   = 32
	reservedSize = 64
	algoSize     = 1
	blkCntSize   = 4
	colCntSize   = 4
	startPosSize = 8
	endPosSize   = 8
	blkIdSize    = 8
	blkCountSize = 8
	blkIdxSize   = 48
	blkRangeSize = 24
	colSizeSize  = 8
	colPosSize   = 8
)

const Version uint64 = 1

//  BlkCnt | Blk0 Pos | Blk1 Pos | ... | BlkEndPos | Blk0 Data | ...
// SegmentWriter writes block data into the created segment file
// when flushSegEvent(.blk count == SegmentMaxBlocks) is triggered.
type SegmentWriter struct {
	// data is the data of the block file,
	// SegmentWriter does not read from the block file
	data         []*batch.Batch
	meta         *metadata.Segment
	dir          string
	fileHandle   *os.File
	preprocessor func([]*batch.Batch, *metadata.Segment) error

	// fileGetter is createFile()，use dir&TableID&SegmentID to
	// create a tmp file for dataFlusher to flush data
	fileGetter func(string, *metadata.Segment) (*os.File, error)

	// fileCommiter is commitFile()，rename file name after
	// dataFlusher is completed
	fileCommiter func(string) error
	indexFlusher func(*os.File, []*batch.Batch, *metadata.Segment) error
	dataFlusher  func(*os.File, []*batch.Batch, *metadata.Segment) error
	preExecutor  func()
	postExecutor func()
}

var FlushIndex = false

// NewSegmentWriter make a SegmentWriter, which is
// used when (block file count) == SegmentMaxBlocks
func NewSegmentWriter(data []*batch.Batch, meta *metadata.Segment, dir string) *SegmentWriter {
	w := &SegmentWriter{
		data: data,
		meta: meta,
		dir:  dir,
	}
	// w.preprocessor = w.defaultPreprocessor
	w.fileGetter, w.fileCommiter = w.createFile, w.commitFile
	w.dataFlusher = flushBlocks
	w.indexFlusher = w.flushIndices
	return w
}

func (sw *SegmentWriter) SetPreExecutor(f func()) {
	sw.preExecutor = f
}

func (sw *SegmentWriter) SetPostExecutor(f func()) {
	sw.postExecutor = f
}

func (sw *SegmentWriter) SetFileGetter(f func(string, *metadata.Segment) (*os.File, error)) {
	sw.fileGetter = f
}

func (sw *SegmentWriter) SetIndexFlusher(f func(*os.File, []*batch.Batch, *metadata.Segment) error) {
	sw.indexFlusher = f
}

func (sw *SegmentWriter) SetDataFlusher(f func(*os.File, []*batch.Batch, *metadata.Segment) error) {
	sw.dataFlusher = f
}

func (sw *SegmentWriter) defaultPreprocessor(data []*batch.Batch, meta *metadata.Segment) error {
	err := mergesort.MergeBlocksToSegment(data)
	return err
}

func (sw *SegmentWriter) commitFile(fname string) error {
	name, err := common.FilenameFromTmpfile(fname)
	if err != nil {
		return err
	}
	err = os.Rename(fname, name)
	return err
}

func (sw *SegmentWriter) createFile(dir string, meta *metadata.Segment) (*os.File, error) {
	id := meta.AsCommonID()
	filename := common.MakeSegmentFileName(dir, id.ToSegmentFileName(), meta.Table.Id, true)
	fdir := filepath.Dir(filename)
	if _, err := os.Stat(fdir); os.IsNotExist(err) {
		err = os.MkdirAll(fdir, 0755)
		if err != nil {
			return nil, err
		}
	}
	w, err := os.Create(filename)
	return w, err
}

func (sw *SegmentWriter) flushIndices(w *os.File, data []*batch.Batch, meta *metadata.Segment) error {
	if !FlushIndex {
		buf, err := index.DefaultRWHelper.WriteIndices([]index.Index{})
		if err != nil {
			return err
		}
		_, err = w.Write(buf)
		return err
	}
	var indices []index.Index
	for idx, colDef := range meta.Table.Schema.ColDefs {
		switch colDef.Type.Oid {
		case types.T_int8:
			// build segment zone map index
			var minv, maxv, blkMaxv, blkMinv int8
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]int8)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			// build bit-sliced index
			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 8, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]int8)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), val); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		case types.T_int16:
			var minv, maxv, blkMaxv, blkMinv int16
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]int16)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 16, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]int16)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), val); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		case types.T_int32:
			var minv, maxv, blkMaxv, blkMinv int32
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]int32)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 32, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]int32)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), val); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		case types.T_int64:
			var minv, maxv, blkMaxv, blkMinv int64
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]int64)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 64, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]int64)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), val); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		case types.T_uint8:
			var minv, maxv, blkMaxv, blkMinv uint8
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]uint8)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 8, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]uint8)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), val); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		case types.T_uint16:
			var minv, maxv, blkMaxv, blkMinv uint16
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]uint16)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 16, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]uint16)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), val); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		case types.T_uint32:
			var minv, maxv, blkMaxv, blkMinv uint32
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]uint32)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 32, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]uint32)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), val); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		case types.T_uint64:
			var minv, maxv, blkMaxv, blkMinv uint64
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]uint64)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 64, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]uint64)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), val); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		case types.T_float32:
			var minv, maxv, blkMaxv, blkMinv float32
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]float32)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 32, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]float32)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), val); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		case types.T_float64:
			var minv, maxv, blkMaxv, blkMinv float64
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]float64)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 64, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]float64)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), val); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		case types.T_char, types.T_json, types.T_varchar:
			var minv, maxv, blkMaxv, blkMinv []byte
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.(*types.Bytes)
				if i == 0 {
					minv = column.Get(0)
					maxv = column.Get(0)
				}
				for j := 0; j < len(column.Lengths); j++ {
					v := column.Get(int64(j))
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if bytes.Compare(minv, v) > 0 {
						minv = v
					}
					if bytes.Compare(maxv, v) < 0 {
						maxv = v
					}
					if bytes.Compare(blkMinv, v) > 0 {
						blkMinv = v
					}
					if bytes.Compare(blkMaxv, v) < 0 {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			// todo: add bsi
		case types.T_datetime:
			var minv, maxv, blkMaxv, blkMinv types.Datetime
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]types.Datetime)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 64, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]types.Datetime)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), int64(val)); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		case types.T_date:
			var minv, maxv, blkMaxv, blkMinv types.Date
			var blkMin, blkMax []interface{}
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]types.Date)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for j, v := range column {
					if j == 0 {
						blkMaxv = v
						blkMinv = v
					}
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
					if blkMinv > v {
						blkMinv = v
					}
					if blkMaxv < v {
						blkMaxv = v
					}
				}
				blkMin = append(blkMin, blkMinv)
				blkMax = append(blkMax, blkMaxv)
			}
			zmi := index.NewSegmentZoneMap(colDef.Type, minv, maxv, int16(idx), blkMin, blkMax)
			indices = append(indices, zmi)

			bsiIdx := index.NewNumericBsiIndex(colDef.Type, 32, int16(idx))
			row := 0
			for _, blk := range data {
				column := blk.Vecs[idx].Col.([]types.Date)
				for _, val := range column {
					if err := bsiIdx.Set(uint64(row), int32(val)); err != nil {
						return err
					}
					row++
				}
			}
			indices = append(indices, bsiIdx)
		}
	}
	buf, err := index.DefaultRWHelper.WriteIndices(indices)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// Execute steps as follows:
// 1. Create a temp block file.
// 3. Flush indices.
// 4. Compress column data and flush them.
// 5. Rename .tmp file to .blk file.
func (sw *SegmentWriter) Execute() error {
	if sw.preprocessor != nil {
		if err := sw.preprocessor(sw.data, sw.meta); err != nil {
			return err
		}
	}
	w, err := sw.fileGetter(sw.dir, sw.meta)
	if err != nil {
		return err
	}
	sw.fileHandle = w
	if sw.preExecutor != nil {
		sw.preExecutor()
	}
	if err = sw.dataFlusher(w, sw.data, sw.meta); err != nil {
		w.Close()
		return err
	}
	if err = sw.indexFlusher(w, sw.data, sw.meta); err != nil {
		w.Close()
		return err
	}
	footer := make([]byte, 64)
	if _, err = w.Write(footer); err != nil {
		return err
	}
	if sw.postExecutor != nil {
		sw.postExecutor()
	}
	filename, _ := filepath.Abs(w.Name())
	w.Close()
	return sw.fileCommiter(filename)
}

// flushBlocks does not read the .blk file, and writes the incoming
// data&meta into the segemnt file.
func flushBlocks(w *os.File, data []*batch.Batch, meta *metadata.Segment) error {
	var metaBuf bytes.Buffer
	header := make([]byte, 32)
	copy(header, encoding.EncodeUint64(Version))
	err := binary.Write(&metaBuf, binary.BigEndian, header)
	if err != nil {
		return err
	}
	reserved := make([]byte, 64)
	err = binary.Write(&metaBuf, binary.BigEndian, reserved)
	if err != nil {
		return err
	}
	err = binary.Write(&metaBuf, binary.BigEndian, uint8(compress.Lz4))
	if err != nil {
		return err
	}
	err = binary.Write(&metaBuf, binary.BigEndian, uint32(len(data)))
	if err != nil {
		return err
	}
	colDefs := meta.Table.Schema.ColDefs
	colCnt := len(colDefs)
	if err = binary.Write(&metaBuf, binary.BigEndian, uint32(colCnt)); err != nil {
		return err
	}
	for _, blk := range meta.BlockSet {
		if err = binary.Write(&metaBuf, binary.BigEndian, blk.Id); err != nil {
			return err
		}
		if err = binary.Write(&metaBuf, binary.BigEndian, blk.Count); err != nil {
			return err
		}

		rangeBuf, _ := meta.CommitInfo.LogRange.Marshal()
		if err = binary.Write(&metaBuf, binary.BigEndian, rangeBuf); err != nil {
			return err
		}

		var preIdx []byte
		if blk.CommitInfo.PrevIndex != nil {
			preIdx, err = blk.CommitInfo.PrevIndex.Marshal()
			if err != nil {
				return err
			}
		} else {
			preIdx = make([]byte, blkIdxSize)
		}
		if err = binary.Write(&metaBuf, binary.BigEndian, preIdx); err != nil {
			return err
		}
		var idx []byte
		if blk.CommitInfo.LogIndex != nil {
			idx, err = blk.CommitInfo.LogIndex.Marshal()
			if err != nil {
				return err
			}
		} else {
			idx = make([]byte, blkIdxSize)
		}
		if err = binary.Write(&metaBuf, binary.BigEndian, idx); err != nil {
			return err
		}
	}

	var dataBuf bytes.Buffer
	colSizes := make([]int, colCnt)
	for i := 0; i < colCnt; i++ {
		colSz := 0
		for _, bat := range data {
			colBuf, err := bat.Vecs[i].Show()
			if err != nil {
				return err
			}
			colSize := len(colBuf)
			cbuf := make([]byte, lz4.CompressBlockBound(colSize))
			if cbuf, err = compress.Compress(colBuf, cbuf, compress.Lz4); err != nil {
				return err
			}
			if err = binary.Write(&metaBuf, binary.BigEndian, uint64(len(cbuf))); err != nil {
				return err
			}
			if err = binary.Write(&metaBuf, binary.BigEndian, uint64(colSize)); err != nil {
				return err
			}
			if err = binary.Write(&dataBuf, binary.BigEndian, cbuf); err != nil {
				return err
			}
			colSz += len(cbuf)
		}
		colSizes[i] = colSz
	}

	metaSize := headerSize +
		reservedSize +
		algoSize +
		blkCntSize +
		colCntSize +
		startPosSize +
		endPosSize +
		len(data)*(blkCountSize+blkIdSize+2*blkIdxSize+blkRangeSize) +
		len(data)*colCnt*(colSizeSize*2) +
		colCnt*colPosSize

	startPos := int64(metaSize)
	curPos := startPos
	colPoses := make([]int64, colCnt)
	for i, colSz := range colSizes {
		colPoses[i] = curPos
		curPos += int64(colSz)
	}
	endPos := curPos
	if err = binary.Write(&metaBuf, binary.BigEndian, startPos); err != nil {
		return err
	}
	if err = binary.Write(&metaBuf, binary.BigEndian, endPos); err != nil {
		return err
	}
	for _, colPos := range colPoses {
		if err = binary.Write(&metaBuf, binary.BigEndian, colPos); err != nil {
			return err
		}
	}

	if _, err = w.Write(metaBuf.Bytes()); err != nil {
		return err
	}

	if _, err = w.Write(dataBuf.Bytes()); err != nil {
		return err

	}

	return nil
}
