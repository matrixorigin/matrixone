package dataio

import (
	"bytes"
	"encoding/binary"
	"github.com/pierrec/lz4"
	"io"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/mergesort"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path/filepath"
	// log "github.com/sirupsen/logrus"
)

//  BlkCnt | Blk0 Pos | Blk1 Pos | ... | BlkEndPos | Blk0 Data | ...
type SegmentWriter struct {
	data         []*batch.Batch
	meta         *md.Segment
	dir          string
	fileHandle   *os.File
	preprocessor func([]*batch.Batch, *md.Segment) error
	fileGetter   func(string, *md.Segment) (*os.File, error)
	fileCommiter func(string) error
	indexFlusher func(*os.File, []*batch.Batch, *md.Segment) error
	dataFlusher  func(*os.File, []*batch.Batch, *md.Segment) error
	preExecutor  func()
	postExecutor func()
}

var FlushIndex = false

func NewSegmentWriter(data []*batch.Batch, meta *md.Segment, dir string) *SegmentWriter {
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

func (sw *SegmentWriter) SetFileGetter(f func(string, *md.Segment) (*os.File, error)) {
	sw.fileGetter = f
}

func (sw *SegmentWriter) SetIndexFlusher(f func(*os.File, []*batch.Batch, *md.Segment) error) {
	sw.indexFlusher = f
}

func (sw *SegmentWriter) SetDataFlusher(f func(*os.File, []*batch.Batch, *md.Segment) error) {
	sw.dataFlusher = f
}

func (sw *SegmentWriter) defaultPreprocessor(data []*batch.Batch, meta *md.Segment) error {
	err := mergesort.MergeBlocksToSegment(data)
	return err
}

func (sw *SegmentWriter) commitFile(fname string) error {
	name, err := e.FilenameFromTmpfile(fname)
	if err != nil {
		return err
	}
	err = os.Rename(fname, name)
	return err
}

func (sw *SegmentWriter) createFile(dir string, meta *md.Segment) (*os.File, error) {
	id := meta.AsCommonID()
	filename := e.MakeSegmentFileName(dir, id.ToSegmentFileName(), meta.Table.ID, true)
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

func (sw *SegmentWriter) flushIndices(w *os.File, data []*batch.Batch, meta *md.Segment) error {
	if !FlushIndex {
		return nil
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

func flushBlocks(w *os.File, data []*batch.Batch, meta *md.Segment) error {
	// Write Header
	// Write Indice
	// Write Blocks
	var metaBuf bytes.Buffer
	header := make([]byte, 32)
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
	for _, blk := range meta.Blocks {
		if err = binary.Write(&metaBuf, binary.BigEndian, blk.ID); err != nil {
			return err
		}
		if err = binary.Write(&metaBuf, binary.BigEndian, blk.Count); err != nil {
			return err
		}
		var preIdx []byte
		if blk.PrevIndex != nil {
			preIdx, err = blk.PrevIndex.Marshall()
			if err != nil {
				return err
			}
			//if err = binary.Write(&metaBuf, binary.BigEndian, uint8(1)); err != nil {
			//	return err
			//}
		} else {
			preIdx = make([]byte, 32)
			//if err = binary.Write(&metaBuf, binary.BigEndian, uint8(0)); err != nil {
			//	return err
			//}
		}
		//if err = binary.Write(w, binary.BigEndian, uint32(len(preIdx))); err != nil {
		//	return err
		//}
		if err = binary.Write(&metaBuf, binary.BigEndian, preIdx); err != nil {
			return err
		}
		var idx []byte
		if blk.Index != nil {
			idx, err = blk.Index.Marshall()
			if err != nil {
				return err
			}
			//if err = binary.Write(&metaBuf, binary.BigEndian, uint8(1)); err != nil {
			//	return err
			//}
		} else {
			idx = make([]byte, 32)
			//if err = binary.Write(&metaBuf, binary.BigEndian, uint8(0)); err != nil {
			//	return err
			//}
		}
		//if err = binary.Write(w, binary.BigEndian, uint32(len(idx))); err != nil {
		//	return err
		//}
		if err = binary.Write(&metaBuf, binary.BigEndian, idx); err != nil {
			return err
		}
	}

	//colPos := make([]uint32, len(colDefs))
	//var buf bytes.Buffer
	var dataBuf bytes.Buffer
	colSizes := make([]int, colCnt)
	for i := 0; i < colCnt; i++ {
		//pos, _ := w.Seek(0, io.SeekCurrent)
		//colPos[i] = uint32(pos)
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
			//if err = binary.Write(&metaBuf, binary.BigEndian, pos); err != nil {
			//	return err
			//}
			if err = binary.Write(&dataBuf, binary.BigEndian, cbuf); err != nil {
				return err
			}
			colSz += len(cbuf)
		}
		//if _, err := w.Write(buf.Bytes()); err != nil {
		//	return err
		//}
		colSizes[i] = colSz
	}

	metaSize := 32+64+1+4+4+8+8+len(data)*(8+8+32+32)+colCnt*(8+8)*len(data) + colCnt*8
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
			panic(err)
		}
	}


	w.Write(metaBuf.Bytes())
	p, _ := w.Seek(0, io.SeekCurrent)
	w.Write(dataBuf.Bytes())


	return nil
}
