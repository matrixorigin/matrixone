package dataio

import (
	"bytes"
	"encoding/binary"
	"io"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
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

func NewSegmentWriter(data []*batch.Batch, meta *md.Segment, dir string) *SegmentWriter {
	w := &SegmentWriter{
		data: data,
		meta: meta,
		dir:  dir,
	}
	// w.preprocessor = w.defaultPreprocessor
	w.fileGetter, w.fileCommiter = w.createFile, w.commitFile
	w.dataFlusher = flushBlocks
	w.indexFlusher = w.flushSegIndices
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
	w, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	return w, err
}

func (sw *SegmentWriter) flushSegIndices(w *os.File, data []*batch.Batch, meta *md.Segment) error {
	var indices []index.Index
	for idx, colDef := range meta.Table.Schema.ColDefs {
		switch colDef.Type.Oid {
		case types.T_int8:
			var minv, maxv int8
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]int8)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for _, v := range column {
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_int16:
			var minv, maxv int16
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]int16)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for _, v := range column {
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_int32:
			var minv, maxv int32
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]int32)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for _, v := range column {
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_int64:
			var minv, maxv int64
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]int64)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for _, v := range column {
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_uint8:
			var minv, maxv uint8
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]uint8)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for _, v := range column {
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_uint16:
			var minv, maxv uint16
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]uint16)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for _, v := range column {
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_uint32:
			var minv, maxv uint32
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]uint32)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for _, v := range column {
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_uint64:
			var minv, maxv uint64
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]uint64)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for _, v := range column {
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_float32:
			var minv, maxv float32
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]float32)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for _, v := range column {
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_float64:
			var minv, maxv float64
			for i, blk := range data {
				column := blk.Vecs[idx].Col.([]float64)
				if i == 0 {
					minv = column[0]
					maxv = column[0]
				}
				for _, v := range column {
					if minv > v {
						minv = v
					}
					if maxv < v {
						maxv = v
					}
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_char, types.T_json, types.T_varchar:
			var minv, maxv []byte
			for i, blk := range data {
				column := blk.Vecs[idx].Col.(*types.Bytes)
				if i == 0 {
					minv = column.Get(0)
					maxv = column.Get(0)
				}
				for j := 0; j < len(column.Lengths); j++ {
					v := column.Get(int64(j))
					if bytes.Compare(minv, v) > 0 {
						minv = v
					}
					if bytes.Compare(maxv, v) < 0 {
						maxv = v
					}
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
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
	if err = sw.indexFlusher(w, sw.data, sw.meta); err != nil {
		w.Close()
		return err
	}
	if err = sw.dataFlusher(w, sw.data, sw.meta); err != nil {
		w.Close()
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
	err := binary.Write(w, binary.BigEndian, uint32(len(data)))
	if err != nil {
		return err
	}
	for _, blk := range meta.Blocks {
		if err = binary.Write(w, binary.BigEndian, blk.ID); err != nil {
			return err
		}
	}
	startPos, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err = w.Seek(int64(4*(len(data)+1)), io.SeekCurrent); err != nil {
		return err
	}

	blkMetaPos := make([]uint32, len(data))
	for i, bat := range data {
		pos, _ := w.Seek(0, io.SeekCurrent)
		blkMetaPos[i] = uint32(pos)
		getter := func(string, *md.Block) (*os.File, error) {
			return w, nil
		}
		writer := NewEmbbedBlockWriter(bat, meta.Blocks[i], getter)
		if err = writer.Execute(); err != nil {
			return err
		}
	}
	blkEndPos, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, pos := range blkMetaPos {
		binary.Write(&buf, binary.BigEndian, pos)
	}
	binary.Write(&buf, binary.BigEndian, uint32(blkEndPos))
	if _, err = w.WriteAt(buf.Bytes(), startPos); err != nil {
		return err
	}
	if _, err = w.Seek(blkEndPos, io.SeekStart); err != nil {
		return err
	}
	return nil
}
