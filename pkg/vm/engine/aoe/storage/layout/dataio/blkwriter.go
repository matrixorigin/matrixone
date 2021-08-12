package dataio

import (
	"bytes"
	"encoding/binary"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine/aoe/mergesort"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path/filepath"

	"github.com/pierrec/lz4"
	// log "github.com/sirupsen/logrus"
)

type BlkDataSerializer func(*os.File, []*vector.Vector, *md.Block) error
type BlkIndexSerializer func(*os.File, []*vector.Vector, *md.Block) error
type FileGetter func(string, *md.Block) (*os.File, error)

var (
	defaultDataSerializer = flushWithLz4Compression
	// defaultDataSerializer = flushNoCompression
)

type BlockWriter struct {
	data       []*vector.Vector
	meta       *md.Block
	dir        string
	embbed     bool
	fileHandle *os.File
	fileGetter FileGetter

	// preprocessor preprocess data before writing, such as SORT
	preprocessor func([]*vector.Vector, *md.Block) error

	// indexSerializer flush indices that pre-defined in meta
	indexSerializer BlkIndexSerializer

	// dataSerializer flush columns data, including compression
	dataSerializer BlkDataSerializer

	preExecutor  func()
	postExecutor func()
}

func NewBlockWriter(data []*vector.Vector, meta *md.Block, dir string) *BlockWriter {
	w := &BlockWriter{
		data: data,
		meta: meta,
		dir:  dir,
	}
	w.fileGetter = w.createIOWriter
	w.preprocessor = w.defaultPreprocessor
	//w.indexSerializer = w.flushBlkIndices
	w.dataSerializer = defaultDataSerializer
	return w
}

func NewEmbbedBlockWriter(bat *batch.Batch, meta *md.Block, getter FileGetter) *BlockWriter {
	w := &BlockWriter{
		data:       bat.Vecs,
		meta:       meta,
		fileGetter: getter,
		embbed:     true,
	}
	w.preprocessor = w.defaultPreprocessor
	w.indexSerializer = w.flushBlkIndices
	w.dataSerializer = defaultDataSerializer
	return w
}

func (bw *BlockWriter) SetPreExecutor(f func()) {
	bw.preExecutor = f
}

func (bw *BlockWriter) SetPostExecutor(f func()) {
	bw.postExecutor = f
}

func (bw *BlockWriter) SetFileGetter(f func(string, *md.Block) (*os.File, error)) {
	bw.fileGetter = f
}

func (bw *BlockWriter) SetIndexFlusher(f BlkIndexSerializer) {
	bw.indexSerializer = f
}

func (bw *BlockWriter) SetDataFlusher(f BlkDataSerializer) {
	bw.dataSerializer = f
}

func (bw *BlockWriter) createIOWriter(dir string, meta *md.Block) (*os.File, error) {
	id := meta.AsCommonID()
	filename := e.MakeBlockFileName(dir, id.ToBlockFileName(), id.TableID)
	fdir := filepath.Dir(filename)
	if _, err := os.Stat(fdir); os.IsNotExist(err) {
		err = os.MkdirAll(fdir, 0755)
		if err != nil {
			return nil, err
		}
	}
	w, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	return w, err
}

func (bw *BlockWriter) defaultPreprocessor(data []*vector.Vector, meta *md.Block) error {
	err := mergesort.SortBlockColumns(data)
	return err
}

func (bw *BlockWriter) flushBlkIndices(w *os.File, data []*vector.Vector, meta *md.Block) error {
	var indices []index.Index
	for idx, colDef := range meta.Segment.Table.Schema.ColDefs {
		switch colDef.Type.Oid {
		case types.T_int8:
			var minv, maxv int8
			column := data[idx].Col.([]int8)
			for i, v := range column {
				if i == 0 {
					maxv = v
					minv = v
				}
				if v > maxv {
					maxv = v
				}
				if v < minv {
					minv = v
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_int16:
			var minv, maxv int16
			column := data[idx].Col.([]int16)
			for i, v := range column {
				if i == 0 {
					maxv = v
					minv = v
				}
				if v > maxv {
					maxv = v
				}
				if v < minv {
					minv = v
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_int32:
			var minv, maxv int32
			column := data[idx].Col.([]int32)
			for i, v := range column {
				if i == 0 {
					maxv = v
					minv = v
				}
				if v > maxv {
					maxv = v
				}
				if v < minv {
					minv = v
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_int64:
			var minv, maxv int64
			column := data[idx].Col.([]int64)
			for i, v := range column {
				if i == 0 {
					maxv = v
					minv = v
				}
				if v > maxv {
					maxv = v
				}
				if v < minv {
					minv = v
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_uint8:
			var minv, maxv uint8
			column := data[idx].Col.([]uint8)
			for i, v := range column {
				if i == 0 {
					maxv = v
					minv = v
				}
				if v > maxv {
					maxv = v
				}
				if v < minv {
					minv = v
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_uint16:
			var minv, maxv uint16
			column := data[idx].Col.([]uint16)
			for i, v := range column {
				if i == 0 {
					maxv = v
					minv = v
				}
				if v > maxv {
					maxv = v
				}
				if v < minv {
					minv = v
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_uint32:
			var minv, maxv uint32
			column := data[idx].Col.([]uint32)
			for i, v := range column {
				if i == 0 {
					maxv = v
					minv = v
				}
				if v > maxv {
					maxv = v
				}
				if v < minv {
					minv = v
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_uint64:
			var minv, maxv uint64
			column := data[idx].Col.([]uint64)
			for i, v := range column {
				if i == 0 {
					maxv = v
					minv = v
				}
				if v > maxv {
					maxv = v
				}
				if v < minv {
					minv = v
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_float32:
			var minv, maxv float32
			column := data[idx].Col.([]float32)
			for i, v := range column {
				if i == 0 {
					maxv = v
					minv = v
				}
				if v > maxv {
					maxv = v
				}
				if v < minv {
					minv = v
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_float64:
			var minv, maxv float64
			column := data[idx].Col.([]float64)
			for i, v := range column {
				if i == 0 {
					maxv = v
					minv = v
				}
				if v > maxv {
					maxv = v
				}
				if v < minv {
					minv = v
				}
			}
			zmi := index.NewZoneMap(colDef.Type, minv, maxv, int16(idx))
			indices = append(indices, zmi)
		case types.T_char, types.T_json, types.T_varchar:
			var minv, maxv []byte
			column := data[idx].Col.(*types.Bytes)
			for i := 0; i < len(column.Lengths); i++ {
				v := column.Get(int64(i))
				if bytes.Compare(minv, v) > 0 {
					minv = v
				}
				if bytes.Compare(maxv, v) < 0 {
					maxv = v
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

func (bw *BlockWriter) GetFileName() string {
	s, _ := filepath.Abs(bw.fileHandle.Name())
	return s
}

func (bw *BlockWriter) Execute() error {
	if bw.preprocessor != nil {
		if err := bw.preprocessor(bw.data, bw.meta); err != nil {
			return err
		}
	}
	w, err := bw.fileGetter(bw.dir, bw.meta)
	if err != nil {
		return err
	}
	bw.fileHandle = w
	if !bw.embbed {
		defer w.Close()
	}
	if bw.preExecutor != nil {
		bw.preExecutor()
	}
	if bw.indexSerializer != nil {
		if err = bw.indexSerializer(w, bw.data, bw.meta); err != nil {
			return err
		}
	}
	if err = bw.dataSerializer(w, bw.data, bw.meta); err != nil {
		return err
	}
	if bw.postExecutor != nil {
		bw.postExecutor()
	}
	return nil
}

func flushWithLz4Compression(w *os.File, data []*vector.Vector, meta *md.Block) error {
	var (
		err error
		buf bytes.Buffer
	)
	algo := uint8(compress.Lz4)
	if err = binary.Write(&buf, binary.BigEndian, uint8(algo)); err != nil {
		return err
	}
	colCnt := len(meta.Segment.Table.Schema.ColDefs)
	if err = binary.Write(&buf, binary.BigEndian, uint16(colCnt)); err != nil {
		return err
	}
	var colBufs [][]byte
	for idx := 0; idx < colCnt; idx++ {
		colBuf, err := data[idx].Show()
		if err != nil {
			return err
		}
		colSize := len(colBuf)
		cbuf := make([]byte, lz4.CompressBlockBound(colSize))
		if cbuf, err = compress.Compress(colBuf, cbuf, compress.Lz4); err != nil {
			return err
		}
		if err = binary.Write(&buf, binary.BigEndian, uint64(len(cbuf))); err != nil {
			return err
		}
		if err = binary.Write(&buf, binary.BigEndian, uint64(colSize)); err != nil {
			return err
		}
		colBufs = append(colBufs, cbuf)
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return err
	}
	for idx := 0; idx < colCnt; idx++ {
		if _, err := w.Write(colBufs[idx]); err != nil {
			return err
		}
	}
	return nil
}

func flushNoCompression(w *os.File, data []*vector.Vector, meta *md.Block) error {
	var (
		err error
		buf bytes.Buffer
	)
	algo := uint8(compress.None)
	if err = binary.Write(&buf, binary.BigEndian, uint8(algo)); err != nil {
		return err
	}
	colCnt := len(meta.Segment.Table.Schema.ColDefs)
	if err = binary.Write(&buf, binary.BigEndian, uint16(colCnt)); err != nil {
		return err
	}
	var colBufs [][]byte
	for idx := 0; idx < colCnt; idx++ {
		colBuf, err := data[idx].Show()
		if err != nil {
			return err
		}
		colSize := len(colBuf)
		cbuf := make([]byte, lz4.CompressBlockBound(colSize))
		if cbuf, err = compress.Compress(colBuf, cbuf, compress.Lz4); err != nil {
			return err
		}
		if err = binary.Write(&buf, binary.BigEndian, uint64(colSize)); err != nil {
			return err
		}
		if err = binary.Write(&buf, binary.BigEndian, uint64(colSize)); err != nil {
			return err
		}
		colBufs = append(colBufs, colBuf)
		// log.Infof("idx=%d, size=%d, osize=%d", idx, len(cbuf), colSize)
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return err
	}
	for idx := 0; idx < colCnt; idx++ {
		if _, err := w.Write(colBufs[idx]); err != nil {
			return err
		}
	}
	return nil
}
