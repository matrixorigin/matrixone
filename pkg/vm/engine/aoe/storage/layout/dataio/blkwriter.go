package dataio

import (
	"bytes"
	"encoding/binary"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path/filepath"
	// "github.com/pierrec/lz4"
	// log "github.com/sirupsen/logrus"
)

type BlockWriter struct {
	data       []*vector.Vector
	meta       *md.Block
	dir        string
	fileHandle *os.File
	fileGetter func(string, *md.Block) (*os.File, error)

	// preprocessor preprocess data before writing, such as SORT
	preprocessor func([]*vector.Vector, *md.Block) error

	// indexFlusher flush indexes that pre-defined in meta
	indexFlusher func(*os.File, []*vector.Vector, *md.Block) error

	// dataFlusher flush columns data, including compression
	dataFlusher func(*os.File, []*vector.Vector, *md.Block) error

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
	w.indexFlusher = w.flushIndexes
	w.dataFlusher = w.flushColsData
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

func (bw *BlockWriter) SetIndexFlusher(f func(*os.File, []*vector.Vector, *md.Block) error) {
	bw.indexFlusher = f
}

func (bw *BlockWriter) SetDataFlusher(f func(*os.File, []*vector.Vector, *md.Block) error) {
	bw.dataFlusher = f
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

func (bw *BlockWriter) flushIndexes(w *os.File, data []*vector.Vector, meta *md.Block) error {
	indexes := []index.Index{}
	hasBsi := false
	for idx, t := range meta.Segment.Table.Schema.ColDefs {
		if t.Type.Oid == types.T_int32 {
			{
				minv := int32(1) + int32(idx)*100
				maxv := int32(99) + int32(idx)*100
				zmi := index.NewZoneMap(t.Type, minv, maxv, int16(idx))
				indexes = append(indexes, zmi)
			}
			if !hasBsi {
				// column := data[idx].Col.([]int32)
				// bsiIdx := index.NewNumericBsiIndex(t.Type, 32, int16(idx))
				// for row, val := range column {
				// 	bsiIdx.Set(uint64(row), int64(val))
				// }
				// indexes = append(indexes, bsiIdx)
				hasBsi = true
			}
		}
	}
	buf, err := index.DefaultRWHelper.WriteIndexes(indexes)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func (bw *BlockWriter) flushColsData(w *os.File, data []*vector.Vector, meta *md.Block) error {
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
		// cbuf := make([]byte, lz4.CompressBlockBound(colSize))
		// if cbuf, err = compress.Compress(colBuf, cbuf, compress.Lz4); err != nil {
		// 	return err
		// }
		if err = binary.Write(&buf, binary.BigEndian, uint64(colSize)); err != nil {
			return err
		}
		if err = binary.Write(&buf, binary.BigEndian, uint64(colSize)); err != nil {
			return err
		}
		colBufs = append(colBufs, colBuf)
		// colBufs = append(colBufs, cbuf)
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
	defer w.Close()
	if bw.preExecutor != nil {
		bw.preExecutor()
	}
	if err = bw.indexFlusher(w, bw.data, bw.meta); err != nil {
		return err
	}
	if err = bw.dataFlusher(w, bw.data, bw.meta); err != nil {
		return err
	}
	if bw.postExecutor != nil {
		bw.postExecutor()
	}
	return nil
}
