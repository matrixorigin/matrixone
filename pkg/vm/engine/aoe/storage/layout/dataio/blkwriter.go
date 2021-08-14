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
type BlockfileGetter func(string, *md.Block) (*os.File, error)

var (
	defaultDataSerializer = flushWithLz4Compression
	// defaultDataSerializer = flushNoCompression
)

type BlockWriter struct {
	data         []*vector.Vector
	meta         *md.Block
	dir          string
	embbed       bool
	fileHandle   *os.File
	fileGetter   BlockfileGetter
	fileCommiter func(string) error

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
	w.fileGetter, w.fileCommiter = w.createIOWriter, w.commitFile
	w.preprocessor = w.defaultPreprocessor
	w.indexSerializer = w.flushIndices
	w.dataSerializer = defaultDataSerializer
	return w
}

func NewEmbbedBlockWriter(bat *batch.Batch, meta *md.Block, getter BlockfileGetter) *BlockWriter {
	w := &BlockWriter{
		data:         bat.Vecs,
		meta:         meta,
		fileGetter:   getter,
		fileCommiter: func(string) error { return nil },
		embbed:       true,
	}
	w.preprocessor = w.defaultPreprocessor
	w.indexSerializer = w.flushIndices
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

func (bw *BlockWriter) commitFile(fname string) error {
	name, err := e.FilenameFromTmpfile(fname)
	if err != nil {
		return err
	}
	err = os.Rename(fname, name)
	return err
}

func (bw *BlockWriter) createIOWriter(dir string, meta *md.Block) (*os.File, error) {
	id := meta.AsCommonID()
	filename := e.MakeBlockFileName(dir, id.ToBlockFileName(), id.TableID, true)
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

func (bw *BlockWriter) defaultPreprocessor(data []*vector.Vector, meta *md.Block) error {
	err := mergesort.SortBlockColumns(data)
	return err
}

func (bw *BlockWriter) flushIndices(w *os.File, data []*vector.Vector, meta *md.Block) error {
	indices := []index.Index{}
	hasBsi := false
	for idx, t := range meta.Segment.Table.Schema.ColDefs {
		if t.Type.Oid == types.T_int32 {
			{
				minv := int32(1) + int32(idx)*100
				maxv := int32(99) + int32(idx)*100
				zmi := index.NewZoneMap(t.Type, minv, maxv, int16(idx))
				indices = append(indices, zmi)
			}
			if !hasBsi {
				// column := data[idx].Col.([]int32)
				// bsiIdx := index.NewNumericBsiIndex(t.Type, 32, int16(idx))
				// for row, val := range column {
				// 	bsiIdx.Set(uint64(row), int64(val))
				// }
				// indices = append(indices, bsiIdx)
				hasBsi = true
			}
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
	fname := bw.fileHandle.Name()
	s, _ := filepath.Abs(bw.fileHandle.Name())
	s, _ = e.FilenameFromTmpfile(fname)
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
	closeFunc := w.Close
	if bw.embbed {
		closeFunc = func() error {
			return nil
		}
	}
	if bw.preExecutor != nil {
		bw.preExecutor()
	}
	if err = bw.indexSerializer(w, bw.data, bw.meta); err != nil {
		closeFunc()
		return err
	}
	if err = bw.dataSerializer(w, bw.data, bw.meta); err != nil {
		closeFunc()
		return err
	}
	if bw.postExecutor != nil {
		bw.postExecutor()
	}
	filename, _ := filepath.Abs(w.Name())
	closeFunc()
	return bw.fileCommiter(filename)
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
