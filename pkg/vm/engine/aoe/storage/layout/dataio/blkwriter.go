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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"os"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/compress"
	gbatch "github.com/matrixorigin/matrixone/pkg/container/batch"
	gvector "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"

	"github.com/pierrec/lz4"
)

type vecsSerializer func(*os.File, []*gvector.Vector, *metadata.Block) error
type vecsIndexSerializer func(*os.File, []*gvector.Vector, *metadata.Block) error
type ivecsSerializer func(*os.File, []vector.IVectorNode, *metadata.Block) error

type blockFileGetter func(string, *metadata.Block) (*os.File, error)

var (
	defaultVecsSerializer  = lz4CompressionVecs
	defaultIVecsSerializer = lz4CompressionIVecs
	// defaultVecsSerializer = noCompressionVecs
)

// BlockWriter writes memTable data into
// the created block file when memTable flush()
type BlockWriter struct {
	data       []*gvector.Vector
	idata      batch.IBatch
	meta       *metadata.Block
	size       int64
	dir        string
	embbed     bool
	fileHandle *os.File

	// fileGetter is createIOWriter()，use dir&TableID&SegmentID&BlockID to
	// create a tmp file for dataSerializer to flush data
	fileGetter blockFileGetter

	// fileCommiter is commitFile()，rename file name after
	// dataSerializer is completed
	fileCommiter func(string) error

	// preprocessor preprocess data before writing, such as SORT
	preprocessor func([]*gvector.Vector, *metadata.Block) error

	// indexSerializer flush indices that pre-defined in meta
	indexSerializer vecsIndexSerializer

	// vecsSerializer flush columns data, including compression
	vecsSerializer  vecsSerializer
	ivecsSerializer ivecsSerializer

	preExecutor  func()
	postExecutor func()
}

// NewBlockWriter make a BlockWriter, which will be used when the memtable is full.
func NewBlockWriter(data []*gvector.Vector, meta *metadata.Block, dir string) *BlockWriter {
	w := &BlockWriter{
		data: data,
		meta: meta,
		dir:  dir,
	}
	w.fileGetter, w.fileCommiter = w.createIOWriter, w.commitFile
	w.preprocessor = w.defaultPreprocessor
	w.indexSerializer = w.flushIndices
	w.vecsSerializer = defaultVecsSerializer
	w.ivecsSerializer = defaultIVecsSerializer
	return w
}

// NewIBatchWriter make a BlockWriter, which will be used
// when the memtable is not full but needs to be flush()
func NewIBatchWriter(bat batch.IBatch, meta *metadata.Block, dir string) *BlockWriter {
	w := &BlockWriter{
		idata: bat,
		meta:  meta,
		dir:   dir,
	}
	w.fileGetter, w.fileCommiter = w.createIOWriter, w.commitFile
	w.ivecsSerializer = defaultIVecsSerializer
	return w
}

func NewEmbbedBlockWriter(bat *gbatch.Batch, meta *metadata.Block, getter blockFileGetter) *BlockWriter {
	w := &BlockWriter{
		data:         bat.Vecs,
		meta:         meta,
		fileGetter:   getter,
		fileCommiter: func(string) error { return nil },
		embbed:       true,
	}
	w.preprocessor = w.defaultPreprocessor
	w.indexSerializer = w.flushIndices
	w.vecsSerializer = defaultVecsSerializer
	return w
}

func (bw *BlockWriter) SetPreExecutor(f func()) {
	bw.preExecutor = f
}

func (bw *BlockWriter) SetPostExecutor(f func()) {
	bw.postExecutor = f
}

func (bw *BlockWriter) SetFileGetter(f func(string, *metadata.Block) (*os.File, error)) {
	bw.fileGetter = f
}

func (bw *BlockWriter) SetIndexFlusher(f vecsIndexSerializer) {
	bw.indexSerializer = f
}

func (bw *BlockWriter) SetDataFlusher(f vecsSerializer) {
	bw.vecsSerializer = f
}

func (bw *BlockWriter) GetSize() int64 {
	return bw.size
}

func (bw *BlockWriter) commitFile(fname string) error {
	name, err := common.FilenameFromTmpfile(fname)
	if err != nil {
		return err
	}
	err = os.Rename(fname, name)
	return err
}

func (bw *BlockWriter) createIOWriter(dir string, meta *metadata.Block) (*os.File, error) {
	id := meta.AsCommonID()
	filename := common.MakeBlockFileName(dir, id.ToBlockFileName(), id.TableID, true)
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

func (bw *BlockWriter) defaultPreprocessor(data []*gvector.Vector, meta *metadata.Block) error {
	err := mergesort.SortBlockColumns(data,meta.Segment.Table.Schema.PrimaryKey)
	return err
}

func (bw *BlockWriter) flushIndices(w *os.File, data []*gvector.Vector, meta *metadata.Block) error {
	var indices []index.Index
	for idx, colDef := range meta.Segment.Table.Schema.ColDefs {
		typ := colDef.Type
		isPrimary := idx == meta.Segment.Table.Schema.PrimaryKey
		zmi, err := index.BuildBlockZoneMapIndex(data[idx], typ, int16(idx), isPrimary)
		if err != nil {
			return err
		}
		indices = append(indices, zmi)
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
	s, _ = common.FilenameFromTmpfile(fname)
	return s
}

func (bw *BlockWriter) Execute() error {
	if bw.data != nil {
		return bw.executeVecs()
	} else if bw.idata != nil {
		return bw.executeIVecs()
	}
	panic("logic error")
}

// excuteIVecs steps as follows:
// 1. Create a temp block file.
// 2. Serialize column data
// 3. Compress column data and flush them.
// 4. Rename .tmp file to .blk file.
func (bw *BlockWriter) executeIVecs() error {
	w, err := bw.fileGetter(bw.dir, bw.meta)
	if err != nil {
		return err
	}
	bw.fileHandle = w
	if bw.preExecutor != nil {
		bw.preExecutor()
	}
	data := make([]vector.IVectorNode, len(bw.meta.Segment.Table.Schema.ColDefs))
	for i := 0; i < len(data); i++ {
		ivec, err := bw.idata.GetVectorByAttr(i)
		if err != nil {
			return err
		}
		data[i] = ivec.(vector.IVectorNode)
	}
	if err = bw.ivecsSerializer(w, data, bw.meta); err != nil {
		w.Close()
		return err
	}
	if bw.postExecutor != nil {
		bw.postExecutor()
	}
	filename, _ := filepath.Abs(w.Name())
	w.Close()
	stat, _ := os.Stat(filename)
	logutil.Infof("filename is %v",filename)
	bw.size = stat.Size()
	return bw.fileCommiter(filename)
}

// executeVecs steps as follows:
// 1. Sort data in memtable.
// 2. Create a temp block file.
// 3. Flush indices.
// 4. Compress column data and flush them.
// 5. Rename .tmp file to .blk file.
func (bw *BlockWriter) executeVecs() error {
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
	if err = bw.vecsSerializer(w, bw.data, bw.meta); err != nil {
		closeFunc()
		return err
	}
	if bw.postExecutor != nil {
		bw.postExecutor()
	}
	filename, _ := filepath.Abs(w.Name())
	closeFunc()
	stat, _ := os.Stat(filename)
	bw.size = stat.Size()
	return bw.fileCommiter(filename)
}

func lz4CompressionVecs(w *os.File, data []*gvector.Vector, meta *metadata.Block) error {
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
	count := meta.Count
	if err = binary.Write(&buf, binary.BigEndian, count); err != nil {
		return err
	}

	rangeBuf, _ := meta.CommitInfo.LogRange.Marshal()
	if err = binary.Write(&buf, binary.BigEndian, rangeBuf); err != nil {
		return err
	}

	var preIdx []byte
	if meta.CommitInfo.PrevIndex != nil {
		preIdx, err = meta.CommitInfo.PrevIndex.Marshal()
		if err != nil {
			return err
		}
	}
	if err = binary.Write(&buf, binary.BigEndian, int32(len(preIdx))); err != nil {
		return err
	}
	if err = binary.Write(&buf, binary.BigEndian, preIdx); err != nil {
		return err
	}
	var idx []byte
	if meta.CommitInfo.LogIndex != nil {
		idx, err = meta.CommitInfo.LogIndex.Marshal()
		if err != nil {
			return err
		}
	}
	if err = binary.Write(&buf, binary.BigEndian, int32(len(idx))); err != nil {
		return err
	}
	if err = binary.Write(&buf, binary.BigEndian, idx); err != nil {
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

func noCompressionVecs(w *os.File, data []*gvector.Vector, meta *metadata.Block) error {
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

func lz4CompressionIVecs(w *os.File, data []vector.IVectorNode, meta *metadata.Block) error {
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
	count := meta.Count
	if err = binary.Write(&buf, binary.BigEndian, count); err != nil {
		return err
	}

	rangeBuf, _ := meta.CommitInfo.LogRange.Marshal()
	if _, err = buf.Write(rangeBuf); err != nil {
		return err
	}

	var preIdx []byte
	if meta.CommitInfo.PrevIndex != nil {
		preIdx, err = meta.CommitInfo.PrevIndex.Marshal()
		if err != nil {
			return err
		}
	}
	if err = binary.Write(&buf, binary.BigEndian, int32(len(preIdx))); err != nil {
		return err
	}
	if err = binary.Write(&buf, binary.BigEndian, preIdx); err != nil {
		return err
	}
	var idx []byte
	if meta.CommitInfo.LogIndex != nil {
		idx, err = meta.CommitInfo.LogIndex.Marshal()
		if err != nil {
			return err
		}
	}
	if err = binary.Write(&buf, binary.BigEndian, int32(len(idx))); err != nil {
		return err
	}
	if err = binary.Write(&buf, binary.BigEndian, idx); err != nil {
		return err
	}
	var colBufs [][]byte
	for idx := 0; idx < colCnt; idx++ {
		colBuf, err := data[idx].Marshal()
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
