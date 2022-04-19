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
	"io"
	"os"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/iterator/iface"

	"github.com/matrixorigin/matrixone/pkg/compress"
	// "github.com/matrixorigin/matrixone/pkg/container/batch"
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
	blkCountSize = 8
	blkIdxSize   = 48
	blkRangeSize = 24
	colSizeSize  = 8
	colPosSize   = 8
)

const Version uint64 = 1

type FileDestoryer = func(string) error

//  BlkCnt | Blk0 Pos | Blk1 Pos | ... | BlkEndPos | Blk0 Data | ...
// SegmentWriter writes block data into the created segment file
// when flushSegEvent(.blk count == SegmentMaxBlocks) is triggered.
type SegmentWriter struct {
	// data is the data of the block file,
	// SegmentWriter does not read from the block file
	data       iface.BlockIterator
	meta       *metadata.Segment
	dir        string
	size       int64
	fileHandle *os.File
	destroyer  FileDestoryer
	//preprocessor func([]*batch.Batch, *metadata.Segment) error

	// fileGetter is createFile()，use dir&TableID&SegmentID to
	// create a tmp file for flusher to flush data
	fileGetter func(string, *metadata.Segment) (*os.File, error)

	// fileCommiter is commitFile()，rename file name after
	// flusher is completed
	fileCommiter func(string) (string, error)
	//indexFlusher func(*os.File, []*batch.Batch, *metadata.Segment) error
	flusher      func(*os.File, iface.BlockIterator, *metadata.Segment) error
	preExecutor  func()
	postExecutor func()
}

// NewSegmentWriter make a SegmentWriter, which is
// used when (block file count) == SegmentMaxBlocks
func NewSegmentWriter(data iface.BlockIterator, meta *metadata.Segment, dir string, post func()) *SegmentWriter {
	w := &SegmentWriter{
		data: data,
		meta: meta,
		dir:  dir,
	}
	// w.preprocessor = w.defaultPreprocessor
	w.fileGetter, w.fileCommiter = w.createFile, w.commitFile
	w.flusher = flush
	w.SetPostExecutor(post)
	//w.indexFlusher = w.flushIndices
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

func (sw *SegmentWriter) GetDestoryer() FileDestoryer {
	return sw.destroyer
}

//func (sw *SegmentWriter) SetIndexFlusher(f func(*os.File, []*batch.Batch, *metadata.Segment) error) {
//	sw.indexFlusher = f
//}

func (sw *SegmentWriter) SetFlusher(f func(*os.File, iface.BlockIterator, *metadata.Segment) error) {
	sw.flusher = f
}

//func (sw *SegmentWriter) defaultPreprocessor(data []*batch.Batch, meta *metadata.Segment) error {
//	err := mergesort.MergeBlocksToSegment(data, meta.Table.Schema.PrimaryKey)
//	return err
//}

func (sw *SegmentWriter) commitFile(fname string) (string, error) {
	name, err := common.FilenameFromTmpfile(fname)
	if err != nil {
		return name, err
	}
	err = os.Rename(fname, name)
	return name, err
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

// Unused
// flushIndices flush embedded index of segment.
// func (sw *SegmentWriter) flushIndices(w *os.File, data []*batch.Batch, meta *metadata.Segment) error {
// 	indices := make([]index.Index, len(meta.Table.Schema.ColDefs))

// 	// ZoneMapIndex
// 	for idx, colDef := range meta.Table.Schema.ColDefs {
// 		columns := make([]*vector.Vector, 0)
// 		typ := colDef.Type
// 		isPrimary := idx == meta.Table.Schema.PrimaryKey
// 		for i := 0; i < len(data); i++ {
// 			columns = append(columns, data[i].Vecs[idx])
// 		}
// 		zmi, err := index.BuildSegmentZoneMapIndex(columns, typ, int16(idx), isPrimary)
// 		if err != nil {
// 			return err
// 		}
// 		indices[idx] = zmi
// 	}

// 	// other embedded indices if needed

// 	buf, err := index.DefaultRWHelper.WriteIndices(indices)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = w.Write(buf)
// 	return err
// }

// Execute steps as follows:
// 1. Create a temp segment file.
// 3. Flush indices.
// 4. Compress column data and flush them.
// 5. Rename .tmp file to .seg file.
func (sw *SegmentWriter) Execute() error {
	w, err := sw.fileGetter(sw.dir, sw.meta)
	if err != nil {
		return err
	}
	sw.fileHandle = w
	if sw.preExecutor != nil {
		sw.preExecutor()
	}
	if err = sw.flusher(w, sw.data, sw.meta); err != nil {
		w.Close()
		return err
	}
	footer := make([]byte, 64)
	w.Seek(0, io.SeekEnd)
	if _, err = w.Write(footer); err != nil {
		return err
	}
	if sw.postExecutor != nil {
		sw.postExecutor()
	}
	filename, _ := filepath.Abs(w.Name())
	w.Close()
	stat, _ := os.Stat(filename)
	sw.size = stat.Size()
	name, err := sw.fileCommiter(filename)
	sw.destroyer = func(reason string) error {
		logutil.Infof("SegmentFile | \"%s\" | Removed | Reason: \"%s\"", name, reason)
		return os.Remove(name)
	}
	return err
}

func (sw *SegmentWriter) GetSize() int64 {
	return sw.size
}

// flush metadata, columns data, indices, and other related infos
// for the segment.
func flush(w *os.File, iter iface.BlockIterator, meta *metadata.Segment) error {
	var metaBuf bytes.Buffer
	blkCnt := iter.BlockCount()
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
	err = binary.Write(&metaBuf, binary.BigEndian, blkCnt)
	if err != nil {
		return err
	}
	colDefs := meta.Table.Schema.ColDefs
	colCnt := len(colDefs)
	if err = binary.Write(&metaBuf, binary.BigEndian, uint32(colCnt)); err != nil {
		return err
	}
	for _, blk := range meta.BlockSet {
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
	metaSize := headerSize +
		reservedSize +
		algoSize +
		blkCntSize +
		colCntSize +
		startPosSize +
		endPosSize +
		int(blkCnt)*(blkCountSize+2*blkIdxSize+blkRangeSize) +
		int(blkCnt)*colCnt*(colSizeSize*2) +
		colCnt*colPosSize

	if _, err = w.Seek(int64(metaSize), io.SeekStart); err != nil {
		return err
	}

	colSizes := make([]int, colCnt)
	sortedIdx := make([]uint16, 0)
	pkIdx := meta.Table.Schema.PrimaryKey
	var outputBuffer bytes.Buffer
	var indices []index.Index
	typs := make([]types.Type, 0)
	for _, def := range meta.Table.Schema.ColDefs {
		typs = append(typs, def.Type)
	}

	// get the shuffle info of the column
	iter.Reset(uint16(pkIdx))
	pkColumn, err := iter.FetchColumn()
	if err != nil {
		return err
	}
	if err = preprocessColumn(pkColumn, &sortedIdx, true); err != nil {
		return err
	}
	// could safely release vectors' mem nodes here
	iter.Reset(0)

	for i := 0; i < colCnt; i++ {
		if i == pkIdx {
			// build zone map
			zmi, err := index.BuildSegmentZoneMapIndex(pkColumn, typs[i], int16(i), true)
			if err != nil {
				return err
			}
			indices = append(indices, zmi)

			colSz, err := processColumn(pkColumn, &metaBuf, &outputBuffer)
			if err != nil {
				return err
			}
			colSizes[i] = colSz
			if _, err := w.Write(outputBuffer.Bytes()); err != nil {
				return err
			}
			iter.Reset(uint16(i + 1))
			outputBuffer.Reset()
			pkColumn = nil
			continue
		}
		column, err := iter.FetchColumn()
		if err != nil {
			return err
		}
		if err = preprocessColumn(column, &sortedIdx, false); err != nil {
			return err
		}
		zmi, err := index.BuildSegmentZoneMapIndex(column, typs[i], int16(i), false)
		if err != nil {
			return err
		}
		indices = append(indices, zmi)
		colSz, err := processColumn(column, &metaBuf, &outputBuffer)
		if err != nil {
			return err
		}
		colSizes[i] = colSz
		if _, err := w.Write(outputBuffer.Bytes()); err != nil {
			return err
		}
		iter.Reset(uint16(i + 1))
		outputBuffer.Reset()
	}

	// flush embedded indices
	buf, err := index.DefaultRWHelper.WriteIndices(indices)
	if err != nil {
		return err
	}
	if _, err = w.Write(buf); err != nil {
		return err
	}

	// back to start, flush metadata
	if _, err = w.Seek(0, io.SeekStart); err != nil {
		return err
	}
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

	//if _, err = w.Write(dataBuf.Bytes()); err != nil {
	//	return err
	//
	//}

	return nil
}

func preprocessColumn(column []*vector.Vector, sortedIdx *[]uint16, isPrimary bool) error {
	if isPrimary {
		*sortedIdx = make([]uint16, vector.Length(column[0])*len(column))
		if err := mergesort.MergeSortedColumn(column, sortedIdx); err != nil {
			return err
		}
	} else {
		if err := mergesort.ShuffleColumn(column, *sortedIdx); err != nil {
			return err
		}
	}
	return nil
}

func processColumn(column []*vector.Vector, metaBuf, dataBuf *bytes.Buffer) (int, error) {
	colSz := 0
	for _, vec := range column {
		colBuf, err := vec.Show()
		if err != nil {
			return 0, err
		}
		colSize := len(colBuf)
		cbuf := make([]byte, lz4.CompressBlockBound(colSize))
		if cbuf, err = compress.Compress(colBuf, cbuf, compress.Lz4); err != nil {
			return 0, err
		}
		if err = binary.Write(metaBuf, binary.BigEndian, uint64(len(cbuf))); err != nil {
			return 0, err
		}
		if err = binary.Write(metaBuf, binary.BigEndian, uint64(colSize)); err != nil {
			return 0, err
		}
		if err = binary.Write(dataBuf, binary.BigEndian, cbuf); err != nil {
			return 0, err
		}
		colSz += len(cbuf)
	}
	return colSz, nil
}
