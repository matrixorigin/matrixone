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

package blockio

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"os"
	"path"
	"sync"
)

type blockFile struct {
	common.RefHelper
	sync.RWMutex
	name     string
	seg      *segmentFile
	id       *common.ID
	metaKey  objectio.Extent
	deltaKey objectio.Extent
	columns  []*columnBlock
	writer   *Writer
	reader   *Reader
}

func newBlock(id uint64, seg *segmentFile, colCnt int) *blockFile {
	blockID := &common.ID{
		TableID:   seg.id.TableID,
		SegmentID: seg.id.SegmentID,
		BlockID:   id,
	}
	name := EncodeBlkName(blockID)
	bf := &blockFile{
		seg:     seg,
		id:      blockID,
		columns: make([]*columnBlock, colCnt),
		name:    name,
	}
	bf.OnZeroCB = bf.close
	for i := range bf.columns {
		bf.columns[i] = newColumnBlock(bf, i)
	}
	bf.Ref()
	return bf
}

func (bf *blockFile) WriteBatch(bat *containers.Batch, ts types.TS) (blk objectio.BlockObject, err error) {
	if bf.writer == nil {
		bf.writer = NewWriter(bf.seg.fs, bf.name)
	}
	block, err := bf.writer.WriteBlock(bat)
	return block, err
}

func (bf *blockFile) GetWriter() objectio.Writer {
	return bf.writer.writer
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

func (bf *blockFile) ReadRows(metaLoc string) uint32 {
	_, _, rows := DecodeMetaLoc(metaLoc)
	return rows
}

func (bf *blockFile) setMetaKey(extent objectio.Extent) {
	bf.Lock()
	defer bf.Unlock()
	bf.metaKey = extent
}

func (bf *blockFile) setDeltaKey(extent objectio.Extent) {
	bf.Lock()
	defer bf.Unlock()
	bf.deltaKey = extent
}

func (bf *blockFile) getMetaKey() objectio.Extent {
	bf.RLock()
	defer bf.RUnlock()
	return bf.metaKey
}

func (bf *blockFile) getDeltaKey() objectio.Extent {
	bf.RLock()
	defer bf.RUnlock()
	return bf.deltaKey
}

func (bf *blockFile) GetMeta() objectio.BlockObject {
	metaKey := bf.getMetaKey()
	if metaKey.End() == 0 {
		panic(any("block meta key err"))
	}
	if bf.reader == nil {
		bf.reader = NewReader(bf.seg.fs, bf, bf.name)
	}
	block, err := bf.reader.ReadMeta(metaKey)
	if err != nil {
		panic(any(err))
	}
	return block
}

func (bf *blockFile) GetMetaFormKey(metaLoc string) objectio.BlockObject {
	name, extent, _ := DecodeMetaLoc(metaLoc)
	if bf.reader == nil {
		bf.reader = NewReader(bf.seg.fs, bf, name)
	}
	block, err := bf.reader.ReadMeta(extent)
	if err != nil {
		// FIXME: Now the block that is gc will also be replayed, here is a work around
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil
		}
		panic(any(err))
	}
	bf.setMetaKey(block.GetExtent())
	return block
}

func (bf *blockFile) GetDelta() objectio.BlockObject {
	metaKey := bf.getDeltaKey()
	if metaKey.End() == 0 {
		panic(any("block meta key err"))
	}
	if bf.reader == nil {
		bf.reader = NewReader(bf.seg.fs, bf, bf.name)
	}
	block, err := bf.reader.ReadMeta(metaKey)
	if err != nil {
		panic(any(err))
	}
	return block
}

func (bf *blockFile) GetDeltaFormKey(metaLoc string) objectio.BlockObject {
	name, extent, _ := DecodeMetaLoc(metaLoc)
	if bf.reader == nil {
		bf.reader = NewReader(bf.seg.fs, bf, name)
	}
	block, err := bf.reader.ReadMeta(extent)
	if err != nil {
		// FIXME: Now the block that is gc will also be replayed, here is a work around
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil
		}
		panic(any(err))
	}
	bf.setDeltaKey(block.GetExtent())
	return block
}

func (bf *blockFile) WriteDeletes(buf []byte) (err error) {
	return
}

func (bf *blockFile) ReadDeletes(buf []byte) (err error) {
	return
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
	if bf.writer == nil {
		return nil
	}
	name := path.Join(bf.writer.fs.Dir, bf.name)
	err := os.Remove(name)
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

func (bf *blockFile) Sync() error {
	blocks, err := bf.writer.Sync()
	if err != nil {
		return err
	}
	bf.setMetaKey(blocks[0].GetExtent())
	if len(blocks) > 1 {
		bf.setDeltaKey(blocks[1].GetExtent())
	}
	return err
}

func (bf *blockFile) LoadBatch(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	opts *containers.Options) (bat *containers.Batch, err error) {
	if bf.reader == nil {
		bat = containers.NewBatch()

		metaKey := bf.getMetaKey()
		for i := range bf.columns {
			vec := containers.MakeVector(colTypes[i], nullables[i], opts)
			bat.AddVector(colNames[i], vec)
			if metaKey.End() == 0 {
				continue
			}
		}
		return bat, err
	}
	return bf.reader.LoadBlkColumns(colTypes, colNames, nullables, opts)
}

func (bf *blockFile) LoadDeletes() (mask *roaring.Bitmap, err error) {
	return bf.reader.LoadDeletes(bf.id)
}
