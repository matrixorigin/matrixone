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
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
)

// UnsortedSegmentFile is a logical file containing some block(.blk) files
type UnsortedSegmentFile struct {
	sync.RWMutex
	common.RefHelper
	ID      common.ID
	Blocks  map[common.ID]base.IBlockFile
	TBlocks map[common.ID]base.IBaseFile
	Dir     string
	Info    *fileStat
}

func NewUnsortedSegmentFile(dirname string, id common.ID) base.ISegmentFile {
	usf := &UnsortedSegmentFile{
		ID:      id,
		Dir:     dirname,
		Blocks:  make(map[common.ID]base.IBlockFile),
		TBlocks: make(map[common.ID]base.IBaseFile),
		Info: &fileStat{
			name: id.ToSegmentFilePath(),
		},
	}
	usf.OnZeroCB = usf.close
	return usf
}

func (sf *UnsortedSegmentFile) close() {
	sf.Destroy()
}

func (sf *UnsortedSegmentFile) GetFileType() common.FileType {
	return common.DiskFile
}

func (sf *UnsortedSegmentFile) GetDir() string {
	return sf.Dir
}

func (sf *UnsortedSegmentFile) RegisterTBlock(id common.ID) (base.IBlockFile, error) {
	sf.Lock()
	defer sf.Unlock()
	_, ok := sf.TBlocks[id]
	if ok {
		return nil, ErrDupBlk
	}
	bf := NewTBlockFile(sf, id)
	sf.TBlocks[id] = bf
	bf.Ref()
	return bf, nil
}

func (sf *UnsortedSegmentFile) RefBlock(id common.ID) {
	sf.Lock()
	defer sf.Unlock()
	bf, ok := sf.TBlocks[id]
	if ok {
		delete(sf.TBlocks, id)
		bf.Unref()
	}
	_, ok = sf.Blocks[id]
	if !ok {
		bf := NewBlockFile(sf, id, nil)
		sf.AddBlock(id, bf)
	}
	sf.Ref()
}

func (sf *UnsortedSegmentFile) UnrefBlock(id common.ID) {
	sf.Unref()
}

func (sf *UnsortedSegmentFile) GetIndicesMeta() *base.IndicesMeta {
	return nil
}

func (sf *UnsortedSegmentFile) GetBlockIndicesMeta(id common.ID) *base.IndicesMeta {
	blk := sf.GetBlock(id)
	if blk == nil {
		return nil
	}
	return blk.GetIndicesMeta()
}

func (sf *UnsortedSegmentFile) MakeVirtualIndexFile(meta *base.IndexMeta) common.IVFile {
	return nil
}

func (sf *UnsortedSegmentFile) MakeVirtualBlkIndexFile(id common.ID, meta *base.IndexMeta) common.IVFile {
	blk := sf.GetBlock(id)
	if blk == nil {
		return nil
	}
	return blk.MakeVirtualIndexFile(meta)
}

func (sf *UnsortedSegmentFile) MakeVirtualSeparateIndexFile(file *os.File, id *common.ID, meta *base.IndexMeta) common.IVFile {
	return newIndexFile(file, id, meta)
}

func (sf *UnsortedSegmentFile) MakeVirtualPartFile(id *common.ID) common.IVFile {
	return newPartFile(id, sf, false)
}

func (sf *UnsortedSegmentFile) Stat() common.FileInfo {
	return sf.Info
}

func (sf *UnsortedSegmentFile) Close() error {
	return nil
}

func (sf *UnsortedSegmentFile) Destroy() {
	for _, blk := range sf.Blocks {
		blk.Unref()
	}
	for _, blk := range sf.TBlocks {
		blk.Unref()
	}
	sf.Blocks = nil
	sf.TBlocks = nil
}

func (sf *UnsortedSegmentFile) RefTBlock(id common.ID) base.IBlockFile {
	sf.RLock()
	defer sf.RUnlock()
	blk := sf.TBlocks[id]
	if blk != nil {
		blk.Ref()
	}
	return blk
}

func (sf *UnsortedSegmentFile) GetBlock(id common.ID) base.IBlockFile {
	sf.RLock()
	defer sf.RUnlock()
	blk := sf.Blocks[id]
	return blk
}

func (sf *UnsortedSegmentFile) AddBlock(id common.ID, bf base.IBlockFile) {
	_, ok := sf.Blocks[id]
	if ok {
		panic("logic error")
	}
	sf.Blocks[id] = bf
	atomic.AddInt64(&sf.Info.size, bf.Stat().Size())
}

func (sf *UnsortedSegmentFile) ReadPoint(ptr *base.Pointer, buf []byte) {
	panic("not supported")
}

func (sf *UnsortedSegmentFile) GetBlockSize(id common.ID) int64 {
	sf.RLock()
	defer sf.RUnlock()
	blk, ok := sf.Blocks[id.AsBlockID()]
	if !ok {
		panic("logic error")
	}
	return blk.Stat().Size()
}

func (sf *UnsortedSegmentFile) ReadBlockPoint(id common.ID, ptr *base.Pointer, buf []byte) {
	sf.RLock()
	blk, ok := sf.Blocks[id.AsBlockID()]
	if !ok {
		panic("logic error")
	}
	sf.RUnlock()
	blk.ReadPoint(ptr, buf)
}

func (sf *UnsortedSegmentFile) DataCompressAlgo(id common.ID) int {
	sf.RLock()
	blk, ok := sf.Blocks[id.AsBlockID()]
	if !ok {
		panic("logic error")
	}
	sf.RUnlock()
	return blk.DataCompressAlgo(id)
}

func (sf *UnsortedSegmentFile) PartSize(colIdx uint64, id common.ID, isOrigin bool) int64 {
	sf.RLock()
	blk, ok := sf.Blocks[id.AsBlockID()]
	if !ok {
		panic("logic error")
	}
	sf.RUnlock()
	return blk.PartSize(colIdx, id, isOrigin)
}

func (sf *UnsortedSegmentFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	sf.RLock()
	blk, ok := sf.Blocks[id.AsBlockID()]
	if !ok {
		panic("logic error")
	}
	sf.RUnlock()
	blk.ReadPart(colIdx, id, buf)
}

func (sf *UnsortedSegmentFile) PrefetchPart(colIdx uint64, id common.ID) error {
	sf.RLock()
	blk, ok := sf.Blocks[id.AsBlockID()]
	if !ok {
		return errors.New(fmt.Sprintf("column block <blk:%d-col:%d> not found", id.BlockID, colIdx))
	}
	sf.RUnlock()
	return blk.PrefetchPart(colIdx, id)
}

func (sf *UnsortedSegmentFile) snapBlocks() ([]base.IBaseFile, []base.IBaseFile) {
	blks := make([]base.IBaseFile, 0, 4)
	tblks := make([]base.IBaseFile, 0, 2)
	sf.RLock()
	for _, blk := range sf.Blocks {
		blks = append(blks, blk)
	}
	for _, tblk := range sf.TBlocks {
		tblk.Ref()
		tblks = append(tblks, tblk)
	}
	sf.RUnlock()
	return blks, tblks
}

func (sf *UnsortedSegmentFile) CopyTo(dir string) error {
	blks, tblks := sf.snapBlocks()
	defer func() {
		for _, tblk := range tblks {
			tblk.Unref()
		}
	}()
	if len(blks)+len(tblks) == 0 {
		return ErrFileNotExist
	}
	var err error
	for _, blk := range blks {
		if err = blk.CopyTo(dir); err != nil {
			if err == ErrFileNotExist {
				err = nil
			} else {
				return err
			}
		}
	}
	for _, tblk := range tblks {
		if err = tblk.CopyTo(dir); err != nil {
			if err == ErrFileNotExist {
				err = nil
			} else {
				return err
			}
		}
	}
	return err
}

func (sf *UnsortedSegmentFile) LinkTo(dir string) error {
	blks, tblks := sf.snapBlocks()
	defer func() {
		for _, tblk := range tblks {
			tblk.Unref()
		}
	}()
	if len(blks)+len(tblks) == 0 {
		return ErrFileNotExist
	}
	var err error
	for _, blk := range blks {
		if err = blk.LinkTo(dir); err != nil {
			if err == ErrFileNotExist {
				err = nil
			} else {
				return err
			}
		}
	}
	for _, tblk := range tblks {
		if err = tblk.LinkTo(dir); err != nil {
			if err == ErrFileNotExist {
				err = nil
			} else {
				return err
			}
		}
	}
	return err
}
