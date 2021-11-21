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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"os"
	"runtime/debug"
)

type EmbedIndexFile struct {
	SegmentFile base.ISegmentFile
	Meta        *base.IndexMeta
	Info        *fileStat
}

type EmbedBlockIndexFile struct {
	EmbedIndexFile
	ID common.ID
}

type IndexFile struct {
	os.File
	common.RefHelper
	ID common.ID
	Meta *base.IndexMeta
	Info *fileStat
}

func newEmbedIndexFile(host base.ISegmentFile, meta *base.IndexMeta) common.IVFile {
	f := &EmbedIndexFile{
		SegmentFile: host,
		Meta:        meta,
		Info: &fileStat{
			size:  int64(meta.Ptr.Len),
			osize: int64(meta.Ptr.Len),
		},
	}
	f.Ref()
	return f
}

func newEmbedBlockIndexFile(id *common.ID, host base.ISegmentFile, meta *base.IndexMeta) common.IVFile {
	f := &EmbedBlockIndexFile{
		EmbedIndexFile: EmbedIndexFile{
			SegmentFile: host,
			Meta:        meta,
			Info: &fileStat{
				size:  int64(meta.Ptr.Len),
				osize: int64(meta.Ptr.Len),
			},
		},
		ID: *id,
	}
	f.Ref()
	return f
}

func newIndexFile(file *os.File, id *common.ID, meta *base.IndexMeta) common.IVFile {
	f := &IndexFile{
		File:      *file,
		ID:        *id,
		Meta:      meta,
		Info:      &fileStat{
			size:  int64(meta.Ptr.Len),
			osize: int64(meta.Ptr.Len),
		},
	}
	f.OnZeroCB = f.close
	f.Ref()
	return f
}

func (f *IndexFile) close() {
	if err := f.Close(); err != nil {
		panic(err)
	}
	logutil.Infof("Destroy index file | %s", f.Name())
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
}

func (f *IndexFile) Unref() {
	debug.PrintStack()
	f.RefHelper.Unref()
}

func (f *IndexFile) Stat() common.FileInfo {
	return f.Info
}

func (f *IndexFile) GetFileType() common.FileType {
	return common.DiskFile
}

func (f *IndexFile) Read(buf []byte) (n int, err error) {
	if len(buf) != int(f.Meta.Ptr.Len) {
		return 0, errors.New("length mismatch reading idx file")
	}
	if _, err := f.ReadAt(buf, f.Meta.Ptr.Offset); err != nil {
		return 0, err
	}
	return len(buf), nil
}

func (f *EmbedIndexFile) Stat() common.FileInfo {
	return f.Info
}

func (f *EmbedIndexFile) Ref() {
	f.SegmentFile.Ref()
}

func (f *EmbedIndexFile) Unref() {
	f.SegmentFile.Ref()
}

func (cpf *EmbedIndexFile) GetFileType() common.FileType {
	return common.DiskFile
}

func (f *EmbedIndexFile) Read(buf []byte) (n int, err error) {
	if len(buf) != int(f.Meta.Ptr.Len) {
		panic("logic error")
	}
	f.SegmentFile.ReadPoint(f.Meta.Ptr, buf)
	return len(buf), nil
}

func (bf *EmbedBlockIndexFile) Stat() common.FileInfo {
	return bf.Info
}
func (bf *EmbedBlockIndexFile) Ref() {
	bf.SegmentFile.RefBlock(bf.ID)
}

func (bf *EmbedBlockIndexFile) Unref() {
	bf.SegmentFile.UnrefBlock(bf.ID)
}

func (bf *EmbedBlockIndexFile) Read(buf []byte) (n int, err error) {
	if len(buf) != int(bf.Meta.Ptr.Len) {
		panic("logic error")
	}
	bf.SegmentFile.ReadBlockPoint(bf.ID, bf.Meta.Ptr, buf)
	return len(buf), nil
}
