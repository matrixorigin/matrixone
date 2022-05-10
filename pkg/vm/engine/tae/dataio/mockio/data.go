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

package mockio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type dataFile struct {
	colBlk *columnBlock
	buf    []byte
	stat   *fileStat
}

type indexFile struct {
	*dataFile
}

type updatesFile struct {
	*dataFile
}

type deletesFile struct {
	block *blockFile
	*dataFile
}

func newData(colBlk *columnBlock) *dataFile {
	df := &dataFile{
		colBlk: colBlk,
		buf:    make([]byte, 0),
	}
	df.stat = &fileStat{}
	return df
}

func newIndex(colBlk *columnBlock) *indexFile {
	return &indexFile{
		dataFile: newData(colBlk),
	}
}

func newUpdates(colBlk *columnBlock) *updatesFile {
	return &updatesFile{
		dataFile: newData(colBlk),
	}
}

func newDeletes(block *blockFile) *deletesFile {
	return &deletesFile{
		block:    block,
		dataFile: newData(nil),
	}
}

func (df *dataFile) Write(buf []byte) (n int, err error) {
	n = len(buf)
	df.buf = make([]byte, len(buf))
	copy(df.buf, buf)
	df.stat.size = int64(len(df.buf))
	return
}

func (df *dataFile) Read(buf []byte) (n int, err error) {
	n = len(buf)
	copy(buf, df.buf)
	return
}

func (df *dataFile) GetFileType() common.FileType {
	return common.DiskFile
}

func (df *dataFile) Ref()            { df.colBlk.Ref() }
func (df *dataFile) Unref()          { df.colBlk.Unref() }
func (df *dataFile) RefCount() int64 { return df.colBlk.RefCount() }

func (df *dataFile) Stat() common.FileInfo { return df.stat }

func (df *deletesFile) Ref()   { df.block.Ref() }
func (df *deletesFile) Unref() { df.block.Unref() }
