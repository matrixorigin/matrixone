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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/tfs"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

const UPGRADE_FILE_NUM = 2

type dataFile struct {
	mutex  sync.RWMutex
	colBlk *columnBlock
	file   []tfs.File
	buf    []byte
	stat   *fileStat
	cache  []byte
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
		file:   make([]tfs.File, 0),
	}
	df.stat = &fileStat{}
	return df
}

func newIndex(colBlk *columnBlock) *indexFile {
	index := &indexFile{
		dataFile: newData(colBlk),
	}
	return index
}

func newUpdates(colBlk *columnBlock) *updatesFile {
	update := &updatesFile{
		dataFile: newData(colBlk),
	}
	return update
}

func newDeletes(block *blockFile) *deletesFile {
	//col := &columnBlock{block: block, blockType: DELETE}
	del := &deletesFile{
		block:    block,
		dataFile: newData(nil),
	}
	return del
}

func (df *dataFile) Write(buf []byte) (n int, err error) {
	file := df.GetFile()
	_, err = file.Write(buf)
	n = len(buf)
	return
}

func (df *dataFile) Read(buf []byte) (n int, err error) {
	bufLen := len(buf)
	if bufLen == 0 {
		return 0, nil
	}
	file := df.GetFile()
	n, err = file.Read(buf)
	return n, nil
}

func (df *dataFile) GetFileCnt() int {
	df.mutex.Lock()
	defer df.mutex.Unlock()
	return len(df.file)
}

func (df *dataFile) GetFileType() common.FileType {
	return common.DiskFile
}

func (df *dataFile) GetFile() tfs.File {
	df.mutex.RLock()
	defer df.mutex.RUnlock()
	return df.file[len(df.file)-1]
}

func (df *dataFile) SetFile(file tfs.File, col, idx uint32) {
	df.mutex.Lock()
	defer df.mutex.Unlock()
	df.file = append(df.file, file)
}

func (df *dataFile) Ref()            { df.colBlk.Ref() }
func (df *dataFile) Unref()          { df.colBlk.Unref() }
func (df *dataFile) RefCount() int64 { return df.colBlk.RefCount() }
func (df *dataFile) Destroy() {
	df.mutex.Lock()
	defer df.mutex.Unlock()
	for _, file := range df.file {
		if file == nil {
			continue
		}
		file.Close()
	}
	df.file = nil
}

func (df *dataFile) Stat() common.FileInfo { return df.stat }

func (df *deletesFile) Ref()   { df.block.Ref() }
func (df *deletesFile) Unref() { df.block.Unref() }
