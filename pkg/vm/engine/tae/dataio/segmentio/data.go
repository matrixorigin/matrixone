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

package segmentio

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

const UPGRADE_FILE_NUM = 2

type dataFile struct {
	mutex  sync.RWMutex
	colBlk *columnBlock
	file   []*DriverFile
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
		file:   make([]*DriverFile, 0),
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
	if df.colBlk != nil && df.colBlk.block.rows > 0 {
		file.SetRows(df.colBlk.block.rows)
	}
	err = file.GetSegement().Append(file, buf)
	meta := file.GetInode()
	df.stat.algo = meta.GetAlgo()
	df.stat.originSize = meta.GetOriginSize()
	df.stat.size = meta.GetFileSize()
	df.upgradeFile()
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

func (df *dataFile) upgradeFile() {
	go func() {
		df.mutex.Lock()
		if len(df.file) < UPGRADE_FILE_NUM {
			df.mutex.Unlock()
			return
		}
		releaseFile := df.file[:len(df.file)-1]
		df.file = df.file[len(df.file)-1 : len(df.file)]
		df.mutex.Unlock()
		for _, file := range releaseFile {
			file.driver.ReleaseFile(file)
		}
	}()
}

func (df *dataFile) GetFileCnt() int {
	df.mutex.Lock()
	defer df.mutex.Unlock()
	return len(df.file)
}

func (df *dataFile) GetFileType() common.FileType {
	return common.DiskFile
}

func (df *dataFile) GetFile() *DriverFile {
	df.mutex.RLock()
	defer df.mutex.RUnlock()
	return df.file[len(df.file)-1]
}

func (df *dataFile) SetFile(file *DriverFile, col, idx uint32) {
	df.mutex.Lock()
	defer df.mutex.Unlock()
	df.file = append(df.file, file)
	file.SetCols(col)
	file.SetIdxs(idx)
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
		file.Unref()
	}
	df.file = nil
}

func (df *dataFile) Stat() common.FileInfo { return df.stat }

func (df *deletesFile) Ref()   { df.block.Ref() }
func (df *deletesFile) Unref() { df.block.Unref() }
