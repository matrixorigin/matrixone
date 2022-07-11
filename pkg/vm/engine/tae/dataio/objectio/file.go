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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"io/fs"
)

type ObjectFile struct {
	common.RefHelper
	nodes map[string]*ObjectFile
	inode *Inode
	fs    *ObjectFS
	stat  *objectFileStat
}

func openObjectFile(fs *ObjectFS, name string) *ObjectFile {
	inode := &Inode{
		magic: MAGIC,
		inode: fs.lastInode,
		typ:   FILE,
		name:  name,
	}
	file := &ObjectFile{}
	file.fs = fs
	file.inode = inode
	fs.lastInode++
	return file
}

func (b *ObjectFile) GetFS() *ObjectFS {
	return b.fs
}

func (b *ObjectFile) GetInode() *Inode {
	b.inode.mutex.RLock()
	defer b.inode.mutex.RUnlock()
	return b.inode
}

func (b *ObjectFile) SetRows(rows uint32) {
	b.inode.rows = rows
}

func (b *ObjectFile) SetCols(cols uint32) {
	b.inode.cols = cols
}

func (b *ObjectFile) SetIdxs(idxs uint32) {
	b.inode.idxs = idxs
}

func (b *ObjectFile) Stat() (fs.FileInfo, error) {
	b.inode.mutex.RLock()
	defer b.inode.mutex.RUnlock()
	stat := &objectFileStat{}
	stat.size = int64(b.inode.size)
	stat.dataSize = int64(b.inode.size)
	stat.oType = b.inode.typ
	return stat, nil
}

func (b *ObjectFile) GetName() string {
	return b.inode.name
}

func (b *ObjectFile) Write(data []byte) (n int, err error) {
	return b.fs.Append(b, data)
}

func (b *ObjectFile) GetExtents() *[]Extent {
	extents := &b.inode.extents
	return extents
}

func (b *ObjectFile) Read(data []byte) (n int, err error) {
	return b.fs.Read(b, data)
}

func (b *ObjectFile) close() {
	b.Destroy()
}

func (b *ObjectFile) Destroy() {
}

func (b *ObjectFile) Close() error {
	return nil
}

func (b *ObjectFile) Sync() error {
	return b.fs.Sync(b)
}
