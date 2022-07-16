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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/tfs"
	"os"
)

type ObjectDir struct {
	common.RefHelper
	nodes  map[string]tfs.File
	inode  *Inode
	fs     *ObjectFS
	extent Extent
}

func openObjectDir(fs *ObjectFS, name string) *ObjectDir {
	inode := &Inode{
		magic:  MAGIC,
		inode:  fs.lastInode,
		typ:    DIR,
		name:   name,
		create: fs.seq,
	}
	file := &ObjectDir{}
	file.fs = fs
	file.inode = inode
	file.nodes = make(map[string]tfs.File)
	fs.lastInode++
	return file
}

func (d *ObjectDir) Stat() common.FileInfo {
	d.inode.mutex.RLock()
	defer d.inode.mutex.RUnlock()
	stat := &objectFileStat{}
	stat.size = int64(len(d.nodes))
	stat.dataSize = int64(len(d.nodes))
	stat.oType = d.inode.typ
	return stat
}

func (d *ObjectDir) Read(bytes []byte) (int, error) {
	return 0, nil
}

func (d *ObjectDir) Close() error {
	return nil
}

func (d *ObjectDir) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (d *ObjectDir) Sync() error {
	return nil
}

func (d *ObjectDir) Delete(name string) {
	d.inode.mutex.Lock()
	defer d.inode.mutex.Unlock()
	delete(d.nodes, name)
}

func (d *ObjectDir) Remove(name string) error {
	file := d.nodes[name]
	if file != nil {
		return os.ErrNotExist
	}
	err := d.fs.Delete(file)
	if err != nil {
		return err
	}
	d.Delete(name)
	return nil
}

func (d *ObjectDir) OpenFile(fs *ObjectFS, name string) tfs.File {
	file := d.nodes[name]
	if file == nil {
		fs.seq++
		file = openObjectFile(fs, d, name)
		d.nodes[name] = file
	}
	return file
}

func (d *ObjectDir) GetFileType() common.FileType {
	return common.DiskFile
}

func (d *ObjectDir) Marshal() (buf []byte, err error) {
	d.inode.mutex.RLock()
	d.inode.extents = make([]Extent, 0)
	for _, node := range d.nodes {
		d.inode.extents = append(d.inode.extents, node.(*ObjectFile).extent)
	}
	d.inode.mutex.RUnlock()
	return d.inode.Marshal()
}
