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
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/tfs"
	"github.com/pierrec/lz4"
	"io/ioutil"
	"os"
	gsort "sort"
	"strings"
	"sync"
)

type ObjectFS struct {
	sync.RWMutex
	common.RefHelper
	nodes     map[string]tfs.File
	data      []*Object
	driver    *MetaDriver
	attr      *Attr
	lastId    uint64
	lastInode uint64
	seq       uint64
}

type Attr struct {
	algo uint8
	dir  string
}

func NewObjectFS() tfs.FS {
	fs := &ObjectFS{
		attr: &Attr{
			algo: compress.Lz4,
		},
		nodes: make(map[string]tfs.File),
	}
	fs.driver = newMetaDriver(fs)
	fs.lastId = 1
	fs.lastInode = 1
	return fs
}

func (o *ObjectFS) SetDir(dir string) {
	o.attr.dir = dir
}

func (o *ObjectFS) OpenDir(name string, nodes *map[string]tfs.File) (*map[string]tfs.File, tfs.File, error) {
	dir := (*nodes)[name]
	if dir == nil {
		o.seq++
		dir = openObjectDir(o, name)
		(*nodes)[name] = dir
	}
	return &dir.(*ObjectDir).nodes, dir, nil
}

func (o *ObjectFS) OpenFile(name string, flag int) (file tfs.File, err error) {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	fileName := strings.Split(name, "/")
	nodes := &o.nodes
	var dir tfs.File
	paths := len(fileName)
	if strings.Contains(name, ".") {
		paths--
	}
	for i := 0; i < paths; i++ {
		parent := fileName[i]
		if i > 0 {
			parent = fileName[i-1]
		}
		nodes, dir, err = o.OpenDir(fileName[i], nodes)
		dir.(*ObjectDir).inode.parent = parent
		if err != nil {
			return nil, err
		}
	}
	if !strings.Contains(name, ".") {
		return dir, nil
	}
	file = dir.(*ObjectDir).OpenFile(o, fileName[paths])
	return file, nil
}

func (o *ObjectFS) ReadDir(dir string) ([]common.FileInfo, error) {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	fileInfos := make([]common.FileInfo, 0)
	entry := o.nodes[dir]
	info := entry.Stat()
	fileInfos = append(fileInfos, info)
	return fileInfos, nil
}

func (o *ObjectFS) Remove(name string) error {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	fileName := strings.Split(name, "/")
	dir := o.nodes[fileName[0]]
	if dir == nil {
		return os.ErrNotExist
	}
	return dir.(*ObjectDir).Remove(fileName[1])
}

func (o *ObjectFS) RemoveAll(dir string) error {
	return nil
}

func (o *ObjectFS) MountInfo() *tfs.MountInfo {
	return nil
}

func (o *ObjectFS) GetData(size uint64) (object *Object, err error) {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	if len(o.data) == 0 ||
		o.data[len(o.data)-1].GetSize()+size >= ObjectSize {
		object, err = OpenObject(o.lastId, DataType, o.attr.dir)
		object.Mount(ObjectSize, PageSize)
		o.data = append(o.data, object)
		o.lastId++
		return
	}
	return o.data[len(o.data)-1], nil
}

func (o *ObjectFS) GetDataWithId(id uint64) *Object {
	o.RWMutex.RLock()
	defer o.RWMutex.RUnlock()
	for _, object := range o.data {
		if object.id == id {
			return object
		}
	}
	return nil
}

func (o *ObjectFS) Append(file *ObjectFile, data []byte) (n int, err error) {
	dataObject, err := o.GetData(uint64(len(data)))
	if err != nil {
		return
	}
	buf := data
	if o.attr.algo == compress.Lz4 {
		buf = make([]byte, lz4.CompressBlockBound(len(buf)))
		if buf, err = compress.Compress(data, buf, compress.Lz4); err != nil {
			return
		}
	}
	offset, allocated := dataObject.allocator.Allocate(uint64(len(buf)))
	_, err = dataObject.Append(buf, int64(offset))
	if err != nil {
		return int(allocated), err
	}
	file.inode.mutex.Lock()
	file.inode.extents = append(file.inode.extents, Extent{
		typ:    APPEND,
		offset: uint32(offset),
		length: uint32(allocated),
		data:   entry{offset: 0, length: uint32(len(buf))},
	})
	file.inode.size += uint64(len(buf))
	file.inode.dataSize += uint64(len(data))
	file.inode.objectId = dataObject.id
	file.inode.mutex.Unlock()
	err = o.driver.Append(file)
	return int(allocated), err
}

func (o *ObjectFS) Read(file *ObjectFile, data []byte) (n int, err error) {
	bufLen := len(data)
	if bufLen == 0 {
		return 0, nil
	}
	inode := file.GetInode()
	dataObject := o.GetDataWithId(inode.objectId)
	return dataObject.oFile.ReadAt(data, int64(inode.extents[0].offset))
}

func (o *ObjectFS) Sync(file *ObjectFile) error {
	data := o.GetDataWithId(file.inode.objectId)
	if data == nil {
		return nil
	}
	return data.oFile.Sync()
}

func (o *ObjectFS) GetLastId() {

}

func (o *ObjectFS) Delete(file tfs.File) error {
	return nil
}

func (o *ObjectFS) RebuildObject() error {
	files, err := ioutil.ReadDir(o.attr.dir)
	if err != nil {
		return err
	}
	for _, file := range files {
		id, oType, err := decodeName(file.Name())
		if err != nil {
			return err
		}
		if id > o.lastId {
			o.lastId = id
		}
		if oType == DataType {
			object, err := OpenObject(id, DataType, o.attr.dir)
			if err != nil {
				return err
			}
			object.Mount(ObjectSize, PageSize)
			object.allocator.available = p2roundup(uint64(file.Size()), PageSize)
			o.data = append(o.data, object)
		} else if oType == NodeType {
			object, err := OpenObject(id, NodeType, o.attr.dir)
			if err != nil {
				return err
			}
			object.Mount(ObjectSize, MetaSize)
			object.allocator.available = p2roundup(uint64(file.Size()), MetaSize)
			o.driver.inode = append(o.driver.inode, object)
		} else if oType == MetadataBlkType {
			object, err := OpenObject(id, MetadataBlkType, o.attr.dir)
			if err != nil {
				return err
			}
			object.Mount(ObjectSize, MetaSize)
			object.allocator.available = p2roundup(uint64(file.Size()), MetaSize)
			o.driver.blk = append(o.driver.blk, object)
		}
	}
	Sort(o.data)
	Sort(o.driver.inode)
	Sort(o.driver.blk)
	return nil
}

type ObjectList []*Object

func (s ObjectList) Len() int           { return len(s) }
func (s ObjectList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ObjectList) Less(i, j int) bool { return s[i].id < s[j].id }

func Sort(data []*Object) {
	gsort.Sort(ObjectList(data))
}
