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
	"os"
	"strings"
	"sync"
)

type ObjectFS struct {
	sync.RWMutex
	common.RefHelper
	dirs      map[string]tfs.File
	data      []*Object
	meta      []*Object
	attr      *Attr
	lastId    uint64
	lastInode uint64
}

type Attr struct {
	algo uint8
	dir  string
}

func NewObjectFS() tfs.FS {
	fs := &ObjectFS{
		attr: &Attr{
			algo: compress.None,
		},
		dirs: make(map[string]tfs.File),
	}
	fs.lastId = 1
	fs.lastInode = 1
	return fs
}

func (o *ObjectFS) SetDir(dir string) {
	o.attr.dir = dir
}

func (o *ObjectFS) OpenFile(name string, flag int) (tfs.File, error) {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	fileName := strings.Split(name, "/")
	dir := o.dirs[fileName[0]]
	if dir == nil {
		dir = openObjectDir(o, fileName[0])
		o.dirs[fileName[0]] = dir
	}
	if len(fileName) == 1 {
		return dir, nil
	}
	file := dir.(*ObjectDir).OpenFile(o, fileName[1])
	return file, nil
}

func (o *ObjectFS) ReadDir(dir string) ([]common.FileInfo, error) {
	fileInfos := make([]common.FileInfo, 0)
	entry := o.dirs[dir]
	info := entry.Stat()
	fileInfos = append(fileInfos, info)
	return fileInfos, nil
}

func (o *ObjectFS) Remove(name string) error {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	fileName := strings.Split(name, "/")
	dir := o.dirs[fileName[0]]
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
		o.data[len(o.data)-1].GetSize()+size >= OBJECT_SIZE {
		object, err = OpenObject(o.lastId, DATATYPE, o.attr.dir)
		o.data = append(o.data, object)
		o.lastId++
		return
	}
	return o.data[len(o.data)-1], nil
}

func (o *ObjectFS) GetMeta(size uint64) (object *Object, err error) {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	if len(o.meta) == 0 ||
		o.meta[len(o.meta)-1].GetSize()+size >= OBJECT_SIZE {
		object, err = OpenObject(o.lastId, METADATA, o.attr.dir)
		o.meta = append(o.meta, object)
		o.lastId++
		return
	}
	return o.meta[len(o.meta)-1], nil
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
	offset, allocated, err := dataObject.Append(data)
	if err != nil {
		return int(allocated), err
	}
	file.inode.mutex.Lock()
	file.inode.extents = append(file.inode.extents, Extent{
		typ:    APPEND,
		offset: uint32(offset),
		length: uint32(len(data)),
		data:   entry{offset: 0, length: uint32(len(data))},
	})
	file.inode.size += uint64(len(data))
	file.inode.dataSize += uint64(len(data))
	file.inode.seq++
	file.inode.objectId = dataObject.id
	file.inode.mutex.Unlock()
	inode, err := file.inode.Marshal()
	if err != nil {
		return int(allocated), err
	}
	metaObject, err := o.GetMeta(uint64(len(inode)))
	if err != nil {
		return
	}
	_, _, err = metaObject.Append(inode)
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

func (o *ObjectFS) Delete(file tfs.File) error {
	return nil
}
