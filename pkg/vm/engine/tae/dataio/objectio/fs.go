package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/tfs"
	"io/fs"
	"sync"
)

type ObjectFS struct {
	sync.RWMutex
	common.RefHelper
	dirs map[string]*ObjectDir
	data []*Object
	meta []*Object
}

func newObjectFS() tfs.FS {
	fs := &ObjectFS{}
	return fs
}

func (o *ObjectFS) OpenFile(name string, flag int) (tfs.File, error) {
	//TODO implement me
	panic("implement me")
}

func (o *ObjectFS) ReadDir(dir string) ([]fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (o *ObjectFS) Remove(name string) error {
	//TODO implement me
	panic("implement me")
}

func (o *ObjectFS) RemoveAll(dir string) error {
	//TODO implement me
	panic("implement me")
}

func (o *ObjectFS) MountInfo() *tfs.MountInfo {
	//TODO implement me
	panic("implement me")
}

func (o *ObjectFS) GetData() *Object {
	o.RWMutex.RLock()
	defer o.RWMutex.RUnlock()
	return o.data[len(o.data)-1]
}

func (o *ObjectFS) GetMeta() *Object {
	o.RWMutex.RLock()
	defer o.RWMutex.RUnlock()
	return o.meta[len(o.meta)-1]
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
	dataObject := o.GetData()
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
	file.inode.originSize += uint64(len(data))
	file.inode.seq++
	file.inode.objectId = dataObject.id
	file.inode.mutex.Unlock()
	inode, err := file.inode.Marshal()
	if err != nil {
		return int(allocated), err
	}
	_, _, err = o.GetMeta().Append(inode)
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
