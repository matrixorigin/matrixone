package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/tfs"
	"io/fs"
	"os"
)

type ObjectDir struct {
	common.RefHelper
	nodes map[string]tfs.File
	inode *Inode
	fs    *ObjectFS
	stat  *objectFileStat
}

func openObjectDir(fs *ObjectFS, name string) *ObjectDir {
	inode := &Inode{
		magic: MAGIC,
		inode: fs.lastInode,
		typ:   DIR,
		name:  name,
	}
	file := &ObjectDir{}
	file.fs = fs
	file.inode = inode
	file.nodes = make(map[string]tfs.File)
	fs.lastInode++
	return file
}

func (d *ObjectDir) Stat() (fs.FileInfo, error) {
	d.inode.mutex.RLock()
	defer d.inode.mutex.RUnlock()
	stat := &objectFileStat{}
	stat.size = int64(len(d.nodes))
	stat.dataSize = int64(len(d.nodes))
	stat.oType = d.inode.typ
	return stat, nil
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
		file = openObjectFile(fs, name)
		d.nodes[name] = file
	}
	return file
}
