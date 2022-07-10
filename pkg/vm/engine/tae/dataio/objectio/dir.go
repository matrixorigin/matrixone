package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"io/fs"
)

type ObjectDir struct {
	common.RefHelper
	nodes map[string]*ObjectFile
	inode *Inode
	fs    *ObjectFS
	stat  *objectFileStat
}

func (d ObjectDir) Stat() (fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (d ObjectDir) Read(bytes []byte) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (d ObjectDir) Close() error {
	//TODO implement me
	panic("implement me")
}

func (d ObjectDir) Write(p []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (d ObjectDir) Sync() error {
	//TODO implement me
	panic("implement me")
}

func openObjectDir(fs *ObjectFS, name string) *ObjectDir {
	inode := &Inode{
		magic: MAGIC,
		inode: fs.lastInode,
		typ:   DIR,
		name:  name,
	}
	file := &ObjectDir{
		fs:    fs,
		inode: inode,
		nodes: make(map[string]*ObjectFile),
	}
	fs.lastInode++
	return file
}

func (d *ObjectDir) OpenFile(fs *ObjectFS, name string) *ObjectFile {
	file := d.nodes[name]
	if file == nil {
		file = openObjectFile(fs, name)
		d.nodes[name] = file
	}
	return file
}
