package tfs

import (
	"io"
	"io/fs"
)

type File interface {
	fs.File
	io.Writer
	Sync() error
}

type MountInfo struct {
	Dir  string
	Misc []byte
}

type MkFsFuncT = func(name string) (FS, error)
type MountFsFuncT = func(name string) (FS, error)

type FS interface {
	OpenFile(name string, flag int) (File, error)
	ReadDir(dir string) ([]fs.FileInfo, error)
	Remove(name string) error
	RemoveAll(dir string) error
	MountInfo() *MountInfo
}
