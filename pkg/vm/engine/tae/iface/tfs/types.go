package tfs

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"io"
)

type File interface {
	common.IVFile
	io.Writer
	Sync() error
	Close() error
}

type MountInfo struct {
	Dir  string
	Misc []byte
}

type MkFsFuncT = func(name string) (FS, error)
type MountFsFuncT = func(name string) (FS, error)

type FS interface {
	OpenFile(name string, flag int) (File, error)
	ReadDir(dir string) ([]common.FileInfo, error)
	Remove(name string) error
	RemoveAll(dir string) error
	MountInfo() *MountInfo
}
