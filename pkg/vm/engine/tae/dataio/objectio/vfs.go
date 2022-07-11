package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"os"
)

type VFS struct {
	common.RefHelper
	cb *columnBlock
}

func (v *VFS) Write(p []byte) (n int, err error) {
	return 0, v.cb.WriteData(p)
}

func (v *VFS) Read(p []byte) (n int, err error) {
	err = v.cb.ReadData(p)
	if err != nil {
		return 0, err
	}
	n = len(p)
	return
}

func (v *VFS) RefCount() int64 {
	return 0
}

func (v *VFS) Stat() common.FileInfo {
	name := EncodeColBlkNameWithVersion(v.cb.id, v.cb.ts, nil)
	vfile, err := v.cb.block.seg.fs.OpenFile(name, os.O_RDWR)
	if err != nil {
		return nil
	}
	return vfile.Stat()
}

func (v *VFS) GetFileType() common.FileType {
	return common.DiskFile
}
