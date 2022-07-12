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
