// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
