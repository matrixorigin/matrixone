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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/tfs"
	"os"
	"strings"
	"sync"
)

type ObjectFS struct {
	sync.RWMutex
	common.RefHelper
	service   fileservice.FileService
	nodes     map[string]tfs.File
	attr      *Attr
	lastId    uint64
	lastInode uint64
	seq       uint64
}

type Attr struct {
	algo uint8
	dir  string
}

func NewObjectFS(service fileservice.FileService) *ObjectFS {
	fs := &ObjectFS{
		attr: &Attr{
			algo: compress.Lz4,
		},
		nodes:   make(map[string]tfs.File),
		service: service,
	}
	return fs
}

func (o *ObjectFS) SetDir(dir string) {
	o.attr.dir = dir
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
	//return dir.(*ObjectDir).Remove(fileName[1])
	return nil
}

func (o *ObjectFS) RemoveAll(dir string) error {
	return nil
}

func (o *ObjectFS) MountInfo() *tfs.MountInfo {
	return nil
}
