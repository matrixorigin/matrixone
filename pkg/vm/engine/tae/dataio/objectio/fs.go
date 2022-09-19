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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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
	writers   map[string]objectio.Writer
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
		writers: make(map[string]objectio.Writer),
	}
	return fs
}

func (o *ObjectFS) SetDir(dir string) {
	if o.attr.dir != "" {
		return
	}
	o.attr.dir = dir
	c := fileservice.Config{
		Name:    "LOCAL",
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c)
	if err != nil {
		panic(fmt.Sprintf("NewFileService failed: %s", err.Error()))
	}
	o.service = service
}

func (o *ObjectFS) GetWriter(name string) (objectio.Writer, error) {
	o.Lock()
	defer o.Unlock()
	writer := o.writers[name]
	if writer != nil {
		return writer, nil
	}
	writer, err := objectio.NewObjectWriter(name, o.service)
	if err != nil {
		return nil, err
	}
	o.writers[name] = writer
	return writer, err
}

func (o *ObjectFS) WriterEnd(name string) error {

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
