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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"sync"
)

type ObjectFS struct {
	sync.RWMutex
	Service fileservice.FileService
	Dir     string
	Writer  map[string]Writer
}

type Attr struct {
	algo uint8
	dir  string
}

func NewObjectFS(service fileservice.FileService) *ObjectFS {
	fs := &ObjectFS{
		Service: service,
		Writer:  make(map[string]Writer),
	}
	return fs
}

func (o *ObjectFS) SetDir(dir string) {
	if o.Dir != "" {
		return
	}
	o.Dir = dir
	c := fileservice.Config{
		Name:    "LOCAL",
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c)
	if err != nil {
		panic(any(fmt.Sprintf("NewFileService failed: %s", err.Error())))
	}
	o.Service = service
}

func (o *ObjectFS) GetWriter(name string) (Writer, error) {
	o.Lock()
	defer o.Unlock()
	writer := o.Writer[name]
	if writer != nil {
		return writer, nil
	}
	writer, err := NewObjectWriter(name, o.Service)
	if err != nil {
		return nil, err
	}
	o.Writer[name] = writer
	return writer, err
}
