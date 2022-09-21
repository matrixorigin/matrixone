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

package blockid

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"sync"
)

type ObjectFS struct {
	sync.RWMutex
	common.RefHelper
	service fileservice.FileService
	dir     string
	writers map[string]objectio.Writer
}

type Attr struct {
	algo uint8
	dir  string
}

func NewObjectFS(service fileservice.FileService) *ObjectFS {
	fs := &ObjectFS{
		service: service,
		writers: make(map[string]objectio.Writer),
	}
	return fs
}

func (o *ObjectFS) SetDir(dir string) {
	if o.dir != "" {
		return
	}
	o.dir = dir
	c := fileservice.Config{
		Name:    "LOCAL",
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c)
	if err != nil {
		panic(any(fmt.Sprintf("NewFileService failed: %s", err.Error())))
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
