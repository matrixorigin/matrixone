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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type ObjectFS struct {
	Service fileservice.FileService
	Dir     string
}

func TmpNewFileservice(dir string) fileservice.FileService {
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c)
	if err != nil {
		err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("NewFileService failed: %s", err.Error()))
		panic(any(err))
	}
	return service
}

func NewObjectFS(service fileservice.FileService, dir string) *ObjectFS {
	fs := &ObjectFS{
		Service: service,
		Dir:     dir,
	}
	return fs
}

func (o *ObjectFS) ListDir(dir string) ([]fileservice.DirEntry, error) {
	return o.Service.List(context.Background(), dir)
}

func (o *ObjectFS) Delete(fileName string) error {
	return o.Service.Delete(context.Background(), fileName)
}
