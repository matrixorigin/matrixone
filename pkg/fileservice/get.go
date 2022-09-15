// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"path/filepath"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func Get[T any](fs FileService, name string) (res T, err error) {
	lowerName := strings.ToLower(name)
	if fs, ok := fs.(*FileServices); ok {
		f, ok := fs.mappings[lowerName]
		if !ok {
			err = moerr.NewNoService(name)
			return
		}
		res, ok = f.(T)
		if !ok {
			err = moerr.NewNoService(name)
			return
		}
		return
	}
	var ok bool
	res, ok = fs.(T)
	if !ok {
		err = moerr.NewNoService(name)
		return
	}
	if !strings.EqualFold(fs.Name(), lowerName) {
		err = moerr.NewNoService(name)
		return
	}
	return
}

func GetForETL(fs FileService, path string) (res ETLFileService, readPath string, err error) {
	fsPath, err := ParsePath(path)
	if err != nil {
		return nil, "", err
	}
	if fsPath.Service == "" {
		// no service, create local ETL fs
		dir, file := filepath.Split(path)
		res, err = NewLocalETLFS("etl", dir)
		if err != nil {
			return nil, "", err
		}
		readPath = file
	} else {
		// get etl fs
		res, err = Get[ETLFileService](fs, fsPath.Service)
		if err != nil {
			return nil, "", err
		}
		readPath = fsPath.Full
	}
	return
}
