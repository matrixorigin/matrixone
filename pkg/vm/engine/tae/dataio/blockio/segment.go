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

package blockio

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"path"
	"strings"
)

type ObjectFactory struct {
	Fs *objectio.ObjectFS
}

func NewObjectFactory(dirname string) file.SegmentFactory {
	serviceDir := path.Join(dirname, "data")
	service := objectio.TmpNewFileservice(path.Join(dirname, "data"))
	SegmentFactory := &ObjectFactory{
		Fs: objectio.NewObjectFS(service, serviceDir),
	}
	return SegmentFactory
}

func (factory *ObjectFactory) EncodeName(id uint64) string {
	return ""
}

func (factory *ObjectFactory) DecodeName(name string) (uint64, error) {
	if !strings.HasSuffix(name, ".seg") {
		return 0, moerr.NewInternalError("blockio: segment name is illegal")
	}
	id, err := DecodeSegName(name)
	if err != nil {
		return 0, err
	}
	return id.SegmentID, err
}

func (factory *ObjectFactory) Build(dir string, id, tid uint64, fs *objectio.ObjectFS) {}
