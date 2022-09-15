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
	"io/fs"
	"time"
)

type objectFileStat struct {
	common.FileInfo
	name     string
	size     int64
	dataSize int64
	algo     uint8
	oType    InodeType
}

func (stat *objectFileStat) Mode() fs.FileMode {
	return 0
}

func (stat *objectFileStat) ModTime() time.Time {
	return time.Time{}
}

func (stat *objectFileStat) IsDir() bool {
	return stat.oType == DIR
}

func (stat *objectFileStat) Sys() any {
	return nil
}

func (stat *objectFileStat) Name() string      { return stat.name }
func (stat *objectFileStat) Size() int64       { return stat.size }
func (stat *objectFileStat) OriginSize() int64 { return stat.dataSize }
func (stat *objectFileStat) CompressAlgo() int { return int(stat.algo) }
