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

package dataio

import (
	"errors"
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type fileStat struct {
	size  int64
	osize int64
	name  string
	algo  uint8
}

func (info *fileStat) Size() int64 {
	return info.size
}

func (info *fileStat) OriginSize() int64 {
	return info.osize
}

func (info *fileStat) Name() string {
	return info.name
}

func (info *fileStat) CompressAlgo() int {
	return int(info.algo)
}

type colPartFileStat struct {
	fileStat
	id *common.ID
}

func (info *colPartFileStat) Name() string {
	return info.id.ToPartFilePath()
}

func CopyFile(src, dest string) (int64, error) {
	stat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}
	if !stat.Mode().IsRegular() {
		return 0, errors.New("not a regular file")
	}
	r, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	return CopyFromReader(r, dest)
}

func CopyFromReader(r io.Reader, dest string) (int64, error) {
	w, err := os.Create(dest)
	if err != nil {
		return 0, err
	}
	defer w.Close()
	return io.Copy(w, r)
}
