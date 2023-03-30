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

package store

import (
	"fmt"
	"io"
	"unsafe"
)

type Index struct {
	LSN  uint64
	CSN  uint32
	Size uint32
}

const (
	IndexSize int64 = int64(unsafe.Sizeof(Index{}))
)

func EncodeIndex(idx *Index) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(idx)), IndexSize)
}

func NewIndex(lsn uint64, csn, size uint32) *Index {
	return &Index{
		LSN:  lsn,
		CSN:  csn,
		Size: size,
	}
}

func (index *Index) Compare(o *Index) int {
	if index.LSN > o.LSN {
		return 1
	} else if index.LSN < o.LSN {
		return -1
	}
	if index.CSN > o.CSN {
		return 1
	} else if index.CSN < o.CSN {
		return -1
	}
	return 0
}

func (index *Index) WriteTo(w io.Writer) (n int64, err error) {
	if _, err = w.Write(EncodeIndex(index)); err != nil {
		return
	}
	n = IndexSize
	return
}

func (index *Index) ReadFrom(r io.Reader) (n int64, err error) {
	if _, err = r.Read(EncodeIndex(index)); err != nil {
		return
	}
	n = IndexSize
	return
}

func (index *Index) Clone() *Index {
	if index == nil {
		return nil
	}
	return &Index{
		LSN:  index.LSN,
		CSN:  index.CSN,
		Size: index.Size,
	}
}
func (index *Index) String() string {
	if index == nil {
		return "<nil index>"
	}
	return fmt.Sprintf("<Index[%d:%d/%d]>", index.LSN, index.CSN, index.Size)
}
