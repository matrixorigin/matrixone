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

package container

import (
	"bytes"
	"io"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
)

type Mask = uint64

const (
	ReadonlyMask Mask = 0x01000000
	HasNullMask  Mask = 0x02000000
	PosMask      Mask = 0x00FFFFFF
)

type VectorType uint8

const (
	StdVec VectorType = iota
	StrVec
	Wrapper
)

type IBatchReader interface {
	IsReadonly() bool
	Length() int
	GetAttrs() []int
	Close() error
	CloseVector(idx int) error
	IsVectorClosed(idx int) (bool, error)
	GetReaderByAttr(attr int) (IVectorReader, error)
}

type IVectorReader interface {
	io.Closer
	GetType() VectorType
	GetValue(int) (interface{}, error)
	IsNull(int) (bool, error)
	HasNull() bool
	NullCnt() int
	Length() int
	Capacity() int
	GetMemorySize() uint64
	SliceReference(start, end int) (IVectorReader, error)
	CopyToVector() (*gvec.Vector, error)
	CopyToVectorWithBuffer(*bytes.Buffer, *bytes.Buffer) (*gvec.Vector, error)
}
