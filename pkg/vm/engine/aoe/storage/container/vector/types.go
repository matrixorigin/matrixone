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

package vector

import (
	"errors"
	"io"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ro "github.com/matrixorigin/matrixone/pkg/container/vector"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"sync"
)

var (
	VecNotRoErr = errors.New("should only be called in read-only mode")
	VecWriteRoErr       = errors.New("write on readonly vector")
	VecInvalidOffsetErr = errors.New("invalid offset error")
	VecTypeNotSupportErr = errors.New("type not supported yet")
)

type IVectorWriter interface {
	io.Closer
	SetValue(int, interface{}) error
	Append(int, interface{}) error
	AppendVector(*ro.Vector, int) (int, error)
}

type IVector interface {
	IsReadonly() bool
	dbi.IVectorReader
	IVectorWriter
	GetLatestView() IVector
	PlacementNew(t types.Type)
}

type IVectorNode interface {
	buf.IMemoryNode
	IVector
}

type BaseVector struct {
	sync.RWMutex
	Type     types.Type
	StatMask container.Mask
	VMask    *nulls.Nulls
}

type StdVector struct {
	BaseVector
	MNode        *common.MemNode
	Data         []byte
	FreeFunc     buf.MemoryFreeFunc
	NodeCapacity uint64
	File         common.IVFile
	UseCompress  bool
}

type StrVector struct {
	BaseVector
	MNodes       []*common.MemNode
	Data         *types.Bytes
	FreeFunc     buf.MemoryFreeFunc
	NodeCapacity uint64
	File         common.IVFile
	UseCompress  bool
}

func NewVector(t types.Type, capacity uint64) IVector {
	switch t.Oid {
	case types.T_char, types.T_varchar, types.T_json:
		return NewStrVector(t, capacity)
	default:
		return NewStdVector(t, capacity)
	}
}
