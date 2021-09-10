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
	"matrixone/pkg/container/types"
	// log "github.com/sirupsen/logrus"
)

type Vector interface {
	Append(Vector, uint64) (n uint64, err error)
	GetData() []byte
	GetCount() uint64
	// GetAuxData() [][]byte
}

type BaseVector struct {
	Type   types.Type
	Data   []byte
	Offset int
}

type StdVector struct {
	BaseVector
}

type StrVector struct {
	BaseVector
	AuxData [][]byte
	// AuxAlloc common.IAllocator
}

func NewStdVector(t types.Type, dataBuf []byte) Vector {
	vec := &StdVector{
		BaseVector: BaseVector{
			Type: t,
			Data: dataBuf,
		},
	}
	return vec
}

func (v *StdVector) GetCount() uint64 {
	return uint64(v.Offset) / uint64(v.Type.Size)
}

func (v *StdVector) GetData() []byte {
	return v.Data
}

func (v *StdVector) Append(o Vector, offset uint64) (n uint64, err error) {
	buf := o.GetData()
	tsize := int(v.Type.Size)
	remaining := cap(v.Data) - v.Offset
	other_remaining := len(buf) - tsize*int(offset)
	to_write := other_remaining
	if other_remaining > remaining {
		to_write = remaining
	}
	start := int(offset) * tsize
	end := int(offset)*tsize + to_write
	// log.Infof("1. cap(v.Data)=%d, len(v.Data)=%d, v.Offset=%d, start=%d, end=%d\n", cap(v.Data), len(v.Data), v.Offset, start, end)
	copy(v.Data[v.Offset:], buf[start:end])
	// log.Infof("2. cap(v.Data)=%d, len(v.Data)=%d, v.Offset=%d\n", cap(v.Data), len(v.Data), v.Offset)
	v.Offset += to_write
	return uint64(to_write / tsize), nil
}

// func NewStrVector(t types.Type, dataBuf []byte, auxBuf []byte, alloc common.IAllocator) Vector {
// 	vec := &StrVector{
// 		BaseVector: BaseVector{
// 			Type: t,
// 			Data: dataBuf,
// 		},
// 		AuxData:  make([][]byte, 0),
// 		AuxAlloc: alloc,
// 	}
// 	vec.AuxData = append(vec.AuxData, auxBuf)
// 	return vec
// }

// func (v *StrVector) GetData() []byte {
// 	return v.Data
// }

// func (v *StrVector) Append(o Vector, offset uint64) (n uint64, err error) {
// 	buf := o.GetData()
// 	remaining := cap(v.Data) - len(v.Data)
// 	other_remaining := len(buf) - int(offset)
// 	to_write := other_remaining
// 	if other_remaining > remaining {
// 		to_write = remaining
// 	}
// 	return uint64(to_write), nil
// }
