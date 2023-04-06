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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"unsafe"
)

const (
	FileNameLen = types.UuidSize + 2
	ExtentOff   = FileNameLen
	ExtentLen   = 16
	RowsOff     = ExtentOff + ExtentLen
	RowsLen     = 4
	BlockIDOff  = RowsOff + RowsLen
	BlockIDLen  = 4
	LocationLen = FileNameLen + ExtentLen + RowsLen + BlockIDLen
)

type Location []byte

type ObjectName []byte

func BuildLocation(name ObjectName, extent Extent, rows uint32, id uint32) Location {
	var location [LocationLen]byte
	copy(location[:FileNameLen], name.Marshal())
	copy(location[ExtentOff:ExtentOff+ExtentSize], extent.Marshal())
	copy(location[RowsOff:RowsOff+RowsLen], types.EncodeUint32(&rows))
	copy(location[BlockIDOff:BlockIDOff+BlockIDLen], types.EncodeUint32(&id))
	return unsafe.Slice((*byte)(unsafe.Pointer(&location)), LocationLen)
}

func (l Location) GetName() ObjectName {
	var name ObjectName
	return name.Unmarshal(l[:FileNameLen])
}

func (l Location) GetExtent() Extent {
	extent := Extent{}
	extent.Unmarshal(l[ExtentOff : ExtentOff+ExtentLen])
	return extent
}

func (l Location) GetRows() uint32 {
	return types.DecodeUint32(l[RowsOff : RowsOff+RowsLen])
}

func (l Location) GetID() uint32 {
	return types.DecodeUint32(l[BlockIDOff : BlockIDOff+BlockIDLen])
}

func BuildObjectName(uuid types.Uuid, num uint16) ObjectName {
	var name [FileNameLen]byte
	copy(name[:types.UuidSize], types.EncodeUuid(&uuid))
	copy(name[types.UuidSize:FileNameLen], types.EncodeUint16(&num))
	return unsafe.Slice((*byte)(unsafe.Pointer(&name)), FileNameLen)
}

func (o ObjectName) Unmarshal(data []byte) ObjectName {
	return *(*ObjectName)(unsafe.Pointer(&data[0]))
}

func (o ObjectName) Marshal() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&o)), FileNameLen)
}

func (o ObjectName) ToString() string {
	return fmt.Sprintf("%v-%d", types.DecodeUuid(o[:16]).ToString(), types.DecodeUint16(o[16:18]))
}
