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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"math/rand"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	ExtentOff   = ObjectNameLen
	ExtentLen   = 16
	RowsOff     = ExtentOff + ExtentLen
	RowsLen     = 4
	BlockIDOff  = RowsOff + RowsLen
	BlockIDLen  = 4
	LocationLen = BlockIDOff + BlockIDLen
)

type Location []byte

const (
	SidLen        = types.UuidSize
	FileNumOff    = SidLen
	FileNumLen    = 2
	NameStringOff = FileNumOff + FileNumLen
	NameStringLen = 42 //uuid[36]+_[1]+filename[5]
	ObjectNameLen = NameStringOff + NameStringLen
)

type ObjectName []byte

func BuildLocation(name ObjectName, extent Extent, rows uint32, id uint32) Location {
	var location [LocationLen]byte
	copy(location[:ObjectNameLen], name)
	copy(location[ExtentOff:ExtentOff+ExtentSize], extent.Marshal())
	copy(location[RowsOff:RowsOff+RowsLen], types.EncodeUint32(&rows))
	copy(location[BlockIDOff:BlockIDOff+BlockIDLen], types.EncodeUint32(&id))
	return unsafe.Slice((*byte)(unsafe.Pointer(&location)), LocationLen)
}

func (l Location) Name() ObjectName {
	return ObjectName(l[:ObjectNameLen])
}

func (l Location) Extent() *Extent {
	return (*Extent)(unsafe.Pointer(&l[ExtentOff]))
}

func (l Location) Rows() uint32 {
	return types.DecodeUint32(l[RowsOff : RowsOff+RowsLen])
}

func (l Location) ID() uint32 {
	return types.DecodeUint32(l[BlockIDOff : BlockIDOff+BlockIDLen])
}

func (l Location) IsEmpty() bool {
	return len(l) == 0
}

func (l Location) String() string {
	if len(l) == 0 {
		return ""
	}
	if len(l) != LocationLen {
		return string(l)
	}
	return fmt.Sprintf("%v_%v_%d_%d", l.Name().String(), l.Extent(), l.Rows(), l.ID())
}

func BuildObjectName(uuid types.Uuid, num uint16) ObjectName {
	var name [ObjectNameLen]byte
	copy(name[:SidLen], types.EncodeUuid(&uuid))
	copy(name[FileNumOff:FileNumOff+FileNumLen], types.EncodeUint16(&num))
	str := fmt.Sprintf("%v_%05d", uuid.ToString(), num)
	copy(name[NameStringOff:NameStringOff+NameStringLen], str)
	return unsafe.Slice((*byte)(unsafe.Pointer(&name)), ObjectNameLen)
}

func (o ObjectName) String() string {
	return string(o[NameStringOff : NameStringOff+NameStringLen])
}

func (o ObjectName) Sid() types.Uuid {
	return types.DecodeUuid(o[:SidLen])
}

func (o ObjectName) Num() uint16 {
	return types.DecodeUint16(o[FileNumOff : FileNumOff+FileNumLen])
}

func MockObjectName() ObjectName {
	return BuildObjectName(common.NewSegmentid(), uint16(rand.Intn(1000)))
}

func MockLocation(name ObjectName) Location {
	extent := Extent{
		id:         uint32(rand.Intn(300)),
		offset:     uint32(rand.Intn(10000)),
		length:     uint32(rand.Intn(10000)),
		originSize: uint32(rand.Intn(10000)),
	}
	return BuildLocation(name, extent, uint32(rand.Intn(8192)), extent.id)
}
