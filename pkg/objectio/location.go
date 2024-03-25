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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	ExtentOff   = ObjectNameLen
	ExtentLen   = ExtentSize
	RowsOff     = ExtentOff + ExtentLen
	RowsLen     = 4
	BlockIDOff  = RowsOff + RowsLen
	BlockIDLen  = 2
	LocationLen = BlockIDOff + BlockIDLen
)

const (
	FileNumOff         = SegmentIdSize
	FileNumLen         = 2
	NameStringOff      = FileNumOff + FileNumLen
	NameStringLen      = 42 //uuid[36]+_[1]+filename[5]
	ObjectNameLen      = NameStringOff + NameStringLen
	ObjectNameShortLen = NameStringOff
)

/*
Location is a fixed-length unmodifiable byte array.
Layout:  ObjectName | Extent | Rows(uint32) | ID(uint16)
*/
type Location []byte

func BuildLocation(name ObjectName, extent Extent, rows uint32, id uint16) Location {
	var location [LocationLen]byte
	copy(location[:ObjectNameLen], name)
	copy(location[ExtentOff:ExtentOff+ExtentSize], extent)
	copy(location[RowsOff:RowsOff+RowsLen], types.EncodeUint32(&rows))
	copy(location[BlockIDOff:BlockIDOff+BlockIDLen], types.EncodeUint16(&id))
	return unsafe.Slice((*byte)(unsafe.Pointer(&location)), LocationLen)
}

func (l Location) Name() ObjectName {
	return ObjectName(l[:ObjectNameLen])
}

func (l Location) ShortName() *ObjectNameShort {
	return (*ObjectNameShort)(unsafe.Pointer(&l[0]))
}

func (l Location) Extent() Extent {
	return Extent(l[ExtentOff : ExtentOff+ExtentLen])
}

func (l Location) Rows() uint32 {
	return types.DecodeUint32(l[RowsOff : RowsOff+RowsLen])
}
func (l Location) SetRows(rows uint32) {
	copy(l[RowsOff:RowsOff+RowsLen], types.EncodeUint32(&rows))
}

func (l Location) ID() uint16 {
	return types.DecodeUint16(l[BlockIDOff : BlockIDOff+BlockIDLen])
}

func (l Location) SetID(id uint16) {
	copy(l[BlockIDOff:BlockIDOff+BlockIDLen], types.EncodeUint16(&id))
}

func (l Location) IsEmpty() bool {
	return len(l) < LocationLen || types.DecodeInt64(l[:ObjectNameLen]) == 0
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
