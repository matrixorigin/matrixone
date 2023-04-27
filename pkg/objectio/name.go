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
	"bytes"
	"fmt"
	"math/rand"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

/*
	 ObjectName is a fixed-length unmodifiable byte array.
	 Layout: SegmentId     |   Num    |  NameStr
		     (types.Uuid)    (uint16)   (string)
	 Size:	   16B         +   2B     +  42B
*/
type ObjectName []byte
type ObjectNameShort [ObjectNameShortLen]byte

func BuildObjectName(segid Segmentid, num uint16) ObjectName {
	var name [ObjectNameLen]byte
	copy(name[:SegmentIdSize], types.EncodeUuid(&segid))
	copy(name[FileNumOff:FileNumOff+FileNumLen], types.EncodeUint16(&num))
	str := fmt.Sprintf("%v_%05d", segid.ToString(), num)
	copy(name[NameStringOff:NameStringOff+NameStringLen], str)
	return unsafe.Slice((*byte)(unsafe.Pointer(&name)), ObjectNameLen)
}

func (s *ObjectNameShort) Segmentid() *Segmentid {
	return (*Segmentid)(unsafe.Pointer(&s[0]))
}

func (s *ObjectNameShort) Num() uint16 {
	return types.DecodeUint16(s[SegmentIdSize:])
}

func (o ObjectName) String() string {
	return string(o[NameStringOff : NameStringOff+NameStringLen])
}

func (o ObjectName) Short() *ObjectNameShort {
	return (*ObjectNameShort)(unsafe.Pointer(&o[0]))
}

func (o ObjectName) SegmentId() Segmentid {
	return types.DecodeUuid(o[:SegmentIdSize])
}

func (o ObjectName) Num() uint16 {
	return types.DecodeUint16(o[FileNumOff : FileNumOff+FileNumLen])
}

func MockObjectName() ObjectName {
	return BuildObjectName(NewSegmentid(), uint16(rand.Intn(1000)))
}

func MockLocation(name ObjectName) Location {
	extent := NewExtent(0, uint32(rand.Intn(10000)), uint32(rand.Intn(10000)), uint32(rand.Intn(10000)))
	return BuildLocation(name, extent, uint32(rand.Intn(8192)), uint16(rand.Intn(300)))
}

func (o ObjectName) Equal(a ObjectName) bool {
	return bytes.Equal(o, a)
}

var NameQueryResult [NameStringOff]byte
var NameCheckPoint [NameStringOff]byte
var NameDiskCleaner [NameStringOff]byte
var NameETL [NameStringOff]byte
var NameNormal [NameStringOff]byte

func init() {
	copy(NameQueryResult[:], "Query_ResultXXXX")
	copy(NameCheckPoint[:], "CheckPoint_MetaX")
	copy(NameDiskCleaner[:], "Disk_CleanerXXXX")
	copy(NameETL[:], "Writer_ETLXXXXXX")
	copy(NameNormal[:], "Writer_NormalXXX")
}

func BuildQueryResultName() ObjectName {
	return NameQueryResult[:]
}

func BuildCheckpointName() ObjectName {
	return NameCheckPoint[:]
}

func BuildDiskCleanerName() ObjectName {
	return NameDiskCleaner[:]
}

func BuildETLName() ObjectName {
	return NameETL[:]
}

func BuildNormalName() ObjectName {
	return NameNormal[:]
}
