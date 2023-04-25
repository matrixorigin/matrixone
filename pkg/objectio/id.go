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
	"unsafe"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	SegmentIdSize = types.UuidSize
)

var emptySegmentId types.Segmentid
var emptyBlockId types.Blockid

type Segmentid = types.Segmentid
type Blockid = types.Blockid
type Rowid = types.Rowid

func NewSegmentid() *Segmentid {
	id := types.Uuid(uuid.Must(uuid.NewUUID()))
	return &id
}

func NewBlockid(segid *Segmentid, fnum, blknum uint16) *Blockid {
	var id Blockid
	size := SegmentIdSize
	copy(id[:size], segid[:])
	copy(id[size:size+2], types.EncodeUint16(&fnum))
	copy(id[size+2:size+4], types.EncodeUint16(&blknum))
	return &id
}

func NewRowid(blkid *Blockid, offset uint32) *types.Rowid {
	var rowid types.Rowid
	size := types.BlockidSize
	copy(rowid[:size], blkid[:])
	copy(rowid[size:size+4], types.EncodeUint32(&offset))
	return &rowid
}

func BuildObjectBlockid(name ObjectName, sequence uint16) *Blockid {
	var id Blockid
	copy(id[:], name[0:NameStringOff])
	copy(id[NameStringOff:], types.EncodeUint16(&sequence))
	return &id
}

func ToObjectNameShort(blkID *Blockid) *ObjectNameShort {
	return (*ObjectNameShort)(unsafe.Pointer(&blkID[0]))
}

func IsBlockInObject(blkID *types.Blockid, objID *ObjectName) bool {
	buf := unsafe.Slice((*byte)(unsafe.Pointer(&blkID[0])), ObjectNameLen)
	return bytes.Equal(buf, *objID)
}

func ToSegmentId(blkID *Blockid) *Segmentid {
	return (*Segmentid)(unsafe.Pointer(blkID))
}

func IsEmptySegid(id *Segmentid) bool {
	return bytes.Equal(id[:], emptySegmentId[:])
}

func IsEmptyBlkid(id *Blockid) bool {
	return bytes.Equal(id[:], emptyBlockId[:])
}
