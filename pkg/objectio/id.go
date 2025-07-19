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
	ObjectIDSize  = types.ObjectidSize
)

var emptySegmentId types.Segmentid
var emptyBlockId types.Blockid

type ObjectId = types.Objectid
type Segmentid = types.Segmentid
type Blockid = types.Blockid
type Rowid = types.Rowid

var NewObjectid = types.NewObjectid
var NewBlockidWithObjectID = types.NewBlockidWithObjectID
var NewRowid = types.NewRowid
var NewRowIDWithObjectIDBlkNumAndRowID = types.NewRowIDWithObjectIDBlkNumAndRowID

func NewSegmentid() *Segmentid {
	id := types.Uuid(uuid.Must(uuid.NewV7()))
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

func NewObjectidWithSegmentIDAndNum(sid *Segmentid, num uint16) *ObjectId {
	var oid ObjectId
	copy(oid[:types.UuidSize], sid[:])
	copy(oid[types.UuidSize:], types.EncodeUint16(&num))
	return &oid
}

func BuildObjectBlockid(name ObjectName, sequence uint16) *Blockid {
	var id Blockid
	FillBlockidWithNameAndSeq(name, sequence, id[:])
	return &id

}

func FillBlockidWithNameAndSeq(name ObjectName, sequence uint16, to []byte) {
	copy(to, name[0:NameStringOff])
	copy(to[NameStringOff:], types.EncodeUint16(&sequence))
}

func IsBlockInObject(blkID *types.Blockid, objID *ObjectName) bool {
	buf := unsafe.Slice((*byte)(unsafe.Pointer(&blkID[0])), ObjectNameLen)
	return bytes.Equal(buf, *objID)
}

func IsEmptySegid(id *Segmentid) bool {
	return bytes.Equal(id[:], emptySegmentId[:])
}

func IsEmptyBlkid(id *Blockid) bool {
	return bytes.Equal(id[:], emptyBlockId[:])
}
