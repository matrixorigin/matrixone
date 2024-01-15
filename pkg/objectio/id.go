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
	"hash/fnv"
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

func NewObjectid() *ObjectId {
	sid := types.Uuid(uuid.Must(uuid.NewV7()))
	var oid ObjectId
	copy(oid[:types.UuidSize], sid[:])
	return &oid
}

func NewObjectidWithSegmentIDAndNum(sid *Segmentid, num uint16) *ObjectId {
	var oid ObjectId
	copy(oid[:types.UuidSize], sid[:])
	copy(oid[types.UuidSize:], types.EncodeUint16(&num))
	return &oid
}

func NewBlockidWithObjectID(segid *ObjectId, blknum uint16) *Blockid {
	var bid Blockid
	size := types.ObjectidSize
	copy(bid[:size], segid[:])
	copy(bid[size:size+2], types.EncodeUint16(&blknum))
	return &bid
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

func Str2Blockid(id string) *Blockid {
	var blkid Blockid
	copy(blkid[:], id)
	return &blkid
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

// some id hacks

// used only in some special cases
func HackU64ToRowid(v uint64) Rowid {
	var id Rowid
	copy(id[:], types.EncodeUint64(&v))
	return id
}

// used only in some special cases
func HackRowidToU64(id Rowid) uint64 {
	return types.DecodeUint64(id[:])
}

// used only in some special cases
func HackBlockid2Rowid(id *Blockid) Rowid {
	var rowid Rowid
	copy(rowid[:], id[:])
	return rowid
}

// used only in some special cases
func HackSegid2Rowid(id *Segmentid) Rowid {
	var rowid Rowid
	copy(rowid[:], id[:])
	return rowid
}

// used only in some special cases
func HackObjid2Rowid(id *ObjectId) Rowid {
	var rowid Rowid
	copy(rowid[:], id[:])
	return rowid
}

// used only in some special cases
func HackBytes2Rowid(bs []byte) Rowid {
	var rowid types.Rowid
	if size := len(bs); size <= types.RowidSize {
		copy(rowid[:size], bs[:size])
	} else {
		hasher := fnv.New128()
		hasher.Write(bs)
		hasher.Sum(rowid[:0])
	}
	return rowid
}
