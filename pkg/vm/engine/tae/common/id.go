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

package common

import (
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// ID is the general identifier type shared by different types like
// table, segment, block, etc.
//
// We could wrap info from upper level via ID, for instance, get the table id,
// segment id, and the block id for one block by ID.AsBlockID, which made
// the resource management easier.
type ID struct {
	// Internal db id
	DbID uint64
	// Internal table id
	TableID uint64
	// Internal block id
	BlockID types.Blockid
}

const (
	IDSize int64 = int64(unsafe.Sizeof(ID{}))
)

func EncodeID(id *ID) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(id)), IDSize)
}

func (id *ID) SegmentID() *types.Segmentid {
	return id.BlockID.Segment()
}

func (id *ID) SetSegmentID(sid *types.Segmentid) {
	copy(id.BlockID[:types.UuidSize], sid[:])
}

func (id *ID) AsBlockID() *ID {
	return &ID{
		DbID:    id.DbID,
		TableID: id.TableID,
		BlockID: id.BlockID,
	}
}

func (id *ID) String() string {
	return fmt.Sprintf("<%d-%d-%s>", id.DbID, id.TableID, id.BlockID.ShortString())
}

func (id *ID) DBString() string {
	return fmt.Sprintf("DB<%d>", id.DbID)
}
func (id *ID) TableString() string {
	return fmt.Sprintf("TBL<%d-%d>", id.DbID, id.TableID)
}
func (id *ID) SegmentString() string {
	return fmt.Sprintf("SEG<%d-%d-%s>", id.DbID, id.TableID, id.BlockID.Segment().ToString())
}

func (id *ID) BlockString() string {
	return fmt.Sprintf("BLK<%d-%d-%s>", id.DbID, id.TableID, id.BlockID.String())
}

func IDArraryString(ids []ID) string {
	str := "["
	for _, id := range ids {
		str = fmt.Sprintf("%s%s,", str, id.String())
	}
	str = fmt.Sprintf("%s]", str)
	return str
}

func BlockIDArraryString(ids []ID) string {
	str := "["
	for _, id := range ids {
		str = fmt.Sprintf("%s%s,", str, id.BlockID.String())
	}
	str = fmt.Sprintf("%s]", str)
	return str
}
