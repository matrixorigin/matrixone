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
	"sync/atomic"
)

// ID is the general identifier type shared by different types like
// table, segment, block, etc.
//
// We could wrap info from upper level via ID, for instance, get the table id,
// segment id, and the block id for one block by ID.AsBlockID, which made
// the resource management easier.
type ID struct {
	// Internal table id
	TableID uint64
	// Internal segment id
	SegmentID uint64
	// Internal block id
	BlockID uint64
	// Internal column part id
	PartID uint32
	// Column index for the column part above
	Idx uint16
	// Iter is used for MVCC
	Iter uint8
}

func (id *ID) AsBlockID() ID {
	return ID{
		TableID:   id.TableID,
		SegmentID: id.SegmentID,
		BlockID:   id.BlockID,
	}
}

func (id *ID) AsSegmentID() ID {
	return ID{
		TableID:   id.TableID,
		SegmentID: id.SegmentID,
	}
}

func (id *ID) String() string {
	return fmt.Sprintf("<%d:%d-%d-%d-%d-%d>", id.Idx, id.TableID, id.SegmentID, id.BlockID, id.PartID, id.Iter)
}

func (id *ID) TableString() string {
	return fmt.Sprintf("TBL<%d:%d>", id.Idx, id.TableID)
}
func (id *ID) SegmentString() string {
	return fmt.Sprintf("SEG<%d:%d-%d>", id.Idx, id.TableID, id.SegmentID)
}

func (id *ID) BlockString() string {
	return fmt.Sprintf("BLK<%d:%d-%d-%d>", id.Idx, id.TableID, id.SegmentID, id.BlockID)
}

func (id *ID) IsSameSegment(o ID) bool {
	return id.TableID == o.TableID && id.SegmentID == o.SegmentID
}

func (id *ID) IsSameBlock(o ID) bool {
	return id.TableID == o.TableID && id.SegmentID == o.SegmentID && id.BlockID == o.BlockID
}

func (id *ID) NextBlock() ID {
	newId := atomic.AddUint64(&id.BlockID, uint64(1))
	bid := *id
	bid.BlockID = newId - 1
	return bid
}

func (id *ID) NextSegment() ID {
	newId := atomic.AddUint64(&id.SegmentID, uint64(1))
	bid := *id
	bid.SegmentID = newId - 1
	return bid
}

func IDArraryString(ids []ID) string {
	str := "["
	for _, id := range ids {
		str = fmt.Sprintf("%s%s,", str, id.String())
	}
	str = fmt.Sprintf("%s]", str)
	return str
}
