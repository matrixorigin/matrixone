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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

var (
	ErrParseBlockFileName = errors.New("aoe: parse block file name")
)

type ID struct {
	TableID   uint64
	Idx       uint16
	SegmentID uint64
	BlockID   uint64
	PartID    uint32
	Iter      uint8
}

const (
	TRANSIENT_TABLE_START_ID uint64 = ^(uint64(0)) / 2
)

func NewTransientID() *ID {
	return &ID{
		TableID: TRANSIENT_TABLE_START_ID,
	}
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
	return fmt.Sprintf("ID<%d:%d-%d-%d-%d-%d>", id.Idx, id.TableID, id.SegmentID, id.BlockID, id.PartID, id.Iter)
}

func (id *ID) TableString() string {
	return fmt.Sprintf("ID<%d>", id.TableID)
}

func (id *ID) SegmentString() string {
	return fmt.Sprintf("ID<%d:%d-%d>", id.Idx, id.TableID, id.SegmentID)
}

func (id *ID) BlockString() string {
	return fmt.Sprintf("ID<%d:%d-%d-%d>", id.Idx, id.TableID, id.SegmentID, id.BlockID)
}

func (id *ID) IsSameSegment(o ID) bool {
	return id.TableID == o.TableID && id.SegmentID == o.SegmentID
}

func (id *ID) IsSameBlock(o ID) bool {
	return id.TableID == o.TableID && id.SegmentID == o.SegmentID && id.BlockID == o.BlockID
}

func (id *ID) Next() *ID {
	new_id := atomic.AddUint64(&id.TableID, uint64(1))
	return &ID{
		TableID: new_id - 1,
	}
}

func (id *ID) NextPart() ID {
	new_id := atomic.AddUint32(&id.PartID, uint32(1))
	bid := *id
	bid.PartID = new_id - 1
	return bid
}

func (id *ID) NextIter() ID {
	return ID{
		TableID:   id.TableID,
		Idx:       id.Idx,
		SegmentID: id.SegmentID,
		BlockID:   id.BlockID,
		PartID:    id.PartID,
		Iter:      id.Iter + 1,
	}
}

func (id *ID) NextBlock() ID {
	new_id := atomic.AddUint64(&id.BlockID, uint64(1))
	bid := *id
	bid.BlockID = new_id - 1
	return bid
}

func (id *ID) NextSegment() ID {
	new_id := atomic.AddUint64(&id.SegmentID, uint64(1))
	bid := *id
	bid.SegmentID = new_id - 1
	return bid
}

func (id *ID) IsTransient() bool {
	if id.TableID >= TRANSIENT_TABLE_START_ID {
		return true
	}
	return false
}

func (id *ID) ToPartFileName() string {
	return fmt.Sprintf("%d_%d_%d_%d_%d", id.Idx, id.TableID, id.SegmentID, id.BlockID, id.PartID)
}

func (id *ID) ToPartFilePath() string {
	return fmt.Sprintf("%d/%d/%d/%d/%d.%d", id.TableID, id.SegmentID, id.BlockID, id.Idx, id.PartID, id.Iter)
}

func (id *ID) ToBlockFileName() string {
	return fmt.Sprintf("%d_%d_%d", id.TableID, id.SegmentID, id.BlockID)
}

func (id *ID) ToBlockFilePath() string {
	return fmt.Sprintf("%d/%d/%d/", id.TableID, id.SegmentID, id.BlockID)
}

func (id *ID) ToSegmentFileName() string {
	return fmt.Sprintf("%d_%d", id.TableID, id.SegmentID)
}

func (id *ID) ToSegmentFilePath() string {
	return fmt.Sprintf("%d/%d/", id.TableID, id.SegmentID)
}

func ParseBlockFileName(name string) (ID, error) {
	var (
		id  ID
		err error
	)
	strs := strings.Split(name, "_")
	if len(strs) != 3 {
		return id, ErrParseBlockFileName
	}
	tid, err := strconv.ParseUint(strs[0], 10, 64)
	if err != nil {
		return id, err
	}
	sid, err := strconv.ParseUint(strs[1], 10, 64)
	if err != nil {
		return id, err
	}
	bid, err := strconv.ParseUint(strs[2], 10, 64)
	if err != nil {
		return id, err
	}
	id.TableID, id.SegmentID, id.BlockID = tid, sid, bid
	return id, nil
}

func ParseSegmentFileName(name string) (ID, error) {
	var (
		id  ID
		err error
	)
	strs := strings.Split(name, "_")
	if len(strs) != 2 {
		return id, ErrParseBlockFileName
	}
	tid, err := strconv.ParseUint(strs[0], 10, 64)
	if err != nil {
		return id, err
	}
	sid, err := strconv.ParseUint(strs[1], 10, 64)
	if err != nil {
		return id, err
	}
	id.TableID, id.SegmentID = tid, sid
	return id, nil
}
