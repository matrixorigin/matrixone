package common

import (
	"fmt"
	"sync/atomic"
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
