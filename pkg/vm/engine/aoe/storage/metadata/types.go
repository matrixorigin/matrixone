package md

import (
	"matrixone/pkg/container/types"
	"sync"
)

type IndexType uint16

const (
	ZoneMap IndexType = iota
)

type LogIndex struct {
	ID       uint64
	Start    uint64
	Count    uint64
	Capacity uint64
}

type TimeStamp struct {
	CreatedOn  int64
	UpdatedOn  int64
	DeltetedOn int64
}

type BoundSate uint8

const (
	STANDLONE BoundSate = iota
	Attached
	Detatched
)

type DataState = uint8

const (
	EMPTY   DataState = iota
	PARTIAL           // Block: 0 < Count < MaxRowCount, Segment: 0 < len(Blocks) < MaxBlockCount
	FULL              // Block: Count == MaxRowCount, Segment: len(Blocks) == MaxBlockCount
	CLOSED            // Segment only. Already FULL and all blocks are FULL
	SORTED            // Segment only. Merge sorted
)

type Block struct {
	sync.RWMutex
	BoundSate
	TimeStamp
	ID          uint64
	MaxRowCount uint64
	Count       uint64
	Index       *LogIndex
	PrevIndex   *LogIndex
	DeleteIndex *uint64
	DataState   DataState
	Segment     *Segment `json:"-"`
}

type Sequence struct {
	NextBlockID     uint64
	NextSegmentID   uint64
	NextPartitionID uint64
	NextTableID     uint64
	NextIndexID     uint64
}

type Segment struct {
	BoundSate
	sync.RWMutex
	TimeStamp
	ID            uint64
	TableID       uint64
	MaxBlockCount uint64
	Count         uint64
	Blocks        []*Block
	ActiveBlk     int
	IdMap         map[uint64]int
	DataState     DataState
	Info          *MetaInfo `json:"-"`
	Schema        *Schema   `json:"-"`
}

type ColDef struct {
	Name string
	Idx  int
	Type types.Type
}

type Schema struct {
	Name      string
	Indexes   []*IndexInfo
	ColDefs   []*ColDef
	NameIdMap map[string]int
}

type IndexInfo struct {
	Type    IndexType
	Columns []uint16
	ID      uint64
}

type Table struct {
	BoundSate
	sync.RWMutex
	TimeStamp
	ID            uint64
	Segments      []*Segment
	SegmentCnt    uint64
	ActiveSegment int            `json:"-"`
	IdMap         map[uint64]int `json:"-"`
	Info          *MetaInfo      `json:"-"`
	Schema        *Schema
}

type Configuration struct {
	Dir              string
	BlockMaxRows     uint64
	SegmentMaxBlocks uint64
}

type MetaInfo struct {
	sync.RWMutex
	Sequence   Sequence       `json:"-"`
	Conf       *Configuration `json:"-"`
	CheckPoint uint64
	Tables     map[uint64]*Table
	TableIds   map[uint64]bool   `json:"-"`
	NameMap    map[string]uint64 `json:"-"`
	Tombstone  map[uint64]bool   `json:"-"`
}

type CopyCtx struct {
	Ts       int64
	Attached bool
}
