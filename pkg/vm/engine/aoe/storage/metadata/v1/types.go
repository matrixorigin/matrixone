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

package metadata

import (
	"io"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"sync"

	"github.com/google/btree"
)

const (
	MAX_SEGMENTID = common.MAX_UINT64
	MAX_TABLEID   = common.MAX_UINT64
)

// Resource is an abstraction for two key types of resources
// currently, MetaInfo and Table.
type Resource interface {
	GetResourceType() ResourceType
	GetFileName() string
	GetLastFileName() string
	Serialize(io.Writer) error
	GetTableId() uint64
}

type ResourceType uint8

const (
	ResInfo ResourceType = iota
	ResTable
)

// IndexType tells the type of the index in schema.
type IndexType uint16

const (
	ZoneMap IndexType = iota
	NumBsi
	FixStrBsi
)

// LogIndex records some block related info.
// Used for replay.
type LogIndex struct {
	ID       uint64
	Start    uint64
	Count    uint64
	Capacity uint64
}

type LogHistory struct {
	CreatedIndex uint64
	DeletedIndex uint64
	AppliedIndex uint64
}

// TimeStamp contains the C/U/D time of a ts.
type TimeStamp struct {
	CreatedOn int64
	UpdatedOn int64
	DeletedOn int64
}

type BoundSate uint8

const (
	Standalone BoundSate = iota
	Attached
	Detached
)

// DataState is the general representation for Block and Segment.
// On its changing, some operations like flush would be triggered.
type DataState = uint8

const (
	EMPTY   DataState = iota
	PARTIAL           // Block: 0 < Count < MaxRowCount, Segment: 0 < len(Blocks) < MaxBlockCount
	FULL              // Block: Count == MaxRowCount, Segment: len(Blocks) == MaxBlockCount
	CLOSED            // Segment only. Already FULL and all blocks are FULL
	SORTED            // Segment only. Merge sorted
)

// Block contains metadata for block.
type Block struct {
	sync.RWMutex
	BoundSate
	TimeStamp
	ID          uint64
	MaxRowCount uint64
	Count       uint64
	Index       *LogIndex
	PrevIndex   *LogIndex
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

// Segment contains metadata for segment.
type Segment struct {
	BoundSate
	sync.RWMutex
	TimeStamp
	ID            uint64
	MaxBlockCount uint64
	Blocks        []*Block
	ActiveBlk     int
	IdMap         map[uint64]int
	DataState     DataState
	Table         *Table `json:"-"`
}

// ColDef defines a column in schema.
type ColDef struct {
	// Column name
	Name string
	// Column index in schema
	Idx  int
	// Column type
	Type types.Type
}

// Schema is in representation of a table schema.
type Schema struct {
	// Table name
	Name      string
	// Indices' info
	Indices   []*IndexInfo
	// Column definitions
	ColDefs   []*ColDef
	// Column name -> column index mapping
	NameIdMap map[string]int
}

// IndexInfo contains metadata for an index.
type IndexInfo struct {
	Type    IndexType
	// Columns that the index works on
	Columns []uint16
	ID      uint64
}

type Statistics struct {
	Rows uint64
	Size uint64
}

// Table contains metadata for a table.
type Table struct {
	BoundSate
	sync.RWMutex
	TimeStamp
	LogHistory
	ID            uint64
	Segments      []*Segment
	SegmentCnt    uint64
	ActiveSegment int            `json:"-"`
	IdMap         map[uint64]int `json:"-"`
	Info          *MetaInfo   `json:"-"`
	Stat          *Statistics `json:"-"`
	ReplayIndex   *LogIndex   `json:"-"`
	Schema        *Schema
	Conf          *Configuration
	CheckPoint    uint64
}

// Configuration contains some basic configs for global DB.
type Configuration struct {
	Dir              string
	BlockMaxRows     uint64	`toml:"block-max-rows"`
	SegmentMaxBlocks uint64	`toml:"segment-max-blocks"`
}

// MetaInfo contains some basic metadata for global DB.
type MetaInfo struct {
	*sync.RWMutex
	Sequence   Sequence       `json:"-"`
	Conf       *Configuration `json:"-"`
	CheckPoint uint64
	Tables     map[uint64]*Table
	TableIds   map[uint64]bool   `json:"-"`
	NameMap    map[string]uint64 `json:"-"`
	NameTree   *btree.BTree      `json:"-"`
	Tombstone  map[uint64]bool   `json:"-"`
	CkpTime    int64
}

type CopyCtx struct {
	Ts       int64
	Attached bool
}
