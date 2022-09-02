package message

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type DBInfo struct {
	name  string
	isSys bool
}

type DatabaseCmd struct {
	dbId uint64
	mask uint64
	//creating database name
	create    DBInfo
	tableCmds map[uint64]TableCmd
	drop      bool
}

type Location struct {
	key    string
	offset uint32
	len    uint32
	//original data size
	olen uint32
}

type ZoneMap struct {
	//ref to plan.proto
	typ      types.Type
	min, max any
}

type ColumnMeta struct {
	idx uint16
	//zoneMap      index.ZoneMap
	zM           ZoneMap
	bfLocation   Location
	dataLocation Location
}

type BlockMeta struct {
	columnCnt uint16
	alog      uint8
}

// BlockMeta + ColumnsMeta
type BlockInfo struct {
	blockMeta   BlockMeta
	columnsMeta []ColumnMeta
}

type Vector struct {
	Data     []byte // raw data
	Typ      types.Type
	Nullable bool
	//bitmap
	//Nsp  *nulls.Nulls // nulls list
	Nsp []byte
	// some attributes for const vector (a vector with a lot of rows of a same const value)
	IsConst bool
}

type AppendInfo struct {
	// Attrs column name list
	Attrs []string
	// Vecs col data
	//FIXME:need to slim down for vector.Vector?
	//ref to computation layer .
	//Vecs []*vector.Vector
	Vecs []Vector
}

// [strt, end)
type RowRange struct {
	//start offset
	start uint32
	//end offset
	end uint32
}

// DeleteInfo delete a range of block: [start, end), or delete a bitmap of rows.
// rangeDel and rows is optional in protobuf.
type DeleteInfo struct {
	rangeDel RowRange
	//bitmap
	//FIXME::How to describe a bitmap in protobuf?
	//bytes?
	//rows roaring.Bitmap
	rows []byte
}

// BlockCmd all the sub commands of BlockCmd are optional in protobuf.
type BlockCmd struct {
	//FIXME::transient block id is unique in CN's workspace.
	id uint64
	//bitmap
	mask   uint32
	create BlockInfo
	append AppendInfo
	delete DeleteInfo
	drop   bool
}

// TBCmd is a compound cmd
type TableCmd struct {
	tid uint64
	//bitmap
	mask   uint64
	create TableDef
	//FIXME::transient block id is unique in CN's workspace.
	blockCmds map[uint64]BlockCmd
	drop      bool
}

type PrecommitWriteCmd struct {
	//ID -->DbCmd
	dbCmds map[uint64]DatabaseCmd
}

type DatabaseID struct {
	Id uint64
}

type TableID struct {
	TabId uint64
	DbId  DatabaseID
}

type BlockID struct {
	// Internal table id
	// Internal segment id
	SegmentId SegmentID
	// Internal block id
	BlockId uint64
}

type TSRange struct {
	from timestamp.Timestamp
	to   timestamp.Timestamp
}

type ScopeDesc struct {
	// True indicates all tables
	// False indicates the specified tables
	All bool

	// Indicates the specified tables when All is false
	Tables []TableID
}

type SyncLogTailReq struct {
	// Most suitable visible checkpoint timestamp
	//base checkpoint
	CheckpointTS timestamp.Timestamp

	// [FromTS, ToTS]
	Range TSRange

	// Table ids to read
	Tables []TableID

	// If true, read all tables
	// Else, read the specified tables
	All bool
}

const (
	CmdBatch int16 = iota
	CmdAppend
	CmdUpdate
	CmdMetaEntry
	CmdComposed
	CmdCompactBlock
	CmdMergeBlocks
)

type Batch struct {
	Attrs []string
	Vecs  []Vector
	//Deletes *roaring.Bitmap
	//bitmap
	Deletes []byte
}

type AppendCtx struct {
	//reserved?
	seq            uint32
	srcOff, srcLen uint32
	dbid           uint64
	//block id
	dest BlockID
	//reserved?
	destOff, destLen uint32
}

type BatchCmd struct {
	//CmdBatch
	CmdType int16
	Bat     Batch
}

type AppendCmd struct {
	//CmdAppend
	cmdType int
	BatCmd  BatchCmd
	Ctxs    []AppendCtx
	//commit ts
	Ts timestamp.Timestamp
}

type AppendNode struct {
	BlockId  BlockID
	startRow uint32
	maxRow   uint32
	commitTs timestamp.Timestamp
}

type DeleteNode struct {
	BlockId BlockID
	//bitmap of deleted rows
	Mask     []byte
	commitTs timestamp.Timestamp
}

type ColumnID struct {
	BlockId  BlockID
	colIndex uint16
}

type ColumnUpdateNode struct {
	ColumnId ColumnID
	//bitmap of updated rows
	Mask []byte
	//row offset-->value of a column cell
	Vals map[uint32]any
}

// update command for mvcc nodes
type UpdateCmd struct {
	//CmdUpdate
	CmdType int16
	DbId    uint64
	BlockId BlockID
	//bitmap
	Mask   int16
	Append AppendNode
	//Update  ColumnUpdateNode
	Delete DeleteNode
}

// commands for catalog
const (
	CmdUpdateDatabase = int16(256) + iota
	CmdUpdateTable
	CmdUpdateSegment
	CmdUpdateBlock
	//CmdLogDatabase
	//CmdLogTable
	//CmdLogSegment
	//CmdLogBlock
)

type EntryState int8

const (
	ES_Appendable EntryState = iota
	ES_NotAppendable
	ES_Frozen
)

type UpdateNode struct {
	CreatedAt timestamp.Timestamp
	DeletedAt timestamp.Timestamp
	MetaLoc   string
	DeltaLoc  string

	Start, End timestamp.Timestamp
	//FIXME::?
	//LogIndex   []*wal.Index
}

type BaseEntry struct {
	Node UpdateNode
	//sequence nnumber?
	//Id  uint64
}

type BlockEntry struct {
	Be         BaseEntry
	BlockId    BlockID
	BlockState EntryState
}

type SegmentEntry struct {
	Be       BaseEntry
	SegId    SegmentID
	SegState EntryState
}

type AccessInfo struct {
	TenantID, UserID, RoleID uint32
	CreateAt                 timestamp.Timestamp
}

type Default struct {
	NullAbility  bool
	Expr         []byte
	OriginString string
}

// TODO:ref to plan.proto?
type ColDef struct {
	Name          string
	Idx           int
	Type          types.Type
	Hidden        bool // Hidden Column is generated by compute layer, keep hidden from user
	PhyAddr       bool // PhyAddr Column is generated by tae as rowid
	NullAbility   bool
	AutoIncrement bool
	Primary       bool
	SortIdx       int8
	SortKey       bool
	Comment       string
	Default       Default
}
type TableDef struct {
	AcInfo    AccessInfo
	Name      string
	ColDefs   []ColDef
	NameIndex map[string]int
	//BlockMaxRows     uint32
	//SegmentMaxBlocks uint16
	Comment   string
	Relkind   string
	Createsql string
	View      string

	//SortKey    *SortKey
	//PhyAddrKey *ColDef
}

type TableEntry struct {
	Be     BaseEntry
	TabId  TableID
	TabDef TableDef
}

type DatabaseEntry struct {
	Be     BaseEntry
	Name   string
	DbId   DatabaseID
	AcInfo AccessInfo
}

type EntryCmd struct {
	//CmdMetaEntry
	CmdType int16
	//bitmask
	Mask     uint16
	BlkEntry BlockEntry
	SegEntry SegmentEntry
	TbEntry  TableEntry
	DbEntry  DatabaseEntry
}

// commands for compaction and merge
type CompactBlockCmd struct {
	//CmdCompactBlock
	CmdType int16
	from    BlockID
	to      BlockID
	//sequence num
	//id   uint32
}

type SegmentID struct {
	Id    uint64
	TabId TableID
}

type MergeBlocksCmd struct {
	//CmdMergeBlocks
	CmdType     int16
	droppedSegs []SegmentID
	createdSegs []SegmentID
	droppedBlks []BlockID
	createdBlks []BlockID
	mapping     []uint32
	fromAddr    []uint32
	toAddr      []uint32
	//sequence num
	//id          uint32
}

type ComposedCmd struct {
	//CmdComposed
	cmdType    int16
	mask       int16
	AppendCmds []AppendCmd
	UpdateCmds []UpdateCmd
	EntryCmds  []EntryCmd
}

type BatchCommands struct {
	// Range description
	Desc TSRange
	// Scope description
	Scope ScopeDesc
	//FIXME::repeated.
	Commands []ComposedCmd
}

type SyncLogTailResp struct {
	// Actual checkpoint timestamp
	CheckpointTS timestamp.Timestamp

	// New checkpoints found in DN
	NewCheckpoints []timestamp.Timestamp

	// Tail commands
	//Commands *BatchCommands
	Commands BatchCommands
}
