package aoe

import "matrixone/pkg/container/types"

type Group uint64

const (
	KVGroup Group = iota
	AOEGroup
)

type SchemaState byte

const (
	// StateNone means this schema element is absent and can't be used.
	StateNone SchemaState = iota
	// StateDeleteOnly means we can only delete items for this schema element.
	StateDeleteOnly
	// StatePublic means this schema element is ok for all write and read operations.
	StatePublic
)

const (
	SharedShardUnique = "###shared"
)

type CatalogInfo struct {
	Id   uint64
	Name string
}

// SchemaInfo stores the information of a schema(database).
type SchemaInfo struct {
	CatalogId uint64       `json:"catalog_id"`
	Id        uint64       `json:"id"`
	Name      string       `json:"name"`
	Tables    []*TableInfo `json:"tables"` // Tables in the DB.
	State     SchemaState  `json:"state"`
}

// TableInfo stores the information of a table or view.
type TableInfo struct {
	SchemaId uint64 `json:"schema_id"`
	Id       uint64 `json:"id"`
	Name     string `json:"name"`
	// Type of the table: BASE TABLE for a normal table, VIEW for a view, etc.
	Type string `json:"type"`
	// Column is listed in order in which they appear in schema
	Columns         []ColumnInfo `json:"columns"`
	Comment         []byte       `json:"comment"`
	State           SchemaState  `json:"state"`
	Partition       []byte       `json:"partition"`
	CreateStatement []byte       `json:"create_statement"`
}

// ColumnInfo stores the information of a column.
type ColumnInfo struct {
	SchemaId uint64     `json:"schema_id"`
	TableID  uint64     `json:"table_id"`
	Id       uint64     `json:"column_id"`
	Name     string     `json:"name"`
	Type     types.Type `json:"type"`
	Alg      int        `json:"alg"`
}

// PartitionInfo stores the information of a partition.
type PartitionInfo struct {
	SchemaId   uint64   `json:"schema_id"`
	TableID    uint64   `json:"table_id"`
	Columns    []string `json:"columns"`
	Ids        []uint64 `json:"ids"`
	Names      []string `json:"names"`
	Definition []byte   `json:"definition"`
}

// SegmentInfo stores the information of a segment.
type SegmentInfo struct {
	TableId     uint64 `json:"table_id"`
	Id          uint64 `json:"id"`
	GroupId     uint64 `json:"group_id"`
	TabletId    string `json:"tablet_id"`
	PartitionId string `json:"partition_id"`
}
