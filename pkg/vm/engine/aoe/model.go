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

package aoe

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
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
	Tables    []*TableInfo `json:"tables"` // Tables in the DBName.
	State     SchemaState  `json:"state"`
	Type      int          `json:"type"` // Engine type of schema: RSE、AOE、Spill
	Epoch     uint64       `json:"epoch"`
}

// TableInfo stores the information of a table or view.
type TableInfo struct {
	SchemaId  uint64       `json:"schema_id"`
	Id        uint64       `json:"id"`
	Name      string       `json:"name"`
	Type      uint64       `json:"type"` // Type of the table: BASE TABLE for a normal table, VIEW for a view, etc.
	Indices   []IndexInfo  `json:"indices"`
	Columns   []ColumnInfo `json:"columns"` // Column is listed in order in which they appear in schema
	Comment   []byte       `json:"comment"`
	State     SchemaState  `json:"state"`
	Partition []byte       `json:"partition"`
	Epoch     uint64       `json:"epoch"`
}

type TabletInfo struct {
	Name    string
	ShardId uint64
	Table   TableInfo
}

// ColumnInfo stores the information of a column.
type ColumnInfo struct {
	SchemaId   uint64               `json:"schema_id"`
	TableID    uint64               `json:"table_id"`
	Id         uint64               `json:"column_id"`
	Name       string               `json:"name"`
	Type       types.Type           `json:"type"`
	Default    metadata.DefaultExpr `json:"default"`
	Alg        int                  `json:"alg"`
	Epoch      uint64               `json:"epoch"`
	PrimaryKey bool                 `json:"primary_key"` // PrimaryKey is the name of the column of the primary key
	NullAbility bool				`json:"nullability"`
}

type IndexInfo struct {
	SchemaId    uint64   `json:"schema_id"`
	TableId     uint64   `json:"table_id"`
	Columns     []uint64 `json:"columns"`
	// Id          uint64   `json:"id"`
	Name        string   `json:"index_names"`
	ColumnNames []string `json:"column_names"`
	Type        IndexT   `json:"type"`
	// Epoch       uint64   `json:"epoch"`
}

type IndexT uint16

const (
	ZoneMap IndexT = iota
	Bsi
	NumBsi
	FixStrBsi
	Invalid
)

type SegmentInfo struct {
	TableId     uint64 `json:"table_id"`
	Id          uint64 `json:"id"`
	GroupId     uint64 `json:"group_id"`
	TabletId    string `json:"tablet_id"`
	PartitionId string `json:"partition_id"`
	Epoch       uint64 `json:"epoch"`
}
