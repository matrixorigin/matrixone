// Copyright 2022 Matrix Origin
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

package cache

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/tidwall/btree"
)

const (
	GcBuffer = 128
)

const (
	MO_OFF           = 2
	MO_ROWID_IDX     = 0
	MO_TIMESTAMP_IDX = 1
)

// catalog cache
type CatalogCache struct {
	tables    *tableCache
	databases *databaseCache
}

// database cache:
//
//		. get by database key
//	    . del by rowid
//	    . gc by timestamp
type databaseCache struct {
	data       *btree.BTreeG[*DatabaseItem]
	rowidIndex *btree.BTreeG[*DatabaseItem]
}

// table cache:
//
//		. get by table key
//	    . del by rowid
//	    . gc by timestamp
type tableCache struct {
	data       *btree.BTreeG[*TableItem]
	rowidIndex *btree.BTreeG[*TableItem]
}

type DatabaseItem struct {
	// database key
	AccountId uint32
	Name      string
	Ts        timestamp.Timestamp

	// database value
	Id    uint64
	Rowid types.Rowid

	// Mark if it is a delete
	deleted bool
}

type TableItem struct {
	// table key
	AccountId  uint32
	DatabaseId uint64
	Name       string
	Ts         timestamp.Timestamp

	// table value
	Id       uint64
	TableDef *plan.TableDef
	Defs     []engine.TableDef
	Rowid    types.Rowid

	// table def
	Kind       string
	ViewDef    string
	Constraint []byte
	Comment    string
	Partition  string
	CreateSql  string

	// primary index
	PrimaryIdx int
	// clusterBy key
	ClusterByIdx int

	// Mark if it is a delete
	deleted bool
}

type tableItemKey struct {
	AccountId    uint32
	DatabaseId   uint64
	Name         string
	PhysicalTime uint64
	LogicalTime  uint32
	NodeId       uint32
}

type column struct {
	name            string
	typ             []byte
	num             int32
	comment         string
	hasDef          int8
	defaultExpr     []byte
	constraintType  string
	isHidden        int8
	isAutoIncrement int8
	hasUpdate       int8
	updateExpr      []byte
	isClusterBy     int8
}

type columns []column

func (cols columns) Len() int           { return len(cols) }
func (cols columns) Swap(i, j int)      { cols[i], cols[j] = cols[j], cols[i] }
func (cols columns) Less(i, j int) bool { return cols[i].num < cols[j].num }

func databaseItemLess(a, b *DatabaseItem) bool {
	if a.AccountId < b.AccountId {
		return true
	}
	if a.AccountId > b.AccountId {
		return false
	}
	if a.Name < b.Name {
		return true
	}
	if a.Name > b.Name {
		return false
	}
	return a.Ts.Greater(b.Ts)
}

func tableItemLess(a, b *TableItem) bool {
	if a.AccountId < b.AccountId {
		return true
	}
	if a.AccountId > b.AccountId {
		return false
	}
	if a.DatabaseId < b.DatabaseId {
		return true
	}
	if a.DatabaseId > b.DatabaseId {
		return false
	}
	if a.Name < b.Name {
		return true
	}
	if a.Name > b.Name {
		return false
	}
	if a.Ts.Equal(b.Ts) {
		return a.deleted
	}
	return a.Ts.Greater(b.Ts)
}

func databaseItemRowidLess(a, b *DatabaseItem) bool {
	return bytes.Compare(a.Rowid[:], b.Rowid[:]) < 0
}

func tableItemRowidLess(a, b *TableItem) bool {
	return bytes.Compare(a.Rowid[:], b.Rowid[:]) < 0
}
