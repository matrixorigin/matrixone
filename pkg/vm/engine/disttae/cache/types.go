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
	Id        uint64
	Rowid     types.Rowid
	Typ       string
	CreateSql string

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
	/*
		Rowids in mo_columns from replayed logtail.

		CORNER CASE:
		create table t1(a int);
		begin;
		drop table t1;
		show tables; //no table t1. no item for t1 in mo_columns also.
	*/
	Rowids []types.Rowid

	// table def
	Kind        string
	ViewDef     string
	Constraint  []byte
	Comment     string
	Partitioned int8
	Partition   string
	CreateSql   string

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
	Id           uint64
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
	rowid           types.Rowid //rowid for a column in mo_columns
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
		/*
			The DN use the table id to distinguish different tables.
			For operation on mo_tables, the rowid is the serialized bytes of the table id.
			So, the rowid is unique for each table in mo_tables. We can use it to sort the items.
			To be clear, the comparation a.Id < b.Id does not means the table a.Id is created before the table b.Id.
			We just use it to reserve the items yielded by the truncate on same table multiple times in one txn.

			CORNER CASE:

			create table t1(a int); //table id x.
			begin;
			truncate t1;//table id x changed to x1
			truncate t1;//table id x1 changed to x2
			truncate t1;//table id x2 changed to x3
			commit;//catalog.insertTable(table id x1,x2,x3). catalog.deleteTable(table id x,x1,x2)

			In above case, the DN does not keep the order of multiple insertTable (or deleteTable).
			That is ,it may generate the insertTable order like: x3,x2,x1.
			In previous design without sort on the table id, the item x3,x2 will be overwritten by the x1.
			Then the item x3 will be lost. The DN will not know the table x3. It is wrong!
			With sort on the table id, the item x3,x2,x1 will be reserved.
		*/
		if a.deleted && !b.deleted { //deleted item head first
			return true
		} else if !a.deleted && b.deleted {
			return false
		} else { //a.deleted && b.deleted || !a.deleted && !b.deleted
			return a.Id < b.Id
		}
	}
	return a.Ts.Greater(b.Ts)
}

func databaseItemRowidLess(a, b *DatabaseItem) bool {
	return bytes.Compare(a.Rowid[:], b.Rowid[:]) < 0
}

func tableItemRowidLess(a, b *TableItem) bool {
	return bytes.Compare(a.Rowid[:], b.Rowid[:]) < 0
}

// copyTableItem copies src to dst
func copyTableItem(dst, src *TableItem) {
	dst.Id = src.Id
	dst.Defs = src.Defs
	dst.Kind = src.Kind
	dst.Comment = src.Comment
	dst.ViewDef = src.ViewDef
	dst.TableDef = src.TableDef
	dst.Constraint = src.Constraint
	dst.Partitioned = src.Partitioned
	dst.Partition = src.Partition
	dst.CreateSql = src.CreateSql
	dst.PrimaryIdx = src.PrimaryIdx
	dst.ClusterByIdx = src.ClusterByIdx
	copy(dst.Rowid[:], src.Rowid[:])
	dst.Rowids = make([]types.Rowid, len(src.Rowids))
	for i, rowid := range src.Rowids {
		copy(dst.Rowids[i][:], rowid[:])
	}
}
