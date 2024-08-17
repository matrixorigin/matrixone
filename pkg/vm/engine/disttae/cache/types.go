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
	"encoding/hex"
	"fmt"
	"sync"
	"unsafe"

	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	GcBuffer = 128
)

const (
	MO_OFF           = 2
	MO_ROWID_IDX     = 0
	MO_TIMESTAMP_IDX = 1
)

type TableChangeQuery struct {
	AccountId  uint32
	DatabaseId uint64
	Name       string
	Version    uint32
	TableId    uint64
	Ts         timestamp.Timestamp
}

// catalog cache
type CatalogCache struct {
	mu struct {
		sync.Mutex
		start types.TS
		end   types.TS
	}
	cdcId string
	//tables and database is safe to be read concurrently.
	tables    *tableCache
	databases *databaseCache
}

// database cache:
//
//		. get by database key
//	    . del by consulting cpkey
//	    . gc by timestamp
type databaseCache struct {
	data       *btree.BTreeG[*DatabaseItem]
	cpkeyIndex *btree.BTreeG[*DatabaseItem]
}

// table cache:
//
//		. get by table key
//	    . del by consulting cpkey
//	    . gc by timestamp
type tableCache struct {
	data       *btree.BTreeG[*TableItem]
	cpkeyIndex *btree.BTreeG[*TableItem]
}

type DatabaseItem struct {
	// database key
	AccountId uint32
	Name      string
	Ts        timestamp.Timestamp
	deleted   bool // Mark if it is a delete

	// database value
	Id        uint64
	Typ       string
	CreateSql string
	Rowid     types.Rowid
	CPKey     []byte
}

func (item *DatabaseItem) String() string {
	return fmt.Sprintln(
		"item ptr", uintptr(unsafe.Pointer(item)),
		"item pk",
		hex.EncodeToString(item.CPKey),
		"accId",
		item.AccountId, item.Name, item.Id, item.Ts, item.deleted)
}

type TableItem struct {
	// table key
	AccountId  uint32
	DatabaseId uint64
	Name       string
	Ts         timestamp.Timestamp
	deleted    bool // Mark if it is a delete

	// table value
	Id           uint64
	TableDef     *plan.TableDef
	Defs         []engine.TableDef
	Version      uint32
	DatabaseName string

	Rowid types.Rowid
	CPKey []byte

	// table def
	Kind           string
	ViewDef        string
	Constraint     []byte
	Comment        string
	Partitioned    int8
	Partition      string
	CreateSql      string
	CatalogVersion uint32

	// primary index
	PrimaryIdx    int
	PrimarySeqnum int
	// clusterBy key
	ClusterByIdx int
}

func (item *TableItem) String() string {
	return fmt.Sprintln(
		"item ptr", uintptr(unsafe.Pointer(item)),
		"item pk",
		hex.EncodeToString(item.CPKey),
		"accId",
		item.AccountId, item.DatabaseName, item.DatabaseId, item.Name, item.Id, item.Ts, item.deleted)
}

type noSliceTs struct {
	pTime  int64
	lTime  uint32
	nodeId uint32
}

func (s *noSliceTs) fromTs(ts *timestamp.Timestamp) {
	s.pTime = ts.PhysicalTime
	s.lTime = ts.LogicalTime
	s.nodeId = ts.NodeID
}

func (s *noSliceTs) toTs() timestamp.Timestamp {
	return timestamp.Timestamp{
		PhysicalTime: s.pTime,
		LogicalTime:  s.lTime,
		NodeID:       s.nodeId,
	}
}

type TableItemKey struct {
	AccountId  uint32
	DatabaseId uint64
	Name       string
	Id         uint64
	Ts         noSliceTs
}

type Columns []catalog.Column

func (cols Columns) Len() int           { return len(cols) }
func (cols Columns) Swap(i, j int)      { cols[i], cols[j] = cols[j], cols[i] }
func (cols Columns) Less(i, j int) bool { return cols[i].Num < cols[j].Num }

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
	if a.Ts.Equal(b.Ts) {
		if !a.deleted && b.deleted {
			return true
		}
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

	// Let's think about how to arrange items for the same name table.
	// how many types of items does a table have?
	// 1. create table (deleted=false, ts=t1, tid=x1, rowid=r1)
	// 2. drop table (deleted=true, ts=t2, tid=x1, rowid=r1)
	// 3. truncate
	//    1. drop table (delete=true, ts=t1, tid=x1， rowid=r1)
	//    2. create table (delete=false, ts=t1, tid=x2, rowid=r2)
	// 4. alter table
	//    1. delete table row (delete=true, ts=t1，tid=x1, rowid=r1)
	//    2. create table row (delete=false, ts=t1, tid=x1, rowid=r2)
	//
	// Note: What logtail in mo_tables/mo_columns will CN receive is exactly dependent on what CN sends to TN.
	// No matter how many ddl operations happened in one txn, it will eventually generate at most a pair of delete and insert batch.
	// --- example1:
	// begin;
	// truncate table t1; (drop x1, create t1 with tid x2)
	// truncate table t1; (drop x2, create t1 with tid x3)
	// alter table t1 comment 'new comment1'; (as x3 is created in the txn, adjust its create batch with new comment1)
	// alter table t1 comment 'new comment1'; (as x3 is created in the txn, adjust its create batch again)
	// commit;
	// --- TN received requests to drop x1 and create x3
	// --- TN replayed logtail as:
	// drop table t1 (delete=true, ts=t1, tid=x1, rowid=r1)
	// create table t1 (delete=false, ts=t1, tid=x3, rowid=r2, comment='new comment2')
	//
	// Order rule:
	// 1. By timestamp descending (order by txn)
	// 2. By delete flag, delete is behind of insert (check the last status in the txn)
	//
	// Given the order of the items, it is simple to lookup a table by name:
	// 1. Find the first item with the same name, if it is not deleted, return found, not found otherwise.

	if a.Ts.Equal(b.Ts) { // happen in the same txn. it is a delete and a insert.
		if !a.deleted && b.deleted {
			return true
		}
	}
	return a.Ts.Greater(b.Ts)
}

func databaseItemCPKeyLess(a, b *DatabaseItem) bool {
	return bytes.Compare(a.CPKey[:], b.CPKey[:]) < 0
}

func tableItemCPKeyLess(a, b *TableItem) bool {
	return bytes.Compare(a.CPKey[:], b.CPKey[:]) < 0
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
	dst.PrimarySeqnum = src.PrimarySeqnum
	dst.Version = src.Version
	copy(dst.Rowid[:], src.Rowid[:])
}

func copyDatabaseItem(dest, src *DatabaseItem) {
	dest.AccountId = src.AccountId
	dest.Name = src.Name
	dest.Ts = src.Ts
	dest.Id = src.Id
	copy(dest.Rowid[:], src.Rowid[:])
	dest.Typ = src.Typ
	dest.CreateSql = src.CreateSql
	//deleted bool
}
