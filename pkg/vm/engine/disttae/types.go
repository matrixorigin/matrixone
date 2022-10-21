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

package disttae

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	INSERT = iota
	DELETE
)

const (
	MO_DATABASE_ID_NAME_IDX       = 1
	MO_DATABASE_ID_ACCOUNT_IDX    = 2
	MO_DATABASE_LIST_ACCOUNT_IDX  = 1
	MO_TABLE_ID_NAME_IDX          = 1
	MO_TABLE_ID_DATABASE_ID_IDX   = 2
	MO_TABLE_ID_ACCOUNT_IDX       = 3
	MO_TABLE_LIST_DATABASE_ID_IDX = 1
	MO_TABLE_LIST_ACCOUNT_IDX     = 2
	MO_PRIMARY_OFF                = 2
)

type DNStore = logservice.DNStore

// tae's block metadata, which is currently just an empty one,
// does not serve any purpose When tae submits a concrete structure,
// it will replace this structure with tae's code
// type BlockMeta struct {

// }

// Cache is a multi-version cache for maintaining some table data.
// The cache is concurrently secure,  with multiple transactions accessing
// the cache at the same time.
// For different dn,  the cache is handled independently, the format
// for our example is k-v, k being the dn number and v the timestamp,
// suppose there are 2 dn, for table A exist dn0 - 100, dn1 - 200.
type Cache interface {
	// update table's cache to the specified timestamp
	Update(ctx context.Context, dnList []DNStore, databaseId uint64,
		tableId uint64, ts timestamp.Timestamp) error
}

type IDGenerator interface {
	AllocateID(ctx context.Context) (uint64, error)
}

// mvcc is the core data structure of cn and is used to
// maintain multiple versions of logtail data for a table's partition
type MVCC interface {
	CheckPoint(ctx context.Context, ts timestamp.Timestamp) error
	Insert(ctx context.Context, primaryKeyIndex int, bat *api.Batch, needCheck bool) error
	Delete(ctx context.Context, bat *api.Batch) error
	BlockList(ctx context.Context, ts timestamp.Timestamp,
		blocks []BlockMeta, entries []Entry) ([]BlockMeta, map[uint64][]int)
	// If blocks is empty, it means no merge operation with the files on s3 is required.
	NewReader(ctx context.Context, readerNumber int, expr *plan.Expr, defs []engine.TableDef,
		tableDef *plan.TableDef, blks []ModifyBlockMeta, ts timestamp.Timestamp,
		fs fileservice.FileService, entries []Entry) ([]engine.Reader, error)
}

type Engine struct {
	sync.RWMutex
	mp                *mpool.MPool
	fs                fileservice.FileService
	db                *DB
	cli               client.TxnClient
	idGen             IDGenerator
	getClusterDetails GetClusterDetailsFunc
	txns              map[string]*Transaction
	// minimum heap of currently active transactions
	txnHeap *transactionHeap
}

// DB is implementataion of cache
type DB struct {
	sync.RWMutex
	dnMap      map[string]int
	metaTables map[string]Partitions
	tables     map[[2]uint64]Partitions
}

type Partitions []*Partition

// a partition corresponds to a dn
type Partition struct {
	sync.RWMutex
	// multi-version data of logtail, implemented with reusee's memengine
	data *memtable.Table[RowID, DataValue, *DataRow]
	// last updated timestamp
	ts timestamp.Timestamp
}

// Transaction represents a transaction
type Transaction struct {
	db *DB
	// readOnly default value is true, once a write happen, then set to false
	readOnly bool
	// db       *DB
	// blockId starts at 0 and keeps incrementing,
	// this is used to name the file on s3 and then give it to tae to use
	blockId uint64
	// use for solving halloween problem
	statementId uint64
	meta        txn.TxnMeta
	op          client.TxnOperator
	// fileMaps used to store the mapping relationship between s3 filenames
	// and blockId
	fileMap map[string]uint64
	// writes cache stores any writes done by txn
	// every statement is an element
	writes   [][]Entry
	dnStores []DNStore
	proc     *process.Process

	idGen IDGenerator

	// interim incremental rowid
	rowId [2]uint64

	// use to cache table
	tableMap *sync.Map
	// use to cache database
	databaseMap *sync.Map

	createTableMap map[uint64]uint8

	deleteMetaTables []string
}

// Entry represents a delete/insert
type Entry struct {
	typ          int
	tableId      uint64
	databaseId   uint64
	tableName    string
	databaseName string
	// blockName for s3 file
	fileName string
	// blockId for s3 file
	blockId uint64
	// update or delete tuples
	bat     *batch.Batch
	dnStore DNStore
}

type transactionHeap []*Transaction

type database struct {
	databaseId   uint64
	databaseName string
	db           *DB
	txn          *Transaction
	fs           fileservice.FileService
}

type tableKey struct {
	accountId  uint32
	databaseId uint64
	name       string
}

type databaseKey struct {
	accountId uint32
	name      string
}

// block list information of table
type tableMeta struct {
	tableName     string
	blocks        [][]BlockMeta
	modifedBlocks [][]ModifyBlockMeta
	defs          []engine.TableDef
}

type table struct {
	tableId    uint64
	tableName  string
	dnList     []int
	db         *database
	meta       *tableMeta
	parts      Partitions
	insertExpr *plan.Expr
	defs       []engine.TableDef
	tableDef   *plan.TableDef
	proc       *process.Process

	primaryIdx int
	viewdef    string
	comment    string
	partition  string
	relKind    string
	createSql  string
}

type column struct {
	accountId  uint32
	tableId    uint64
	databaseId uint64
	// column name
	name            string
	tableName       string
	databaseName    string
	typ             []byte
	typLen          int32
	num             int32
	comment         string
	notNull         int8
	hasDef          int8
	defaultExpr     []byte
	constraintType  string
	isHidden        int8
	isAutoIncrement int8
	hasUpdate       int8
	updateExpr      []byte
}

type blockReader struct {
	blks     []BlockMeta
	ctx      context.Context
	fs       fileservice.FileService
	ts       timestamp.Timestamp
	tableDef *plan.TableDef
}

type blockMergeReader struct {
	sels     []int64
	blks     []ModifyBlockMeta
	ctx      context.Context
	fs       fileservice.FileService
	ts       timestamp.Timestamp
	tableDef *plan.TableDef
}

type mergeReader struct {
	rds []engine.Reader
}

type emptyReader struct {
}

type BlockMeta struct {
	Rows    int64
	Info    catalog.BlockInfo
	Zonemap [][64]byte
}

type ModifyBlockMeta struct {
	meta    BlockMeta
	deletes []int
}

type Columns []column

func (cols Columns) Len() int           { return len(cols) }
func (cols Columns) Swap(i, j int)      { cols[i], cols[j] = cols[j], cols[i] }
func (cols Columns) Less(i, j int) bool { return cols[i].num < cols[j].num }

func (a BlockMeta) Eq(b BlockMeta) bool {
	return a.Info.BlockID == b.Info.BlockID
}
