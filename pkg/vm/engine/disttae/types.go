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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memorytable"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	GcCycle = 10 * time.Second
)

const (
	INSERT = iota
	DELETE
	UPDATE
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
	NewReader(ctx context.Context, readerNumber int, index memtable.Tuple, defs []engine.TableDef,
		tableDef *plan.TableDef, skipBlocks map[uint64]uint8, blks []ModifyBlockMeta,
		ts timestamp.Timestamp, fs fileservice.FileService, entries []Entry) ([]engine.Reader, error)
}

type Engine struct {
	sync.RWMutex
	mp                *mpool.MPool
	fs                fileservice.FileService
	db                *DB
	cli               client.TxnClient
	idGen             IDGenerator
	getClusterDetails engine.GetClusterDetailsFunc
	txns              map[string]*Transaction
	catalog           *cache.CatalogCache
	// minimum heap of currently active transactions
	txnHeap *transactionHeap
}

// DB is implementataion of cache
type DB struct {
	sync.RWMutex
	dnMap      map[string]int
	metaTables map[string]Partitions
	partitions map[[2]uint64]Partitions
}

type Partitions []*Partition

// a partition corresponds to a dn
type Partition struct {
	lock chan struct{}
	// multi-version data of logtail, implemented with reusee's memengine
	data             *memtable.Table[RowID, DataValue, *DataRow]
	columnsIndexDefs []ColumnsIndexDef
	// last updated timestamp
	ts timestamp.Timestamp
	// used for block read in PartitionReader
	txn *Transaction
}

// Transaction represents a transaction
type Transaction struct {
	sync.Mutex
	db *DB
	// readOnly default value is true, once a write happen, then set to false
	readOnly bool
	// db       *DB
	// blockId starts at 0 and keeps incrementing,
	// this is used to name the file on s3 and then give it to tae to use
	// not-used now
	// blockId uint64

	// use for solving halloween problem
	statementId uint64
	// local timestamp for workspace operations
	localTS timestamp.Timestamp
	meta    txn.TxnMeta
	op      client.TxnOperator
	// fileMaps used to store the mapping relationship between s3 filenames
	// and blockId
	fileMap map[string]uint64
	// writes cache stores any writes done by txn
	// every statement is an element
	writes    [][]Entry
	workspace *memorytable.Table[RowID, *workspaceRow, *workspaceRow]
	dnStores  []DNStore
	proc      *process.Process

	idGen IDGenerator

	// interim incremental rowid
	rowId [2]uint64

	catalog *cache.CatalogCache

	// use to cache table
	tableMap *sync.Map
	// use to cache database
	databaseMap *sync.Map
	// use to cache created table
	createMap *sync.Map
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
	tableId    uint64
	name       string
}

type databaseKey struct {
	accountId uint32
	id        uint64
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

	primaryIdx   int // -1 means no primary key
	clusterByIdx int // -1 means no clusterBy key
	viewdef      string
	comment      string
	partition    string
	relKind      string
	createSql    string
	constraint   []byte

	updated bool
	// use for skip rows
	skipBlocks map[uint64]uint8
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
	isClusterBy     int8
	isHidden        int8
	isAutoIncrement int8
	hasUpdate       int8
	updateExpr      []byte
}

type blockReader struct {
	blks       []BlockMeta
	ctx        context.Context
	fs         fileservice.FileService
	ts         timestamp.Timestamp
	tableDef   *plan.TableDef
	primaryIdx int
	expr       *plan.Expr

	// cached meta data.
	colIdxs        []uint16
	colTypes       []types.Type
	colNulls       []bool
	pkidxInColIdxs int
	pkName         string
}

type blockMergeReader struct {
	sels     []int64
	blks     []ModifyBlockMeta
	ctx      context.Context
	fs       fileservice.FileService
	ts       timestamp.Timestamp
	tableDef *plan.TableDef

	// cached meta data.
	colIdxs  []uint16
	colTypes []types.Type
	colNulls []bool
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

type workspaceRow struct {
	rowID   RowID
	tableID uint64
	indexes []memtable.Tuple
}

var _ memtable.Row[RowID, *workspaceRow] = new(workspaceRow)

func (w *workspaceRow) Key() RowID {
	return w.rowID
}

func (w *workspaceRow) Value() *workspaceRow {
	return w
}

func (w *workspaceRow) Indexes() []memtable.Tuple {
	return w.indexes
}

func (w *workspaceRow) UniqueIndexes() []memtable.Tuple {
	return nil
}

type pkRange struct {
	isRange bool
	items   []int64
	ranges  []int64
}
