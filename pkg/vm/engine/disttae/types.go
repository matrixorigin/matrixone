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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

type DNStore = metadata.DNService

type IDGenerator interface {
	AllocateID(ctx context.Context) (uint64, error)
	// AllocateIDByKey allocate a globally unique ID by key.
	AllocateIDByKey(ctx context.Context, key string) (uint64, error)
}

type Engine struct {
	sync.RWMutex
	mp         *mpool.MPool
	fs         fileservice.FileService
	cli        client.TxnClient
	idGen      IDGenerator
	txns       map[string]*Transaction
	catalog    *cache.CatalogCache
	dnMap      map[string]int
	partitions map[[2]uint64]Partitions
	packerPool *fileservice.Pool[*types.Packer]

	// XXX related to cn push model
	usePushModel       bool
	subscriber         *logTailSubscriber
	receiveLogTailTime syncLogTailTimestamp
	subscribed         subscribedTable
}

type Partitions []*Partition

// a partition corresponds to a dn
type Partition struct {
	lock  chan struct{}
	state atomic.Pointer[PartitionState]
	ts    timestamp.Timestamp // last updated timestamp
}

// Transaction represents a transaction
type Transaction struct {
	sync.Mutex
	engine *Engine
	// readOnly default value is true, once a write happen, then set to false
	readOnly bool
	// db       *DB
	// blockId starts at 0 and keeps incrementing,
	// this is used to name the file on s3 and then give it to tae to use
	// not-used now
	// blockId uint64

	// local timestamp for workspace operations
	meta txn.TxnMeta
	op   client.TxnOperator
	// fileMaps used to store the mapping relationship between s3 filenames
	// and blockId
	fileMap map[string]uint64
	// writes cache stores any writes done by txn
	writes []Entry
	// txn workspace size
	workspaceSize uint64

	dnStores []DNStore
	proc     *process.Process

	idGen IDGenerator

	// interim incremental rowid
	rowId [2]uint64

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
	bat       *batch.Batch
	dnStore   DNStore
	pkChkByDN int8
}

// txnDatabase represents an opened database in a transaction
type txnDatabase struct {
	databaseId        uint64
	databaseName      string
	databaseType      string
	databaseCreateSql string
	txn               *Transaction
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

// txnTable represents an opened table in a transaction
type txnTable struct {
	tableId   uint64
	tableName string
	dnList    []int
	db        *txnDatabase
	meta      *tableMeta
	parts     []*PartitionState
	//	insertExpr *plan.Expr
	defs     []engine.TableDef
	tableDef *plan.TableDef

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
	// snapshot for read
	writes []Entry
	// offset of the writes in workspace
	writesOffset int
	skipBlocks   map[types.Blockid]uint8

	// localState stores uncommitted data
	localState *PartitionState
	// this should be the statement id
	// but seems that we're not maintaining it at the moment
	localTS timestamp.Timestamp
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

type pkRange struct {
	isRange bool
	items   []int64
	ranges  []int64
}
