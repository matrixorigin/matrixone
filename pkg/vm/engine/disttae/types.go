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
	"math"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	PREFETCH_THRESHOLD = 512
	PREFETCH_ROUNDS    = 32
)

const (
	INSERT = iota
	DELETE
	COMPACTION_CN
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
	INIT_ROWID_OFFSET             = math.MaxUint32
)

var GcCycle = 10 * time.Second

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
	catalog    *cache.CatalogCache
	dnID       string
	partitions map[[2]uint64]*logtailreplay.Partition
	packerPool *fileservice.Pool[*types.Packer]

	// XXX related to cn push model
	pClient pushClient
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

	// writes cache stores any writes done by txn
	writes []Entry
	// txn workspace size
	workspaceSize uint64

	dnStores []DNStore
	proc     *process.Process

	idGen IDGenerator

	// interim incremental rowid
	rowId [6]uint32
	segId types.Uuid
	// use to cache table
	tableMap *sync.Map
	// use to cache database
	databaseMap *sync.Map
	// use to cache created table
	createMap *sync.Map
	/*
		for deleted table
		CORNER CASE
		create table t1(a int);
		begin;
		drop table t1; // t1 does not exist
		select * from t1; // can not access t1
		create table t2(a int); // new table
		drop table t2;
		create table t2(a int,b int); //new table
		commit;
		show tables; //no table t1; has table t2(a,b)
	*/
	deletedTableMap *sync.Map

	deletedBlocks *deletedBlocks
	// blkId -> Pos
	cnBlkId_Pos                     map[types.Blockid]Pos
	blockId_raw_batch               map[types.Blockid]*batch.Batch
	blockId_dn_delete_metaLoc_batch map[types.Blockid][]*batch.Batch
}

type Pos struct {
	idx    int
	offset int64
}

// FIXME: The map inside this one will be accessed concurrently, using
// a mutex, not sure if there will be performance issues
type deletedBlocks struct {
	sync.RWMutex

	// used to store cn block's deleted rows
	// blockId => deletedOffsets
	offsets map[types.Blockid][]int64
}

func (b *deletedBlocks) addDeletedBlocks(blockID *types.Blockid, offsets []int64) {
	b.Lock()
	defer b.Unlock()
	b.offsets[*blockID] = append(b.offsets[*blockID], offsets...)
}

func (b *deletedBlocks) getDeletedOffsetsByBlock(blockID *types.Blockid) []int64 {
	b.RLock()
	defer b.RUnlock()
	res := b.offsets[*blockID]
	offsets := make([]int64, len(res))
	copy(offsets, res)
	return offsets
}

func (b *deletedBlocks) removeBlockDeletedInfos(ids []*types.Blockid) {
	b.Lock()
	defer b.Unlock()
	for _, id := range ids {
		delete(b.offsets, *id)
	}
}

func (b *deletedBlocks) iter(fn func(*types.Blockid, []int64) bool) {
	b.RLock()
	defer b.RUnlock()
	for id, offsets := range b.offsets {
		if !fn(&id, offsets) {
			return
		}
	}
}

func (txn *Transaction) PutCnBlockDeletes(blockId *types.Blockid, offsets []int64) {
	txn.deletedBlocks.addDeletedBlocks(blockId, offsets)
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
	/*
		if truncate is true,it denotes the Entry with typ DELETE
		on mo_tables is generated by the Truncate operation.
	*/
	truncate bool
}

// isGeneratedByTruncate denotes the entry is yielded by the truncate operation.
func (e *Entry) isGeneratedByTruncate() bool {
	return e.typ == DELETE &&
		e.databaseId == catalog.MO_CATALOG_ID &&
		e.tableId == catalog.MO_TABLES_ID &&
		e.truncate
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

// txnTable represents an opened table in a transaction
type txnTable struct {
	tableId   uint64
	version   uint32
	tableName string
	dnList    []int
	db        *txnDatabase
	//	insertExpr *plan.Expr
	defs           []engine.TableDef
	tableDef       *plan.TableDef
	seqnums        []uint16
	typs           []types.Type
	_partState     *logtailreplay.PartitionState
	modifiedBlocks []ModifyBlockMeta

	// blockInfos stores all the block infos for this table of this transaction
	// it is only generated when the table is not created by this transaction
	// it is initialized by updateBlockInfos and once it is initialized, it will not be updated
	blockInfos []catalog.BlockInfo

	// specify whether the blockInfos is updated. once it is updated, it will not be updated again
	blockInfosUpdated bool
	// specify whether the logtail is updated. once it is updated, it will not be updated again
	logtailUpdated bool

	primaryIdx    int // -1 means no primary key
	primarySeqnum int // -1 means no primary key
	clusterByIdx  int // -1 means no clusterBy key
	viewdef       string
	comment       string
	partitioned   int8   //1 : the table has partitions ; 0 : no partition
	partition     string // the info about partitions when the table has partitions
	relKind       string
	createSql     string
	constraint    []byte

	// use for skip rows
	// snapshot for read
	writes []Entry
	// offset of the writes in workspace
	writesOffset int

	// localState stores uncommitted data
	localState *logtailreplay.PartitionState
	// this should be the statement id
	// but seems that we're not maintaining it at the moment
	localTS timestamp.Timestamp
	//rowid in mo_tables
	rowid types.Rowid
	//rowids in mo_columns
	rowids []types.Rowid
	//the old table id before truncate
	oldTableId uint64
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
	seqnum          uint16
}

type blockReader struct {
	blks          []*catalog.BlockInfo
	ctx           context.Context
	fs            fileservice.FileService
	ts            timestamp.Timestamp
	tableDef      *plan.TableDef
	primarySeqnum int
	expr          *plan.Expr

	//used for prefetch
	infos           [][]*catalog.BlockInfo
	steps           []int
	currentStep     int
	prefetchColIdxs []uint16 //need to remove rowid

	// cached meta data.
	seqnums        []uint16
	colTypes       []types.Type
	colNulls       []bool
	pkidxInColIdxs int
	pkName         string
	// binary search info
	init       bool
	canCompute bool
	searchFunc func(*vector.Vector) int
}

type blockMergeReader struct {
	sels     []int64
	blks     []ModifyBlockMeta
	ctx      context.Context
	fs       fileservice.FileService
	ts       timestamp.Timestamp
	tableDef *plan.TableDef

	// cached meta data.
	seqnums  []uint16
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
	Zonemap []Zonemap
}

func (a *BlockMeta) MarshalBinary() ([]byte, error) {
	return a.Marshal()
}

func (a *BlockMeta) UnmarshalBinary(data []byte) error {
	return a.Unmarshal(data)
}

type Zonemap [64]byte

func (z *Zonemap) ProtoSize() int {
	return 64
}

func (z *Zonemap) MarshalToSizedBuffer(data []byte) (int, error) {
	if len(data) < z.ProtoSize() {
		panic("invalid byte slice")
	}
	n := copy(data, z[:])
	return n, nil
}

func (z *Zonemap) MarshalTo(data []byte) (int, error) {
	size := z.ProtoSize()
	return z.MarshalToSizedBuffer(data[:size])
}

func (z *Zonemap) Marshal() ([]byte, error) {
	data := make([]byte, z.ProtoSize())
	n, err := z.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (z *Zonemap) Unmarshal(data []byte) error {
	if len(data) < z.ProtoSize() {
		panic("invalid byte slice")
	}
	copy(z[:], data)
	return nil
}

type ModifyBlockMeta struct {
	meta    catalog.BlockInfo
	deletes []int64
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
