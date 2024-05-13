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
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/udf"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	PREFETCH_THRESHOLD  = 256
	PREFETCH_ROUNDS     = 24
	SMALLSCAN_THRESHOLD = 100
	LARGESCAN_THRESHOLD = 1500
)

const (
	INSERT = iota
	DELETE
	COMPACTION_CN
	UPDATE
	ALTER
	INSERT_TXN // Only for CN workspace consumption, not sent to DN
	DELETE_TXN // Only for CN workspace consumption, not sent to DN

	MERGEOBJECT
)

var (
	typesNames = map[int]string{
		INSERT:        "insert",
		DELETE:        "delete",
		COMPACTION_CN: "compaction_cn",
		UPDATE:        "update",
		ALTER:         "alter",
		INSERT_TXN:    "insert_txn",
		DELETE_TXN:    "delete_txn",
	}
)

const (
	SMALL = iota
	NORMAL
	LARGE
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

const (
	WorkspaceThreshold uint64 = 1 * mpool.MB
	GCBatchOfFileCount int    = 1000
	GCPoolSize         int    = 5
)

var (
	_ client.Workspace = (*Transaction)(nil)
)

var GcCycle = 10 * time.Second

type DNStore = metadata.TNService

type IDGenerator interface {
	AllocateID(ctx context.Context) (uint64, error)
	// AllocateIDByKey allocate a globally unique ID by key.
	AllocateIDByKey(ctx context.Context, key string) (uint64, error)
}

type Engine struct {
	sync.RWMutex
	mp       *mpool.MPool
	fs       fileservice.FileService
	ls       lockservice.LockService
	qc       qclient.QueryClient
	hakeeper logservice.CNHAKeeperClient
	us       udf.Service
	cli      client.TxnClient
	idGen    IDGenerator
	tnID     string

	//latest catalog will be loaded from TN when engine is initialized.
	catalog *cache.CatalogCache
	//snapshot catalog will be loaded from TN When snapshot read is needed.
	snapCatalog *struct {
		sync.Mutex
		snaps []*cache.CatalogCache
	}
	//latest partitions which be protected by e.Lock().
	partitions map[[2]uint64]*logtailreplay.Partition
	//snapshot partitions
	mu struct {
		sync.Mutex
		snapParts map[[2]uint64]*struct {
			sync.Mutex
			snaps []*logtailreplay.Partition
		}
	}

	packerPool *fileservice.Pool[*types.Packer]

	gcPool *ants.Pool

	// XXX related to cn push model
	pClient PushClient

	// globalStats is the global stats information, which is updated
	// from logtail updates.
	globalStats *GlobalStats
}

// Transaction represents a transaction
type Transaction struct {
	sync.Mutex
	engine *Engine
	// readOnly default value is true, once a write happen, then set to false
	readOnly atomic.Bool
	// db       *DB
	// blockId starts at 0 and keeps incrementing,
	// this is used to name the file on s3 and then give it to tae to use
	// not-used now
	// blockId uint64
	op       client.TxnOperator
	sqlCount atomic.Uint64

	// writes cache stores any writes done by txn
	writes []Entry
	// txn workspace size
	workspaceSize uint64

	// the last snapshot write offset
	snapshotWriteOffset int

	tnStores []DNStore
	proc     *process.Process

	idGen IDGenerator

	// interim incremental rowid
	rowId [6]uint32
	segId types.Uuid
	// use to cache opened snapshot tables by current txn.
	tableCache struct {
		cachedIndex int
		tableMap    *sync.Map
	}
	// use to cache databases created by current txn.
	databaseMap *sync.Map
	// Used to record deleted databases in transactions.
	deletedDatabaseMap *sync.Map
	// use to cache tables created by current txn.
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

	//deletes for uncommitted blocks generated by CN writing S3.
	deletedBlocks *deletedBlocks

	// map uncommitted data block generated by CN writing S3 to block's meta location.
	// notice that it's guarded by txn.Lock() and has the same lifecycle as transaction.
	cnBlkId_Pos map[types.Blockid]Pos
	// committed block belongs to txn's snapshot data -> delta locations for committed block's deletes.
	blockId_tn_delete_metaLoc_batch struct {
		sync.RWMutex
		data map[types.Blockid][]*batch.Batch
	}
	//select list for raw batch comes from txn.writes.batch.
	batchSelectList map[*batch.Batch][]int64
	toFreeBatches   map[tableKey][]*batch.Batch

	rollbackCount int
	//current statement id
	statementID int
	//offsets of the txn.writes for statements in a txn.
	offsets []int
	//for RC isolation, the txn's snapshot TS for each statement.
	timestamps []timestamp.Timestamp

	hasS3Op              atomic.Bool
	removed              bool
	startStatementCalled bool
	incrStatementCalled  bool
	syncCommittedTSCount uint64
	pkCount              int

	adjustCount int
}

type Pos struct {
	bat       *batch.Batch
	accountId uint32
	tbName    string
	dbName    string
	offset    int64
	blkInfo   objectio.BlockInfo
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

func (b *deletedBlocks) hasDeletes(blockID *types.Blockid) bool {
	b.RLock()
	defer b.RUnlock()
	_, ok := b.offsets[*blockID]
	return ok
}

func (b *deletedBlocks) isEmpty() bool {
	b.RLock()
	defer b.RUnlock()
	return len(b.offsets) == 0
}

func (b *deletedBlocks) getDeletedOffsetsByBlock(blockID *types.Blockid, offsets *[]int64) {
	b.RLock()
	defer b.RUnlock()
	res := b.offsets[*blockID]
	*offsets = append(*offsets, res...)
}

func (b *deletedBlocks) clean() {
	b.Lock()
	defer b.Unlock()
	b.offsets = make(map[types.Blockid][]int64)
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

func (txn *Transaction) StartStatement() {
	if txn.startStatementCalled {
		logutil.Fatal("BUG: StartStatement called twice")
	}
	txn.startStatementCalled = true
	txn.incrStatementCalled = false
}

func (txn *Transaction) EndStatement() {
	if !txn.startStatementCalled {
		logutil.Fatal("BUG: StartStatement not called")
	}

	txn.startStatementCalled = false
	txn.incrStatementCalled = false
}

func (txn *Transaction) IncrStatementID(ctx context.Context, commit bool) error {
	if !commit {
		if !txn.startStatementCalled {
			logutil.Fatal("BUG: StartStatement not called")
		}
		if txn.incrStatementCalled {
			logutil.Fatal("BUG: IncrStatementID called twice")
		}
		txn.incrStatementCalled = true
	}

	txn.Lock()
	defer txn.Unlock()
	//free batches
	for key := range txn.toFreeBatches {
		for _, bat := range txn.toFreeBatches[key] {
			txn.proc.PutBatch(bat)
		}
		delete(txn.toFreeBatches, key)
	}
	//merge writes for the last statement
	if err := txn.mergeTxnWorkspaceLocked(); err != nil {
		return err
	}
	// dump batch to s3, starting from 0 (begining of the workspace)
	if err := txn.dumpBatchLocked(0); err != nil {
		return err
	}
	txn.offsets = append(txn.offsets, len(txn.writes))
	txn.statementID++

	return txn.handleRCSnapshot(ctx, commit)
}

// writeOffset returns the offset of the first write in the workspace
func (txn *Transaction) WriteOffset() uint64 {
	txn.Lock()
	defer txn.Unlock()
	return uint64(len(txn.writes))
}

// Adjust adjust writes order after the current statement finished.
func (txn *Transaction) Adjust(writeOffset uint64) error {
	start := time.Now()
	seq := txn.op.NextSequence()
	trace.GetService().AddTxnDurationAction(
		txn.op,
		client.WorkspaceAdjustEvent,
		seq,
		0,
		0,
		nil)
	defer func() {
		trace.GetService().AddTxnDurationAction(
			txn.op,
			client.WorkspaceAdjustEvent,
			seq,
			0,
			time.Since(start),
			nil)
	}()

	txn.Lock()
	defer txn.Unlock()
	if err := txn.adjustUpdateOrderLocked(writeOffset); err != nil {
		return err
	}
	//FIXME:: Do merge the workspace here, is it a must ?
	//        since it has been called already in IncrStatementID.
	if err := txn.mergeTxnWorkspaceLocked(); err != nil {
		return err
	}

	txn.traceWorkspaceLocked(false)
	return nil
}

func (txn *Transaction) traceWorkspaceLocked(commit bool) {
	index := txn.adjustCount
	if commit {
		index = -1
	}
	idx := 0
	trace.GetService().TxnAdjustWorkspace(
		txn.op,
		index,
		func() (tableID uint64, typ string, bat *batch.Batch, more bool) {
			if idx == len(txn.writes) {
				return 0, "", nil, false
			}
			e := txn.writes[idx]
			idx++
			return e.tableId, typesNames[e.typ], e.bat, true
		})
	txn.adjustCount++
}

// The current implementation, update's delete and insert are executed concurrently, inside workspace it
// may be the order of insert+delete that needs to be adjusted.
func (txn *Transaction) adjustUpdateOrderLocked(writeOffset uint64) error {
	if txn.statementID > 0 {
		writes := make([]Entry, 0, len(txn.writes[writeOffset:]))
		for i := writeOffset; i < uint64(len(txn.writes)); i++ {
			if !txn.writes[i].isCatalog() && txn.writes[i].typ == DELETE {
				writes = append(writes, txn.writes[i])
			}
		}
		for i := writeOffset; i < uint64(len(txn.writes)); i++ {
			if txn.writes[i].isCatalog() || txn.writes[i].typ != DELETE {
				writes = append(writes, txn.writes[i])
			}
		}
		txn.writes = append(txn.writes[:writeOffset], writes...)
	}
	return nil
}

func (txn *Transaction) gcObjs(start int) error {
	objsToGC := make(map[string]struct{})
	var objsName []string
	for i := start; i < len(txn.writes); i++ {
		if txn.writes[i].bat == nil {
			continue
		}
		//1. Remove blocks from txn.cnBlkId_Pos lazily till txn commits or rollback.
		//2. Remove the segments generated by this statement lazily till txn commits or rollback.
		//3. Now, GC the s3 objects(data objects and tombstone objects) asynchronously.
		if txn.writes[i].fileName != "" {
			vs := vector.MustStrCol(txn.writes[i].bat.GetVector(0))
			for _, s := range vs {
				loc, _ := blockio.EncodeLocationFromString(s)
				if _, ok := objsToGC[loc.Name().String()]; !ok {
					objsToGC[loc.Name().String()] = struct{}{}
					objsName = append(objsName, loc.Name().String())
				}
			}
		}
	}
	//gc the objects asynchronously.
	//TODO:: to handle the failure when CN is down.
	step := GCBatchOfFileCount
	if len(objsName) > 0 && len(objsName) < step {
		if err := txn.engine.gcPool.Submit(func() {
			if err := txn.engine.fs.Delete(
				context.Background(),
				objsName...); err != nil {
				logutil.Warnf("failed to delete objects:%v, err:%v",
					objsName,
					err)

			}
		}); err != nil {
			return err
		}
		return nil
	}
	for i := 0; i < len(objsName); i += step {
		if i+step > len(objsName) {
			step = len(objsName) - i
		}
		start := i
		end := i + step
		if err := txn.engine.gcPool.Submit(func() {
			//notice that the closure can't capture the loop variable i, so we need to use start and end.
			if err := txn.engine.fs.Delete(
				context.Background(),
				objsName[start:end]...); err != nil {
				logutil.Warnf("failed to delete objects:%v, err:%v",
					objsName[i:i+step],
					err)

			}
		}); err != nil {
			return err
		}
	}
	return nil
}

func (txn *Transaction) RollbackLastStatement(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	txn.rollbackCount++
	if txn.statementID > 0 {
		txn.clearTableCache()
		txn.rollbackCreateTableLocked()

		txn.statementID--
		end := txn.offsets[txn.statementID]
		if err := txn.gcObjs(end); err != nil {
			panic("to gc objects generated by CN failed")
		}
		for i := end; i < len(txn.writes); i++ {
			if txn.writes[i].bat == nil {
				continue
			}
			txn.writes[i].bat.Clean(txn.proc.Mp())
		}
		txn.writes = txn.writes[:end]
		txn.offsets = txn.offsets[:txn.statementID]
		txn.timestamps = txn.timestamps[:txn.statementID]
	}
	// rollback current statement's writes info
	for b := range txn.batchSelectList {
		delete(txn.batchSelectList, b)
	}
	//rollback the deleted blocks for the current statement.
	txn.deletedBlocks.clean()

	// current statement has been rolled back, make can call IncrStatementID again.
	txn.incrStatementCalled = false
	return nil
}
func (txn *Transaction) resetSnapshot() error {
	txn.tableCache.tableMap.Range(func(key, value interface{}) bool {
		value.(*txnTable).resetSnapshot()
		return true
	})
	return nil
}

func (txn *Transaction) IncrSQLCount() {
	n := txn.sqlCount.Add(1)
	v2.TxnLifeCycleStatementsTotalHistogram.Observe(float64(n))
}

func (txn *Transaction) GetSQLCount() uint64 {
	return txn.sqlCount.Load()
}

// For RC isolation, update the snapshot TS of transaction for each statement.
// only 2 cases need to reset snapshot
// 1. cn sync latest commit ts from mo_ctl
// 2. not first sql
func (txn *Transaction) handleRCSnapshot(ctx context.Context, commit bool) error {
	needResetSnapshot := false
	newTimes := txn.proc.TxnClient.GetSyncLatestCommitTSTimes()
	if newTimes > txn.syncCommittedTSCount {
		txn.syncCommittedTSCount = newTimes
		needResetSnapshot = true
	}
	if !commit && txn.op.Txn().IsRCIsolation() &&
		(txn.GetSQLCount() > 0 || needResetSnapshot) {
		trace.GetService().TxnUpdateSnapshot(
			txn.op,
			0,
			"before execute")
		if err := txn.op.UpdateSnapshot(
			ctx,
			timestamp.Timestamp{}); err != nil {
			return err
		}
		txn.resetSnapshot()
	}
	//Transfer row ids for deletes in RC isolation
	if !commit {
		return txn.transferDeletesLocked()
	}
	return nil
}

// Entry represents a delete/insert
type Entry struct {
	typ int
	//the tenant owns the tableId and databaseId
	accountId    uint32
	tableId      uint64
	databaseId   uint64
	tableName    string
	databaseName string
	// blockName for s3 file
	fileName string
	//tuples would be applied to the table which belongs to the tenant(accountId)
	bat       *batch.Batch
	tnStore   DNStore
	pkChkByTN int8
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

// isCatalog denotes the entry is apply the tree tables
func (e *Entry) isCatalog() bool {
	return e.tableId == catalog.MO_TABLES_ID ||
		e.tableId == catalog.MO_COLUMNS_ID ||
		e.tableId == catalog.MO_DATABASE_ID
}

// txnDatabase represents an opened database in a transaction
type txnDatabase struct {
	databaseId        uint64
	databaseName      string
	databaseType      string
	databaseCreateSql string
	//txn               *Transaction
	op client.TxnOperator

	rowId types.Rowid
}

type tableKey struct {
	accountId  uint32
	databaseId uint64
	dbName     string
	name       string
}

type databaseKey struct {
	accountId uint32
	id        uint64
	name      string
}

// txnTable represents an opened table in a transaction
type txnTable struct {
	sync.Mutex

	accountId uint32
	tableId   uint64
	version   uint32
	tableName string
	db        *txnDatabase
	//	insertExpr *plan.Expr
	defs       []engine.TableDef
	tableDef   *plan.TableDef
	seqnums    []uint16
	typs       []types.Type
	_partState atomic.Pointer[logtailreplay.PartitionState]
	// specify whether the logtail is updated. once it is updated, it will not be updated again
	logtailUpdated atomic.Bool

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

	// timestamp of the last operation on this table
	lastTS timestamp.Timestamp

	// this should be the statement id
	// but seems that we're not maintaining it at the moment
	// localTS timestamp.Timestamp
	//rowid in mo_tables
	rowid types.Rowid
	//rowids in mo_columns
	rowids []types.Rowid
	//the old table id before truncate
	oldTableId uint64

	// process for statement
	//proc *process.Process
	proc atomic.Pointer[process.Process]

	createByStatementID int
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
	enumValues      string
}

type withFilterMixin struct {
	ctx      context.Context
	fs       fileservice.FileService
	ts       timestamp.Timestamp
	proc     *process.Process
	tableDef *plan.TableDef

	// columns used for reading
	columns struct {
		seqnums  []uint16
		colTypes []types.Type
		// colNulls []bool

		compPKPositions []uint16 // composite primary key pos in the columns

		pkPos    int // -1 means no primary key in columns
		rowidPos int // -1 means no rowid in columns

		indexOfFirstSortedColumn int
	}

	filterState struct {
		evaluated bool
		//point select for primary key
		expr     *plan.Expr
		filter   blockio.ReadFilter
		seqnums  []uint16 // seqnums of the columns in the filter
		colTypes []types.Type
		hasNull  bool
	}

	sels []int32
}

type blockSortHelper struct {
	blk *objectio.BlockInfo
	zm  index.ZM
}

type blockReader struct {
	withFilterMixin

	// used for prefetch
	dontPrefetch bool
	infos        [][]*objectio.BlockInfo
	steps        []int
	currentStep  int

	scanType int
	// block list to scan
	blks []*objectio.BlockInfo
	//buffer for block's deletes
	buffer []int64

	// for ordered scan
	desc     bool
	blockZMS []index.ZM
	OrderBy  []*plan.OrderBySpec
	sorted   bool // blks need to be sorted by zonemap
	filterZM objectio.ZoneMap
}

type blockMergeReader struct {
	*blockReader
	table *txnTable

	encodedPrimaryKey []byte

	//for perfetch deletes
	loaded     bool
	pkidx      int
	deletaLocs map[string][]objectio.Location
}

type mergeReader struct {
	rds []engine.Reader
}

type emptyReader struct {
}
