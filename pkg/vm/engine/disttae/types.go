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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/udf"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

const (
	PREFETCH_THRESHOLD  = 256
	PREFETCH_ROUNDS     = 24
	SMALLSCAN_THRESHOLD = 150
	LARGESCAN_THRESHOLD = 1500
)

const (
	INSERT = iota
	DELETE
	ALTER // alter command for TN. Update batches for mo_tables and mo_columns will fall into the category of INSERT and DELETE.
)

type NoteLevel string

const (
	DATABASE NoteLevel = "database"
	TABLE    NoteLevel = "table"
	COLUMN   NoteLevel = "column"
)

var (
	typesNames = map[int]string{
		INSERT: "insert",
		DELETE: "delete",
		ALTER:  "alter",
	}
)

func noteForCreate(id uint64, name string) string {
	return fmt.Sprintf("create-%v-%v", id, name)
}

func noteForDrop(id uint64, name string) string {
	return fmt.Sprintf("drop-%v-%v", id, name)
}

func noteForAlterDel(tid uint64, name string) string {
	return fmt.Sprintf("alter-d-%v-%v", tid, name)
}

func noteForAlterIns(tid uint64, name string) string {
	return fmt.Sprintf("alter-i-%v-%v", tid, name)
}

func noteSplitAlter(note string) (bool, int, uint64, string) {
	if len(note) < 6 || note[:5] != "alter" {
		return false, 0, 0, ""
	}
	typ := INSERT
	if note[6] == 'd' {
		typ = DELETE
	}
	for i := 8; i < len(note); i++ {
		if note[i] == '-' {
			id, _ := strconv.ParseUint(note[8:i], 10, 64)
			return true, typ, id, note[i+1:]
		}
	}
	panic("bad format of alter note")
}

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
	CommitWorkspaceThreshold       uint64 = 1 * mpool.MB
	WriteWorkspaceThreshold        uint64 = 5 * mpool.MB
	ExtraWorkspaceThreshold        uint64 = 500 * mpool.MB
	InsertEntryThreshold                  = 5000
	GCBatchOfFileCount             int    = 1000
	GCPoolSize                     int    = 5
	CNTransferTxnLifespanThreshold        = time.Second * 5
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

type EngineOptions func(*Engine)

func WithCommitWorkspaceThreshold(th uint64) EngineOptions {
	return func(e *Engine) {
		e.config.commitWorkspaceThreshold = th
	}
}

func WithWriteWorkspaceThreshold(th uint64) EngineOptions {
	return func(e *Engine) {
		e.config.writeWorkspaceThreshold = th
	}
}

func WithExtraWorkspaceThresholdQuota(quota uint64) EngineOptions {
	return func(e *Engine) {
		e.config.quota.Store(quota)
	}
}

func WithInsertEntryMaxCount(th int) EngineOptions {
	return func(e *Engine) {
		e.config.insertEntryMaxCount = th
	}
}

func WithCNTransferTxnLifespanThreshold(th time.Duration) EngineOptions {
	return func(e *Engine) {
		e.config.cnTransferTxnLifespanThreshold = th
	}
}

func WithSQLExecFunc(f func() ie.InternalExecutor) EngineOptions {
	return func(e *Engine) {
		e.config.ieFactory = f
	}
}

func WithMoTableStatsConf(conf MoTableStatsConfig) EngineOptions {
	return func(e *Engine) {
		e.config.statsConf = conf
	}
}

func WithMoServerStateChecker(checker func() bool) EngineOptions {
	return func(e *Engine) {
		e.config.moServerStateChecker = checker
	}
}

type Engine struct {
	sync.RWMutex
	service  string
	mp       *mpool.MPool
	fs       fileservice.FileService
	ls       lockservice.LockService
	qc       qclient.QueryClient
	hakeeper logservice.CNHAKeeperClient
	us       udf.Service
	cli      client.TxnClient
	idGen    IDGenerator
	tnID     string

	config struct {
		insertEntryMaxCount      int
		commitWorkspaceThreshold uint64
		writeWorkspaceThreshold  uint64
		extraWorkspaceThreshold  uint64
		quota                    atomic.Uint64

		cnTransferTxnLifespanThreshold time.Duration

		ieFactory            func() ie.InternalExecutor
		statsConf            MoTableStatsConfig
		moServerStateChecker func() bool
	}

	//latest catalog will be loaded from TN when engine is initialized.
	catalog atomic.Pointer[cache.CatalogCache]
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

	//for message on multiCN, use uuid to get the messageBoard
	messageCenter *message.MessageCenter

	timeFixed             bool
	moCatalogCreatedTime  *vector.Vector
	moDatabaseCreatedTime *vector.Vector
	moTablesCreatedTime   *vector.Vector
	moColumnsCreatedTime  *vector.Vector

	dynamicCtx
	// for test only.
	skipConsume bool
}

func (e *Engine) SetService(svr string) {
	e.service = svr
}

func (txn *Transaction) String() string {
	return fmt.Sprintf("writes %v", txn.writes)
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
	// txn workspace size, includes in memory entries and persisted entries.
	workspaceSize uint64
	// the approximation of total size for insert entries
	approximateInMemInsertSize uint64
	// the approximation of total row count for insert entries
	approximateInMemInsertCnt int
	// the approximation of total row count for delete entries
	approximateInMemDeleteCnt int
	// the last snapshot write offset
	snapshotWriteOffset int

	tnStores []DNStore
	proc     *process.Process

	idGen IDGenerator

	// interim incremental rowid
	rowId [6]uint32
	segId types.Uuid
	// use to cache opened snapshot tables by current txn.
	tableCache *sync.Map

	// used to keep updated tables in the current txn
	tableOps            *tableOpsChain
	databaseOps         *dbOpsChain
	restoreTxnTableFunc []func()

	// record the table dropped in the txn,
	// for filtering effortless inserts and deletes before committing.
	// tableid -> statementId
	tablesInVain map[uint64]int

	//deletes for uncommitted blocks generated by CN writing S3.
	deletedBlocks *deletedBlocks

	// map uncommitted data block generated by CN writing S3 to block's meta location.
	// notice that it's guarded by txn.Lock() and has the same lifecycle as transaction.
	cnBlkId_Pos map[types.Blockid]Pos
	// committed block belongs to txn's snapshot data -> delta locations for committed block's deletes.
	cn_flushed_s3_tombstone_object_stats_list *sync.Map
	//cn_flushed_s3_tombstone_object_stats_list struct {
	//	sync.RWMutex
	//	data []objectio.ObjectStats
	//}
	//select list for raw batch comes from txn.writes.batch.
	batchSelectList map[*batch.Batch][]int64

	rollbackCount int
	//current statement id
	statementID int
	//offsets of the txn.writes for statements in a txn.
	offsets []int
	//for RC isolation, the txn's snapshot TS for each statement.

	transfer struct {
		lastTransferred types.TS
		timestamps      []timestamp.Timestamp
		pendingTransfer bool

		//workerPool *ants.Pool
	}

	//the start time of first statement in a txn.
	start time.Time

	hasS3Op              atomic.Bool
	removed              bool
	startStatementCalled bool
	incrStatementCalled  bool
	syncCommittedTSCount uint64
	pkCount              int

	adjustCount int

	haveDDL atomic.Bool

	writeWorkspaceThreshold      uint64
	commitWorkspaceThreshold     uint64
	extraWriteWorkspaceThreshold uint64 // acquired from engine quota
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

func (b *deletedBlocks) getDeletedRowIDs(appendTo func(row *types.Rowid)) {
	b.RLock()
	defer b.RUnlock()
	for bid, offsets := range b.offsets {
		for _, offset := range offsets {
			rowId := types.NewRowid(&bid, uint32(offset))
			appendTo(rowId)
		}
	}
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

func NewTxnWorkSpace(eng *Engine, proc *process.Process) *Transaction {
	id := objectio.NewSegmentid()
	bytes := types.EncodeUuid(id)
	txn := &Transaction{
		proc:         proc,
		engine:       eng,
		idGen:        eng.idGen,
		tnStores:     eng.GetTNServices(),
		tableCache:   new(sync.Map),
		databaseOps:  newDbOps(),
		tableOps:     newTableOps(),
		tablesInVain: make(map[uint64]int),
		rowId: [6]uint32{
			types.DecodeUint32(bytes[0:4]),
			types.DecodeUint32(bytes[4:8]),
			types.DecodeUint32(bytes[8:12]),
			types.DecodeUint32(bytes[12:16]),
			0,
			0,
		},
		segId: *id,
		deletedBlocks: &deletedBlocks{
			offsets: map[types.Blockid][]int64{},
		},
		cnBlkId_Pos:          map[types.Blockid]Pos{},
		batchSelectList:      make(map[*batch.Batch][]int64),
		syncCommittedTSCount: eng.cli.GetSyncLatestCommitTSTimes(),
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),

		commitWorkspaceThreshold: eng.config.commitWorkspaceThreshold,
		writeWorkspaceThreshold:  eng.config.writeWorkspaceThreshold,
	}

	//txn.transfer.workerPool, _ = ants.NewPool(min(runtime.NumCPU(), 4))

	txn.readOnly.Store(true)
	// transaction's local segment for raw batch.
	colexec.Get().PutCnSegment(id, colexec.TxnWorkSpaceIdType)
	return txn
}

func (txn *Transaction) PutCnBlockDeletes(blockId *types.Blockid, offsets []int64) {
	txn.deletedBlocks.addDeletedBlocks(blockId, offsets)
}

func (txn *Transaction) StashFlushedTombstones(stats objectio.ObjectStats) {
	txn.cn_flushed_s3_tombstone_object_stats_list.Store(stats, nil)
}

func (txn *Transaction) Readonly() bool {
	return txn.readOnly.Load()
}

func (txn *Transaction) PPString() string {

	writesString := stringifySlice(txn.writes, func(a any) string {
		ent := a.(Entry)
		return ent.String()
	})
	buf := &bytes.Buffer{}

	stringifySyncMap := func(m *sync.Map) string {
		buf.Reset()
		i := 0
		buf.WriteRune('{')
		m.Range(func(key, _ interface{}) bool {
			if i > 0 {
				buf.WriteString(",")
			}
			k := key.(tableKey)
			buf.WriteString(k.String())
			i++
			return true
		})
		buf.WriteRune('}')
		buf.WriteString(fmt.Sprintf("[%d]", i))
		return buf.String()
	}

	return fmt.Sprintf("Transaction{writes: %v, batchSelectList: %v, tableOps:%v, tablesInVain: %v,  tableCache: %v, insertCount: %v, snapshotWriteOffset: %v, rollbackCount: %v, statementID: %v, offsets: %v, timestamps: %v}",
		writesString,
		stringifyMap(txn.batchSelectList, func(k, v any) string {
			return fmt.Sprintf("%p:%v", k, len(v.([]int64)))
		}),
		txn.tableOps.string(),
		stringifyMap(txn.tablesInVain, func(k, v any) string {
			return fmt.Sprintf("%v:%v", k.(uint64), v.(int))
		}),
		stringifySyncMap(txn.tableCache),
		txn.approximateInMemInsertCnt,
		txn.snapshotWriteOffset,
		txn.rollbackCount,
		txn.statementID,
		stringifySlice(txn.offsets, func(a any) string { return fmt.Sprintf("%v", a) }),
		stringifySlice(txn.transfer.timestamps, func(a any) string { t := a.(timestamp.Timestamp); return t.DebugString() }))
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
	txn.op.EnterIncrStmt()
	defer txn.op.ExitIncrStmt()
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
	//merge writes for the last statement
	if err := txn.mergeTxnWorkspaceLocked(ctx); err != nil {
		return err
	}
	// dump batch to s3, starting from 0 (begining of the workspace)
	if err := txn.dumpBatchLocked(ctx, 0); err != nil {
		return err
	}
	txn.offsets = append(txn.offsets, len(txn.writes))

	txn.statementID++

	if txn.op.Txn().IsRCIsolation() {
		// each statement's start snapshot
		// will be used by transfer than between statements
		txn.transfer.timestamps = append(txn.transfer.timestamps, txn.op.SnapshotTS())

		if txn.transfer.lastTransferred.IsEmpty() {
			txn.start = time.Now()
			txn.transfer.lastTransferred = types.TimestampToTS(txn.transfer.timestamps[0])
		}

		updated, err := txn.handleRCSnapshot(ctx, commit)
		if err != nil {
			return err
		}

		return txn.transferTombstonesByStatement(ctx, updated, commit)
	}

	return nil
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
	trace.GetService(txn.proc.GetService()).AddTxnDurationAction(
		txn.op,
		client.WorkspaceAdjustEvent,
		seq,
		0,
		0,
		nil)
	defer func() {
		trace.GetService(txn.proc.GetService()).AddTxnDurationAction(
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

	// DO NOT merge workspace here, branch-out internal sql, like ones reading mo-tables,
	// have not rights to confirm the status of the workspace.
	// if err := txn.mergeTxnWorkspaceLocked(); err != nil {
	// 	return err
	// }

	txn.traceWorkspaceLocked(false)
	return nil
}

func (txn *Transaction) traceWorkspaceLocked(commit bool) {
	index := txn.adjustCount
	if commit {
		index = -1
	}
	idx := 0
	trace.GetService(txn.proc.GetService()).TxnAdjustWorkspace(
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
	var objsName []string
	for i := start; i < len(txn.writes); i++ {
		if txn.writes[i].bat == nil ||
			txn.writes[i].bat.RowCount() == 0 {
			continue
		}
		//1. Remove blocks from txn.cnBlkId_Pos lazily till txn commits or rollback.
		//2. Remove the segments generated by this statement lazily till txn commits or rollback.
		//3. Now, GC the s3 objects(data objects and tombstone objects) asynchronously.
		if txn.writes[i].fileName != "" {
			var vec *vector.Vector
			//  [object_stats, pk]
			if txn.writes[i].typ == DELETE {
				vec = txn.writes[i].bat.Vecs[0]
			} else {
				// [%!%mo__meta_loc, object_stats]
				vec = txn.writes[i].bat.Vecs[1]
			}

			for j := range vec.Length() {
				ss := objectio.ObjectStats(vec.GetBytesAt(j))
				if txn.writes[i].typ == DELETE {
					txn.cn_flushed_s3_tombstone_object_stats_list.Delete(ss)
				}
				objsName = append(objsName, ss.ObjectName().String())
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
	txn.op.EnterRollbackStmt()
	defer txn.op.ExitRollbackStmt()
	var (
		beforeEntries int
		afterEntries  int
	)
	defer func() {
		logutil.Info(
			"RollbackLastStatement",
			zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
			zap.Int("before", beforeEntries),
			zap.Int("after", afterEntries),
		)
	}()
	txn.Lock()
	defer txn.Unlock()

	beforeEntries = len(txn.writes)

	txn.rollbackCount++
	if txn.statementID > 0 {
		txn.clearTableCache()
		txn.rollbackTableOpLocked()

		txn.statementID--
		end := txn.offsets[txn.statementID]
		if err := txn.gcObjs(end); err != nil {
			panic("to gc objects generated by CN failed")
		}
		for i := end; i < len(txn.writes); i++ {
			if txn.writes[i].bat == nil {
				continue
			}
			txn.workspaceSize -= uint64(txn.writes[i].bat.Size())
			txn.writes[i].bat.Clean(txn.proc.Mp())
		}
		txn.writes = txn.writes[:end]
		txn.offsets = txn.offsets[:txn.statementID]

		// transfer stuff
		if txn.op.Txn().IsRCIsolation() {
			txn.transfer.timestamps = txn.transfer.timestamps[:txn.statementID]

			if txn.statementID == 0 {
				txn.transfer.pendingTransfer = false
				txn.transfer.lastTransferred = types.TS{}
			} else if txn.transfer.timestamps[txn.statementID-1].Less(txn.transfer.lastTransferred.ToTimestamp()) {
				txn.transfer.lastTransferred = types.TimestampToTS(txn.transfer.timestamps[txn.statementID-1])
			}
		}
	}
	// rollback current statement's writes info
	for b := range txn.batchSelectList {
		delete(txn.batchSelectList, b)
	}

	afterEntries = len(txn.writes)

	for i := len(txn.restoreTxnTableFunc) - 1; i >= 0; i-- {
		txn.restoreTxnTableFunc[i]()
	}
	txn.restoreTxnTableFunc = txn.restoreTxnTableFunc[:0]
	//rollback the deleted blocks for the current statement.
	txn.deletedBlocks.clean()

	// current statement has been rolled back, make can call IncrStatementID again.
	txn.incrStatementCalled = false
	return nil
}
func (txn *Transaction) resetSnapshot() error {
	txn.tableCache.Range(func(key, value interface{}) bool {
		value.(*txnTableDelegate).origin.resetSnapshot()
		return true
	})
	return nil
}

func (txn *Transaction) IncrSQLCount() {
	n := txn.sqlCount.Add(1)
	v2.TxnLifeCycleStatementsTotalHistogram.Observe(float64(n))
}

func (txn *Transaction) GetProc() *process.Process {
	return txn.proc
}

func (txn *Transaction) GetSQLCount() uint64 {
	return txn.sqlCount.Load()
}

func (txn *Transaction) advanceSnapshot(
	ctx context.Context,
	minTS timestamp.Timestamp) error {

	if err := txn.op.UpdateSnapshot(ctx, minTS); err != nil {
		return err
	}

	// reset to get the latest partitionstate
	return txn.resetSnapshot()
}

// For RC isolation, update the snapshot TS of transaction for each statement.
// only 2 cases need to reset snapshot
// 1. cn sync latest commit ts from mo_ctl
// 2. not first sql
func (txn *Transaction) handleRCSnapshot(ctx context.Context, commit bool) (bool, error) {
	needResetSnapshot := false
	newTimes := txn.proc.Base.TxnClient.GetSyncLatestCommitTSTimes()
	if newTimes > txn.syncCommittedTSCount {
		txn.syncCommittedTSCount = newTimes
		needResetSnapshot = true
	}

	if !commit && (txn.GetSQLCount() > 0 || needResetSnapshot) {
		trace.GetService(txn.proc.GetService()).TxnUpdateSnapshot(
			txn.op, 0, "before execute")

		return true, txn.advanceSnapshot(ctx, timestamp.Timestamp{})
	}

	return false, nil
}

// Entry represents a delete/insert
type Entry struct {
	typ          int
	note         string // debug friendly note
	tableName    string
	databaseName string

	//the tenant owns the tableId and databaseId
	accountId  uint32
	tableId    uint64
	databaseId uint64

	// blockName for s3 file
	fileName string
	//tuples would be applied to the table which belongs to the tenant(accountId)
	bat       *batch.Batch
	tnStore   DNStore
	pkChkByTN int8
}

func (e *Entry) String() string {
	batinfo := "nil"
	if e.bat != nil {
		batinfo = fmt.Sprintf("{rows:%v, cols:%v, ptr:%p}", e.bat.RowCount(), len(e.bat.Vecs), e.bat)
	}
	return fmt.Sprintf("Entry{type:%v, note:%v, table:%v, db:%v, account:%v, tableId:%v, dbId:%v, bat:%v, fileName:%v}",
		typesNames[e.typ], e.note, e.tableName, e.databaseName, e.accountId, e.tableId, e.databaseId, batinfo, e.fileName)
}

func (e *Entry) DatabaseId() uint64 {
	return e.databaseId
}

func (e *Entry) TableId() uint64 {
	return e.tableId
}

func (e *Entry) Type() int         { return e.typ }
func (e *Entry) FileName() string  { return e.fileName }
func (e *Entry) Bat() *batch.Batch { return e.bat }

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
}

type tableKey struct {
	accountId  uint32
	databaseId uint64
	dbName     string
	name       string
}

func (k tableKey) String() string {
	return fmt.Sprintf("%v-%v-%v-%v", k.accountId, k.databaseId, k.dbName, k.name)
}

type tableOp struct {
	kind        int
	tableId     uint64
	statementId int
	payload     *txnTable
}

type tableOpsChain struct {
	sync.RWMutex
	names map[tableKey][]tableOp
}

type dbOp struct {
	kind        int
	databaseId  uint64
	statementId int
	payload     *txnDatabase
}

type databaseKey struct {
	accountId uint32
	name      string
}

type dbOpsChain struct {
	sync.RWMutex
	names map[databaseKey][]dbOp
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
	defs          []engine.TableDef
	tableDef      *plan.TableDef
	seqnums       []uint16
	typs          []types.Type
	_partState    atomic.Pointer[logtailreplay.PartitionState]
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
	extraInfo     *api.SchemaExtra

	// timestamp of the last operation on this table
	lastTS timestamp.Timestamp

	// process for statement
	//proc *process.Process
	proc atomic.Pointer[process.Process]

	enableLogFilterExpr atomic.Bool

	remoteWorkspace bool
	createdInTxn    bool
	eng             engine.Engine
}

// FIXME: no pointer here
type blockSortHelper struct {
	blk *objectio.BlockInfo
	zm  index.ZM
}
