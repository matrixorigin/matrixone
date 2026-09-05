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
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/panjf2000/ants/v2"
	"github.com/tidwall/btree"
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
	ALTER              // alter command for TN. Update batches for mo_tables and mo_columns will fall into the category of INSERT and DELETE.
	SOFT_DELETE_OBJECT // soft delete object command for TN
)

type NoteLevel string

const (
	DATABASE NoteLevel = "database"
	TABLE    NoteLevel = "table"
	COLUMN   NoteLevel = "column"
)

var (
	typesNames = map[int]string{
		INSERT:             "insert",
		DELETE:             "delete",
		ALTER:              "alter",
		SOFT_DELETE_OBJECT: "soft_delete_object",
	}
)

// softDeleteObjectPrefix is used to encode soft-delete intent in the fileName field
// of a write entry, since there is no dedicated field for this purpose.
// Format: "soft_delete_object:<is_tombstone>"
const softDeleteObjectPrefix = "soft_delete_object:"

// makeSoftDeleteFileName encodes a soft-delete object intent into a fileName string.
func makeSoftDeleteFileName(isTombstone bool) string {
	return fmt.Sprintf("%s%v", softDeleteObjectPrefix, isTombstone)
}

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

var (
	GcCycle = 10 * time.Second
)

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

func WithExtraWorkspaceThreshold(th uint64) EngineOptions {
	return func(e *Engine) {
		e.config.extraWorkspaceThreshold = th
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

func WithPrefetchOnSubscribed(th []string) EngineOptions {
	return func(e *Engine) {
		var (
			err error
		)

		for i := range th {
			r, err2 := regexp.Compile(th[i])
			if err2 != nil {
				err = errors.Join(err, err2)
				continue
			}
			e.config.prefetchOnSubscribed = append(e.config.prefetchOnSubscribed, r)
		}

		logutil.Info("Set-Prefetch-On-Subscribed-By-TOML",
			zap.Strings("patterns", th),
			zap.Error(err),
		)
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

		memThrottler rscthrottler.RSCThrottler

		prefetchOnSubscribed           []*regexp.Regexp
		cnTransferTxnLifespanThreshold time.Duration

		ieFactory            func() ie.InternalExecutor
		statsConf            MoTableStatsConfig
		moServerStateChecker func() bool
	}

	//latest catalog will be loaded from TN when engine is initialized.
	catalog atomic.Pointer[cache.CatalogCache]
	//latest partitions which be protected by e.Lock().
	partitions map[[2]uint64]*logtailreplay.Partition

	//snapshot partitions manager
	snapshotMgr *SnapshotManager

	packerPool *fileservice.Pool[*types.Packer]

	gcPool *ants.Pool

	// XXX related to cn push model
	pClient PushClient

	// globalStats is the global stats information, which is updated
	// from logtail updates.
	globalStats *GlobalStats

	//for message on multiCN, use uuid to get the messageBoard
	messageCenter *message.MessageCenter

	timeFixed bool
	// sysTablesCreatedTime stores the created_time vectors for system tables.
	// Index mapping:
	//   0 - mo_catalog (database)
	//   1 - mo_database (table in mo_tables)
	//   2 - mo_tables (table in mo_tables)
	//   3 - mo_columns (table in mo_tables)
	//   4 - __mo_index_unique_mo_tables_logical_id (index table in mo_tables)
	sysTablesCreatedTime []*vector.Vector

	dynamicCtx
	// for test only.
	skipConsume bool

	cloneTxnCache *CloneTxnCache

	// ccprTxnCache tracks CCPR objects and their associated transactions
	ccprTxnCache *CCPRTxnCache
}

func (e *Engine) getPrefetchOnSubscribed() []*regexp.Regexp {
	if overridden, regs := engine.GetPrefetchOnSubscribed(); overridden {
		return regs
	}
	return e.config.prefetchOnSubscribed
}

func (e *Engine) SetService(svr string) {
	e.service = svr
}

func (e *Engine) ResetGCWorkerPool(pool *ants.Pool) {
	e.gcPool.Release()
	e.gcPool = pool
}

// GetCCPRTxnCache returns the CCPR transaction cache
func (e *Engine) GetCCPRTxnCache() *CCPRTxnCache {
	return e.ccprTxnCache
}

func (txn *Transaction) String() string {
	snapshot := txn.workspace.diagnosticSnapshot()
	return fmt.Sprintf("workspace mutations %v", snapshot.activeEntries)
}

// Transaction represents a transaction
type Transaction struct {
	sync.Mutex
	engine *Engine
	// workspace owns statement identity, logical mutations and table overlays.
	// Transaction fields below are migrated into it in this change; no caller
	// may use positional write offsets once the migration is complete.
	workspace *txnWorkspace
	// readOnly default value is true, once a write happen, then set to false
	readOnly atomic.Bool
	// db       *DB
	// blockId starts at 0 and keeps incrementing,
	// this is used to name the file on s3 and then give it to tae to use
	// not-used now
	// blockId uint64
	op       client.TxnOperator
	sqlCount atomic.Uint64

	tnStores []DNStore
	proc     *process.Process

	idGen IDGenerator

	currentRowId types.Rowid

	// use to cache opened snapshot tables by current txn.
	tableCache *sync.Map

	rollbackCount int

	//the start time of first statement in a txn.
	start time.Time

	hasS3Op atomic.Bool
	removed bool
	pkCount int

	adjustCount int

	haveDDL             atomic.Bool
	isCloneTxn          bool
	loadCleanupTimeout  time.Duration
	isCCPRTxn           bool
	ccprTaskID          string
	syncProtectionJobID string

	writeWorkspaceThreshold      uint64
	commitWorkspaceThreshold     uint64
	extraWriteWorkspaceThreshold uint64 // acquired from engine quota
}

func (txn *Transaction) SetCloneTxn(snapshot int64) {
	txn.isCloneTxn = true
	txn.engine.cloneTxnCache.AddTxn(txn.op.Txn().ID, snapshot)
}

// ProtectCloneFiles records pre-existing objects reused by a clone-like write.
// Objects already referenced by this transaction remain txn-local: statement
// rollback must preserve them for earlier statements, while transaction
// rollback must still delete them. Other objects are owned by committed state
// outside this transaction and must never be deleted by clone rollback.
func (txn *Transaction) ProtectCloneFiles(names ...string) {
	txn.Lock()
	defer txn.Unlock()
	txnID := txn.op.Txn().ID
	liveNames := txn.workspace.liveObjectReferences(names, nil)
	for _, name := range names {
		if _, ok := liveNames[name]; ok {
			txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txnID, name)
		} else {
			txn.engine.cloneTxnCache.AddSharedFile(txnID, name)
		}
	}
}

// TrackLoadFiles records object files physically created by LOAD TABLE. They
// are protected from the generic clone GC and synchronously removed by the
// statement/transaction rollback path while LOAD's global install lock is
// still held. This avoids both orphaning partial installs and deleting an
// object that a concurrent LOAD has begun to reuse.
func (txn *Transaction) TrackLoadFiles(names ...string) {
	txn.Lock()
	defer txn.Unlock()
	if err := txn.workspace.appendLoadFiles(names...); err != nil {
		panic(err)
	}
	txnID := txn.op.Txn().ID
	for _, name := range names {
		txn.engine.cloneTxnCache.AddSharedFile(txnID, name)
	}
}

const (
	defaultLoadFileCleanupTimeout = 2 * time.Minute
	loadFileCleanupRetryAttempts  = 128
)

// deleteLoadFiles attempts physical cleanup after statement execution has
// stopped. It never holds the transaction mutex across file-service I/O.
// Retryable failures are retried within one bounded cleanup deadline while
// LOAD's install lock still prevents another transaction from reusing a name.
// Successfully deleted names are returned with their clone-GC protection still
// installed; the caller removes that protection only after ordinary workspace
// GC has inspected the same generation.
func (txn *Transaction) deleteLoadFiles(
	ctx context.Context,
	names []string,
) (deleted []string, err error) {
	if len(names) == 0 {
		return nil, nil
	}
	names, err = txn.workspace.prepareLoadFileCleanup(names...)
	if err != nil || len(names) == 0 {
		return nil, err
	}
	// Rollback commonly runs after its request context has been canceled. Keep
	// cleanup independent from that cancellation, but bounded so a failed file
	// service cannot hold transaction locks forever.
	timeout := txn.loadCleanupTimeout
	if timeout <= 0 {
		timeout = defaultLoadFileCleanupTimeout
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	for start := 0; start < len(names); start += GCBatchOfFileCount {
		end := min(start+GCBatchOfFileCount, len(names))
		_, err := fileservice.DoWithRetryContext(
			cleanupCtx,
			"delete LOAD TABLE objects",
			func() (struct{}, error) {
				return struct{}{}, txn.engine.fs.Delete(cleanupCtx, names[start:end]...)
			},
			loadFileCleanupRetryAttempts,
			fileservice.IsRetryableError,
		)
		if err != nil {
			return deleted, err
		}
		deleted = append(deleted, names[start:end]...)
		if err := txn.workspace.completePhysicalLoadFileCleanup(
			names[start:end]...,
		); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

func (txn *Transaction) removeLoadFileProtectionsLocked(names []string) {
	txnID := txn.op.Txn().ID
	for _, name := range names {
		if !txn.workspace.hasLoadFile(name) {
			txn.engine.cloneTxnCache.RemoveSharedFile(txnID, name)
		}
	}
}

// SetCCPRTxn marks this transaction as a CCPR transaction.
// CCPR transactions will call CCPRTxnCache.OnTxnCommit/OnTxnRollback when committing/rolling back.
func (txn *Transaction) SetCCPRTxn() {
	txn.isCCPRTxn = true
}

// IsCCPRTxn returns true if this transaction is a CCPR transaction.
func (txn *Transaction) IsCCPRTxn() bool {
	return txn.isCCPRTxn
}

// SetCCPRTaskID sets the CCPR task ID for this transaction.
// When a CCPR task ID is set, the transaction can bypass shared object read-only checks.
func (txn *Transaction) SetCCPRTaskID(taskID string) {
	txn.ccprTaskID = taskID
}

// GetCCPRTaskID returns the CCPR task ID for this transaction.
// Returns empty string if no task ID is set.
func (txn *Transaction) GetCCPRTaskID() string {
	return txn.ccprTaskID
}

// SetSyncProtectionJobID sets the sync protection job ID for this transaction.
// This is used to pass the job ID to TN for commit-time validation.
func (txn *Transaction) SetSyncProtectionJobID(jobID string) {
	txn.syncProtectionJobID = jobID
}

// GetSyncProtectionJobID returns the sync protection job ID for this transaction.
// Returns empty string if no job ID is set.
func (txn *Transaction) GetSyncProtectionJobID() string {
	return txn.syncProtectionJobID
}

func NewTxnWorkSpace(eng *Engine, proc *process.Process) *Transaction {
	txn := &Transaction{
		proc:                     proc,
		engine:                   eng,
		workspace:                newTxnWorkspace(),
		idGen:                    eng.idGen,
		tnStores:                 eng.GetTNServices(),
		tableCache:               new(sync.Map),
		commitWorkspaceThreshold: eng.config.commitWorkspaceThreshold,
		writeWorkspaceThreshold:  eng.config.writeWorkspaceThreshold,
	}

	txn.readOnly.Store(true)
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

	return txn
}

// SupportsAutoIncrEpochFence reports the capability of the exact TN snapshot
// captured by this transaction. Missing or legacy targets fail closed. The
// V7-only terminal commit remains the authoritative mixed-version boundary.
func (txn *Transaction) SupportsAutoIncrEpochFence() bool {
	if len(txn.tnStores) == 0 {
		return false
	}
	for _, store := range txn.tnStores {
		if !store.AutoIncrEpochFenceSupported {
			return false
		}
	}
	return true
}

func (txn *Transaction) Readonly() bool {
	return txn.readOnly.Load()
}

func (txn *Transaction) PPString() string {
	workspaceSnapshot := txn.workspace.diagnosticSnapshot()
	usage := txn.workspace.usageSnapshot()
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

	droppedTables := txn.workspace.droppedTablesSnapshot()
	return fmt.Sprintf("Transaction{workspaceMutations: %v, workspaceRevision: %v, publishedReadView: %v, workspaceStatement: %v/%v, tableOps:%v, droppedTables: %v, tableCache: %v, insertCount: %v, rollbackCount: %v, timestamps: %v}",
		workspaceSnapshot.activeEntries,
		workspaceSnapshot.revision,
		workspaceSnapshot.published,
		workspaceSnapshot.statementID,
		workspaceSnapshot.attemptID,
		txn.workspace.ddlString(),
		len(droppedTables.keys),
		stringifySyncMap(txn.tableCache),
		usage.inMemoryInsertRows,
		txn.rollbackCount,
		stringifySlice(workspaceSnapshot.rc.snapshots, func(a any) string { t := a.(timestamp.Timestamp); return t.DebugString() }))
}

func (txn *Transaction) StartStatement() {
	if err := txn.workspace.beginStatementExecution(); err != nil {
		logutil.Fatal(err.Error())
	}
}

func (txn *Transaction) EndStatement() {
	retired, err := txn.workspace.endStatementExecution()
	if err != nil {
		logutil.Fatal(err.Error())
	}
	for _, bat := range retired {
		bat.Clean(txn.proc.Mp())
	}
}

func (txn *Transaction) IncrStatementID(ctx context.Context, commit bool) error {
	txn.op.EnterIncrStmt()
	defer txn.op.ExitIncrStmt()
	if !commit {
		if err := txn.workspace.markStatementBoundaryAdvanced(); err != nil {
			logutil.Fatal(err.Error())
		}
	}

	txn.Lock()
	defer txn.Unlock()
	//merge writes for the last statement
	if err := txn.mergeTxnWorkspaceLocked(ctx); err != nil {
		return err
	}
	// A statement boundary may spill any active memory payload. The workspace
	// journal, rather than a physical slice position, preserves rollback ownership.
	dumpScope := workspaceDumpAll(false)
	if commit {
		dumpScope = workspaceDumpCommitBoundary()
	}
	if err := txn.dumpBatchLocked(ctx, dumpScope); err != nil {
		return err
	}
	if !txn.op.Txn().IsRCIsolation() {
		_, err := txn.workspace.advanceStatement()
		return err
	}

	// The snapshot visible at statement entry is also the lower transfer bound
	// for the first RC statement. Keep every local field unchanged until the
	// snapshot update and optional tombstone transition have both succeeded.
	statementSnapshot := txn.op.SnapshotTS()
	rcState := txn.workspace.rcBoundaryState()
	transferStart := rcState.lastTransferred
	initializeTransfer := transferStart.IsEmpty()
	if initializeTransfer {
		transferStart = types.TimestampToTS(statementSnapshot)
	}

	updated, err := txn.handleRCSnapshot(ctx, commit)
	if err != nil {
		return err
	}
	forced := forceTransfer(ctx)
	transferNow := (updated || forced) && !commit
	if transferNow && !updated {
		// A test-forced transfer still needs a fresh upper snapshot bound.
		if err := txn.advanceSnapshot(ctx, timestamp.Timestamp{}); err != nil {
			return err
		}
	}

	if transferNow {
		transferEnd := types.TimestampToTS(txn.op.SnapshotTS())
		if err := txn.transferTombstonesRange(
			ctx,
			transferStart,
			transferEnd,
			true,
			rcBoundaryPublication{
				recordStatement:   true,
				statementSnapshot: statementSnapshot,
				lastTransferred:   transferEnd,
				pendingTransfer:   false,
			},
		); err != nil {
			return err
		}
	} else {
		lastTransferred := rcState.lastTransferred
		if initializeTransfer {
			lastTransferred = transferStart
		}
		if err := txn.workspace.advanceRCStatement(rcBoundaryPublication{
			recordStatement:   true,
			statementSnapshot: statementSnapshot,
			lastTransferred:   lastTransferred,
			pendingTransfer:   rcState.pendingTransfer || updated || forced,
		}); err != nil {
			return err
		}
	}

	if initializeTransfer {
		txn.start = time.Now()
	}
	return nil
}

func (txn *Transaction) AdvanceSnapshot(ctx context.Context, ts timestamp.Timestamp) error {
	txn.op.EnterIncrStmt()
	defer txn.op.ExitIncrStmt()

	txn.Lock()
	defer txn.Unlock()

	if !txn.op.Txn().IsRCIsolation() {
		return txn.advanceSnapshot(ctx, ts)
	}

	rcState := txn.workspace.rcBoundaryState()
	transferStart := rcState.lastTransferred
	initializeTransfer := transferStart.IsEmpty()
	if initializeTransfer {
		transferStart = types.TimestampToTS(txn.op.SnapshotTS())
	}

	if err := txn.advanceSnapshot(ctx, ts); err != nil {
		return err
	}

	transferEnd := types.TimestampToTS(txn.op.SnapshotTS())
	if err := txn.transferTombstonesRange(
		ctx,
		transferStart,
		transferEnd,
		false,
		rcBoundaryPublication{
			lastTransferred: transferEnd,
			pendingTransfer: false,
		},
	); err != nil {
		return err
	}
	if initializeTransfer {
		txn.start = time.Now()
	}
	return nil
}

func (txn *Transaction) PublishReadView() client.WorkspaceReadView {
	return txn.workspace.publishReadView()
}

func (txn *Transaction) CurrentReadView() client.WorkspaceReadView {
	return txn.workspace.currentReadView()
}

func (txn *Transaction) BeginWriteAttempt() client.WorkspaceWriteMark {
	return txn.workspace.beginWriteAttempt()
}

// Adjust closes one write scope. Mutation commit order is immutable from
// creation; the workspace only validates ownership, attempt identity and
// exactly-once completion here. Concurrent Compile branches may close in any
// order within the same statement attempt.
func (txn *Transaction) Adjust(mark client.WorkspaceWriteMark) error {
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
	if err := txn.workspace.adjustAttempt(mark); err != nil {
		return err
	}

	// DO NOT merge workspace here, branch-out internal sql, like ones reading mo-tables,
	// have not rights to confirm the status of the workspace.
	// if err := txn.mergeTxnWorkspaceLocked(); err != nil {
	// 	return err
	// }

	return txn.traceWorkspaceLocked(false)
}

func (txn *Transaction) traceWorkspaceLocked(commit bool) error {
	traceService := trace.GetService(txn.proc.GetService())
	if !traceService.Enabled(trace.FeatureTraceTxnWorkspace) {
		// Workspace tracing is disabled by default. Do not materialize and pin
		// every active mutation only for TxnAdjustWorkspace to discard the
		// snapshot at its own feature gate. Keep adjustCount advancing so a
		// trace enabled later in the same transaction observes the same
		// statement index sequence as before.
		txn.adjustCount++
		return nil
	}
	entries, err := txn.workspace.commitEntries()
	if err != nil {
		return err
	}
	defer entries.Close()
	index := txn.adjustCount
	if commit {
		index = -1
	}
	idx := 0
	traceService.TxnAdjustWorkspace(
		txn.op,
		index,
		func() (tableID uint64, typ string, bat *batch.Batch, more bool) {
			if idx == len(entries.entries) {
				return 0, "", nil, false
			}
			e := entries.entries[idx]
			idx++
			return e.tableId, typesNames[e.typ], e.bat, true
		})
	txn.adjustCount++
	return nil
}

type cloneGCScope int

const (
	// Intermediate GC happens while the clone transaction is still alive, for
	// example when replacing an earlier ALTER workspace with a later one. It
	// must keep txn-local objects that a later clone/ALTER now references.
	cloneGCIntermediate cloneGCScope = iota
	// Transaction rollback is the final cleanup of the clone transaction. Only
	// source files owned by committed state are protected; txn-local files must
	// be removed because no committed table can reference them after rollback.
	cloneGCTxnRollback
)

func gcFiles(txn *Transaction, scope cloneGCScope, names ...string) error {
	if txn.isCloneTxn {
		names = readutil.RemoveIf(names, func(name string) bool {
			txnID := txn.op.Txn().ID
			if txn.engine.cloneTxnCache.IsSharedFile(txnID, name) {
				return true
			}
			return scope == cloneGCIntermediate &&
				txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txnID, name)
		})
	}

	if len(names) == 0 {
		return nil
	}

	//getCaller := func(depth int) (str []string) {
	//	pc := make([]uintptr, depth)
	//	n := runtime.Callers(2, pc)
	//	frames := runtime.CallersFrames(pc[:n])
	//
	//	i := 0
	//	for {
	//		frame, more := frames.Next()
	//		funcName := filepath.Base(frame.Function)
	//		str = append(str, funcName)
	//		i++
	//		if !more || i >= depth {
	//			break
	//		}
	//	}
	//	return
	//}

	logutil.Info("GC-WORKSPACE-FILES",
		zap.Strings("names", names),
		zap.String("txn-info", txn.op.Txn().DebugString()),
		//zap.String("stack", strings.Join(getCaller(5), "<-")),
	)

	//gc the objects asynchronously.
	//TODO:: to handle the failure when CN is down.
	step := GCBatchOfFileCount
	if len(names) > 0 && len(names) < step {
		if err := txn.engine.gcPool.Submit(func() {
			if err := txn.engine.fs.Delete(context.Background(), names...); err != nil {
				logutil.Warnf("failed to delete objects:%v, err:%v", names, err)
			}
		}); err != nil {
			return err
		}

		return nil
	}

	for i := 0; i < len(names); i += step {
		if i+step > len(names) {
			step = len(names) - i
		}
		start := i
		end := i + step
		if err := txn.engine.gcPool.Submit(func() {
			//notice that the closure can't capture the loop variable i, so we need to use start and end.
			if err := txn.engine.fs.Delete(context.Background(), names[start:end]...); err != nil {
				logutil.Warnf("failed to delete objects:%v, err:%v", names[i:i+step], err)
			}
		}); err != nil {
			return err
		}
	}

	return nil

}

func (txn *Transaction) GCObjsByStats(sl ...objectio.ObjectStats) (err error) {
	names := make([]string, 0, len(sl))

	defer func() {
		if err != nil {
			logutil.Warn("gc objects by stats list failed",
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.Strings("names", names),
				zap.Error(err),
			)
		}
	}()

	for _, stats := range sl {
		names = append(names, stats.ObjectName().String())
	}

	return gcFiles(txn, cloneGCIntermediate, names...)
}

func (txn *Transaction) gcWorkspaceEntries(
	entries []workspaceEntryView,
	scope cloneGCScope,
) (err error) {
	var objsName []string
	defer func() {
		if err != nil {
			logutil.Warn("gc workspace entries failed",
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.Strings("names", objsName),
				zap.Error(err),
			)
		}
	}()

	for idx := range entries {
		entry := &entries[idx]
		if entry.bat == nil || entry.bat.RowCount() == 0 ||
			entry.fileName == "" || entry.typ == SOFT_DELETE_OBJECT {
			continue
		}
		var vec *vector.Vector
		if entry.typ == DELETE {
			vec = entry.bat.Vecs[0]
		} else {
			vec = entry.bat.Vecs[1]
		}
		for row := range vec.Length() {
			stats := objectio.ObjectStats(vec.GetBytesAt(row))
			objsName = append(objsName, stats.ObjectName().String())
		}
	}
	return gcFiles(txn, scope, objsName...)
}

func (txn *Transaction) RollbackLastStatement(ctx context.Context) error {
	txn.op.EnterRollbackStmt()
	defer txn.op.ExitRollbackStmt()
	v2.TxnRollbackLastStatementCounter.Inc()
	var (
		beforeEntries int
		afterEntries  int
	)
	defer func() {
		common.DoIfDebugEnabled(func() {
			logutil.Debug(
				"RollbackLastStatement",
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.Int("before", beforeEntries),
				zap.Int("after", afterEntries),
			)
		})
	}()
	var (
		rolledBack *workspaceRollback
		err        error
	)
	if txn.op.Txn().IsRCIsolation() {
		rolledBack, err = txn.workspace.rollbackCurrentAttemptWithRC()
	} else {
		rolledBack, err = txn.workspace.rollbackCurrentAttempt()
	}
	if err != nil {
		return err
	}
	defer rolledBack.Close()
	deletedLoadFiles, loadCleanupErr := txn.deleteLoadFiles(ctx, rolledBack.loadFiles)

	txn.Lock()
	defer txn.Unlock()

	beforeEntries = txn.workspace.activeMutationCount()

	txn.rollbackCount++
	if rolledBack.statementID > 0 {
		txn.clearTableCache()
		// Skip GC for CCPR transactions - CCPRTxnCache handles GC to avoid deleting shared objects
		if !txn.isCCPRTxn {
			if err := txn.gcWorkspaceEntries(
				rolledBack.entries.entries,
				cloneGCIntermediate,
			); err != nil {
				panic("to gc objects generated by CN failed")
			}
		}
	}
	txn.assertWorkspaceAccountingLocked()
	afterEntries = txn.workspace.activeMutationCount()

	rolledBack.RunActions()
	// The current attempt was rolled back; the same execution may publish the
	// retry attempt boundary again.
	txn.workspace.reopenStatementBoundary()
	txn.removeLoadFileProtectionsLocked(deletedLoadFiles)
	return loadCleanupErr
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

	return nil
}

// For RC isolation, update the snapshot TS for every statement execution.
// RC should observe the latest committed schema/data at statement start,
// including the first statement in an explicit transaction.
func (txn *Transaction) handleRCSnapshot(ctx context.Context, commit bool) (bool, error) {
	if !commit {
		trace.GetService(txn.proc.GetService()).TxnUpdateSnapshot(
			txn.op, 0, "before execute")

		return true, txn.advanceSnapshot(ctx, timestamp.Timestamp{})
	}

	return false, nil
}

// Entry represents a delete/insert
type Entry struct {
	// workspaceMutationID is the stable logical identity of this mutation.
	// It is never derived from, or converted back to, a slice position.
	workspaceMutationID workspaceMutationID

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
	// autoIncrEpoch is the allocator epoch used to plan this user-table write.
	// autoIncrEpochKnown distinguishes a valid initial zero epoch from an
	// old CN that did not send the dependency.
	autoIncrEpoch      uint32
	autoIncrEpochKnown bool

	// skipTransfer indicates this entry should skip transfer processing
	// Used by CCPR to avoid transfer errors for cross-cluster tombstones
	skipTransfer bool

	// pkCheck is resolved before the mutation is published. Its zero value means
	// that this mutation has no transaction-local primary-key duplicate check;
	// enabled descriptors always name one exact vector in bat.
	pkCheck workspacePKCheck
	// pkIndex is independent of duplicate checking. It identifies the encoded
	// primary-key vector used by current-state point reads, including hidden
	// composite primary keys that are deliberately excluded from pkCheck.
	pkIndex workspacePKIndex
}

// workspacePKCheck is the immutable duplicate-check contract attached to a
// workspace mutation. There is deliberately no "unresolved" state: a caller
// must either publish an exact vector position or fail the write before the
// mutation becomes visible.
type workspacePKCheck struct {
	vectorPos int
	enabled   bool
}

// workspacePKIndex is the immutable read-index contract attached to an
// in-memory INSERT. Its zero value means that the mutation cannot participate
// in an authoritative point-read index; TableOverlay records that incomplete
// coverage explicitly instead of interpreting an index miss as absence.
type workspacePKIndex struct {
	vectorPos int
	enabled   bool
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
	accountId         uint32
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

// workspaceTableKey keeps batches planned against different table definitions
// from being coalesced when the CN workspace is flushed to S3.
type workspaceTableKey struct {
	tableKey
	autoIncrEpoch      uint32
	autoIncrEpochKnown bool
}

func (k tableKey) String() string {
	return fmt.Sprintf("%v-%v-%v-%v", k.accountId, k.databaseId, k.dbName, k.name)
}

type databaseKey struct {
	accountId uint32
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
	defs          []engine.TableDef
	tableDef      *plan.TableDef
	seqnums       []uint16
	typs          []types.Type
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
	logicalId     uint64

	// timestamp of the last operation on this table
	lastTS timestamp.Timestamp

	// process for statement
	//proc *process.Process
	proc atomic.Pointer[process.Process]

	enableLogFilterExpr atomic.Bool

	remoteWorkspace bool
	createdInTxn    bool
	eng             engine.Engine

	fake bool
}

// FIXME: no pointer here
type blockSortHelper struct {
	blk *objectio.BlockInfo
	zm  index.ZM
}

type CloneTxnCache struct {
	items *btree.BTreeG[cloneTxnItem]
}

func newCloneTxnCache() *CloneTxnCache {
	return &CloneTxnCache{
		items: btree.NewBTreeG(cloneTxnItem.Less),
	}
}

func (ctc CloneTxnCache) IsSharedFile(txnId []byte, name string) bool {
	item, exist := ctc.items.Get(cloneTxnItem{txnID: txnId})
	if !exist {
		return false
	}

	_, exist = item.sharedFiles.Get(name)
	return exist
}

func (ctc CloneTxnCache) IsTxnLocalSharedFile(txnId []byte, name string) bool {
	item, exist := ctc.items.Get(cloneTxnItem{txnID: txnId})
	if !exist {
		return false
	}

	_, exist = item.txnLocalSharedFiles.Get(name)
	return exist
}

func (ctc CloneTxnCache) DeleteTxn(txnId []byte) {
	ctc.items.Delete(cloneTxnItem{txnID: txnId})
}

func (ctc CloneTxnCache) AddSharedFile(txnId []byte, name string) {
	item, exist := ctc.items.Get(cloneTxnItem{txnID: txnId})
	if !exist {
		return
	}

	item.sharedFiles.Set(name)
	ctc.items.Set(item)
}

func (ctc CloneTxnCache) RemoveSharedFile(txnId []byte, name string) {
	item, exist := ctc.items.Get(cloneTxnItem{txnID: txnId})
	if !exist {
		return
	}

	item.sharedFiles.Delete(name)
	ctc.items.Set(item)
}

func (ctc CloneTxnCache) AddTxnLocalSharedFile(txnId []byte, name string) {
	item, exist := ctc.items.Get(cloneTxnItem{txnID: txnId})
	if !exist {
		return
	}

	item.txnLocalSharedFiles.Set(name)
	ctc.items.Set(item)
}

func (ctc CloneTxnCache) RemoveTxnLocalSharedFile(txnId []byte, name string) {
	item, exist := ctc.items.Get(cloneTxnItem{txnID: txnId})
	if !exist {
		return
	}

	item.txnLocalSharedFiles.Delete(name)
	ctc.items.Set(item)
}

func (ctc CloneTxnCache) AddTxn(txnId []byte, snapshot int64) {
	item, exist := ctc.items.Get(cloneTxnItem{txnID: txnId})
	if exist {
		if item.snapTS > snapshot {
			item.snapTS = snapshot
			ctc.items.Set(item)
		}
		return
	}

	item = cloneTxnItem{
		txnID:               txnId,
		snapTS:              snapshot,
		sharedFiles:         btree.NewBTreeG(func(a, b string) bool { return a < b }),
		txnLocalSharedFiles: btree.NewBTreeG(func(a, b string) bool { return a < b }),
	}

	ctc.items.Set(item)
}

type cloneTxnItem struct {
	txnID  []byte
	snapTS int64
	// sharedFiles are cloned from committed pState and are owned outside this
	// transaction, so clone GC must never delete them.
	sharedFiles *btree.BTreeG[string]
	// txnLocalSharedFiles are produced by this transaction and later reused by
	// another clone/ALTER step in the same transaction. They are protected from
	// intermediate cleanup, but rollback must still delete them.
	txnLocalSharedFiles *btree.BTreeG[string]
}

func (cti cloneTxnItem) Less(other cloneTxnItem) bool {
	return types.Uuid(cti.txnID).Lt(types.Uuid(other.txnID))
}
