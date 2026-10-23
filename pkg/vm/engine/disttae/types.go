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
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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

	// unpublishedPending is the engine's ownership sink for storage-object
	// names whose publication failed before any workspace entry recorded
	// them and whose on-the-spot and transaction-terminal deletions failed
	// on a dirty file service (issue #25590 round-8 review). The queue
	// retains a name until a delete actually succeeds; the GC pool drains
	// it asynchronously, a failed drain leaves the names right here for
	// the next drain to retry, and a submission that cannot be scheduled
	// loses nothing — the queue already owns the names.
	unpublishedPending unpublishedPendingQueue

	gcMu              sync.Mutex
	gcSchedulerCancel context.CancelFunc
	gcSchedulerDone   chan struct{}

	// gcProducers counts producers of queue names whose names have not
	// yet all reached an owner: GC callers admitted past gcFiles' admission
	// snapshot but not yet through every name entering an owner (issue
	// #25590 round-14 review), and in-flight orphan handoffs between the
	// moment their transfer clears the transaction's list and the moment
	// the enqueue makes the names visible on the queue (issue #25590
	// round-15 review). Anything descheduled in either window still owns
	// names no owner sees, so Engine.Close's coordinated final drain
	// consults this count under gcMu before concluding on an empty queue,
	// and waits for it inside the same shutdown deadline.
	gcProducers int64

	closeOnce sync.Once
	closeErr  error

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

func (e *Engine) ResetGCWorkerPool(pool *ants.Pool) error {
	if e.dynamicCtx.closed.Load() {
		return moerr.NewInvalidStateNoCtx("engine GC is closed")
	}
	e.gcMu.Lock()
	if e.dynamicCtx.closed.Load() {
		e.gcMu.Unlock()
		return moerr.NewInvalidStateNoCtx("engine GC is closed")
	}
	old := e.gcPool
	e.gcPool = pool
	e.gcMu.Unlock()
	// The old pool's release — a worker-exit wait — runs OUTSIDE gcMu:
	// where the pool's workers can stall on storage, the lock may not
	// stand between the wait and every other gcMu user (issue #25590
	// round-13 review).
	if old != nil {
		if err := old.ReleaseContext(context.Background()); err != nil {
			return err
		}
	}
	return nil
}

// GetCCPRTxnCache returns the CCPR transaction cache
func (e *Engine) GetCCPRTxnCache() *CCPRTxnCache {
	return e.ccprTxnCache
}

func (txn *Transaction) String() string {
	return fmt.Sprintf("writes %v", txn.writes)
}

type statementTransferState struct {
	lastTransferred types.TS
	pendingTransfer bool
}

// Transaction represents a transaction
type Transaction struct {
	catalogVisibilityRevision atomic.Uint64
	catalogMutations          atomic.Int64
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
	// snapshotWriteOffset is the statement boundary of txn.writes: readers of
	// this transaction iterate the [0, snapshotWriteOffset) prefix. It is
	// advanced under txn.Lock at statement boundaries only (a new compile of
	// a user statement), never by internal SQL, and mid-statement dumps only
	// compact entries at or after the boundary; reading it is lock-free.
	snapshotWriteOffset atomic.Int64
	// the earliest write offset that Adjust must still consider for the current SQL
	// after in-place workspace compaction shifts surviving writes to the left.
	adjustWriteOffset int

	tnStores []DNStore
	proc     *process.Process

	idGen IDGenerator

	currentRowId types.Rowid

	// use to cache opened snapshot tables by current txn.
	tableCache *sync.Map

	// used to keep updated tables in the current txn
	tableOps            *tableOpsChain
	databaseOps         *dbOpsChain
	restoreTxnTableFunc []func()

	// admissionInterrupted is set when IncrStatementID has published its
	// statement bookkeeping (boundary, compaction) but still returns a
	// deferred maintenance error (merge or dump). The frontend then ends
	// the failed statement with RollbackLastStatement. A failed admission
	// happens before the statement executes, so the pending delete state
	// (batchSelectList, deletedBlocks) at that point belongs to earlier
	// successful statements — the failed admission is the one that would
	// have applied it, and the next admission reapplies it. The rollback
	// must therefore not wipe that state (issue #25590): wiping it here
	// would resurrect the rows those earlier statements deleted.
	admissionInterrupted bool

	// unpublishedDumpReservations maps the per-object admission
	// reservations a spilling writer holds under the engine's bound: the
	// capacity hook reserved each object's name BEFORE it persisted, and
	// the record choke — or the failed-publication epilogue — converts a
	// reservation into retained ownership by retiring exactly one
	// in-flight unit per name (issue #25590 round-18 review). Keyed by
	// name so records that arrive without a reservation (every non-dump
	// record creator) are still admitted fail-closed at creation, and so
	// direct dump consumers — statement admission, commit and the tests
	// alike — stay correct without any surrounding window flag.
	//
	// round-19: the hook now fires on worker and pipeline goroutines that
	// do NOT hold the transaction lock (the compaction rewriters, the
	// direct-S3 insert writer and its sink-pool jobs), so the map has its
	// own small guard. Every mutation takes unpublishedDumpReservationsMu
	// in one short section and never reaches back out to the transaction
	// lock or the engine queue — lock order is txn.Lock ->
	// unpublishedDumpReservationsMu -> engine queue — while the
	// reservation's capacity accounting itself stays where r13/r18 put
	// it: behind the engine queue's single mutex (issue #25590
	// round-19 review).
	unpublishedDumpReservationsMu sync.Mutex
	unpublishedDumpReservations   map[string]struct{}

	// recordedUnpublishedObjectNames is this transaction's copy of the
	// storage-object names its recorded workspace entries created — the
	// obligations a ROLLBACK would hand to the engine's cleanup queue.
	// They joined the engine's capacity ledger at creation (the record
	// choke), and the terminal paths retire exactly the members still
	// owned: commit published them, CCPR rollback shares them, other
	// transfers moved them already (issue #25590 round-18 review).
	recordedUnpublishedObjectNames map[string]struct{}

	// pendingObjectCleanup holds the storage-object names this transaction
	// owns as unrecorded cleanup obligations — files persisted by a failed
	// dump publication whose deletion could not be completed on the spot,
	// or handed over by a producer teardown whose metadata record never
	// landed. The objects belong to no workspace entry, so only this list
	// can collect them: every dump attempt retries the publication, and
	// Commit/Rollback purge what is left — keeping this ownership makes a
	// failed publication leak-free after retry or transaction death
	// (issue #25590).
	//
	// r5 (issue #25590 round-21 review, finding G1): every entry couples
	// its name with the statement epoch that retained it. An entry retained
	// by an operator teardown — whose downstream recording consumer can
	// still run inside the SAME statement — is a publication IN FLIGHT: a
	// mid-statement handoff may not move current-epoch entries, and every
	// quiesced boundary rechecks recorded membership before enqueuing, so
	// the engine queue only ever receives definite orphans. Definite
	// orphans stamp retainedIfRecordingLater=false and keep the round-14
	// immediate-transfer semantics.
	pendingObjectCleanup []pendingObjectCleanupItem

	// record the table dropped in the txn,
	// for filtering effortless inserts and deletes before committing.
	// tableid -> statementId
	tablesInVain map[uint64]int

	//deletes for uncommitted blocks generated by CN writing S3.
	deletedBlocks *deletedBlocks

	// map uncommitted data block generated by CN writing S3 to block's meta location.
	// notice that it's guarded by txn.Lock() and has the same lifecycle as transaction.
	cnObjsSummary map[types.Objectid]Summary
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
	// statementLifecycle is the r5 retain-boundary ordinal (issue #25590
	// round-21 review, finding G1): advanced at every StartStatement, so a
	// publication-in-flight cleanup entry stamps its retaining statement
	// and every boundary whose ordinal moved past the stamp classifies the
	// entry quiesced.
	statementLifecycle atomic.Int64
	//offsets of the txn.writes for statements in a txn.
	offsets []int

	transfer struct {
		lastTransferred types.TS
		statements      []statementTransferState
		pendingTransfer bool
	}

	compactWorker *ants.Pool

	//the start time of first statement in a txn.
	start time.Time

	hasS3Op              atomic.Bool
	removed              bool
	startStatementCalled bool
	incrStatementCalled  bool
	pkCount              int

	haveDDL             atomic.Bool
	isCloneTxn          bool
	loadFiles           map[int]map[string]struct{}
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
	liveNames := make(map[string]struct{}, len(names))
	for _, entry := range txn.writes {
		for _, stats := range collectObjectStatsFromEntry(entry) {
			liveNames[stats.ObjectName().String()] = struct{}{}
		}
	}
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
	if txn.loadFiles == nil {
		txn.loadFiles = make(map[int]map[string]struct{})
	}
	files := txn.loadFiles[txn.statementID]
	if files == nil {
		files = make(map[string]struct{})
		txn.loadFiles[txn.statementID] = files
	}
	txnID := txn.op.Txn().ID
	for _, name := range names {
		files[name] = struct{}{}
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
	statementID *int,
) (deleted []string, err error) {
	txn.Lock()
	if len(txn.loadFiles) == 0 {
		txn.Unlock()
		return nil, nil
	}
	selectedIDs := make([]int, 0, len(txn.loadFiles))
	nameSet := make(map[string]struct{})
	for id, files := range txn.loadFiles {
		if statementID != nil && id != *statementID {
			continue
		}
		selectedIDs = append(selectedIDs, id)
		for name := range files {
			nameSet[name] = struct{}{}
		}
	}
	txn.Unlock()

	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}
	slices.Sort(names)
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
		txn.Lock()
		for _, id := range selectedIDs {
			files := txn.loadFiles[id]
			for _, name := range names[start:end] {
				delete(files, name)
			}
			if len(files) == 0 {
				delete(txn.loadFiles, id)
			}
		}
		txn.Unlock()
	}
	return deleted, nil
}

func (txn *Transaction) removeLoadFileProtectionsLocked(names []string) {
	txnID := txn.op.Txn().ID
	for _, name := range names {
		stillTracked := false
		for _, files := range txn.loadFiles {
			if _, ok := files[name]; ok {
				stillTracked = true
				break
			}
		}
		if !stillTracked {
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

type Summary struct {
	objBat             *batch.Batch
	accountId          uint32
	databaseId         uint64
	tableId            uint64
	tbName             string
	dbName             string
	autoIncrEpoch      uint32
	autoIncrEpochKnown bool
}

// FIXME: The map inside this one will be accessed concurrently, using
// a mutex, not sure if there will be performance issues
type deletedBlocks struct {
	sync.RWMutex

	// used to store cn block's deleted rows
	// blockId => deletedOffsets
	offsets map[types.Blockid][]int64
}

func (b *deletedBlocks) size() int {
	if b == nil {
		return 0
	}
	b.RLock()
	defer b.RUnlock()
	return len(b.offsets)
}

func (b *deletedBlocks) addDeletedBlocks(blockID *types.Blockid, offsets []int64) {
	b.Lock()
	defer b.Unlock()
	b.offsets[*blockID] = append(b.offsets[*blockID], offsets...)
}

func (b *deletedBlocks) getDeletedRowIDs(appendTo func(row types.Rowid)) {
	b.RLock()
	defer b.RUnlock()
	for bid, offsets := range b.offsets {
		for _, offset := range offsets {
			rowId := types.NewRowid(&bid, uint32(offset))
			appendTo(rowId)
		}
	}
}

func (b *deletedBlocks) getDeletedRowIDsForBlocks(
	blocks []types.Blockid,
	appendTo func(row types.Rowid),
) {
	b.RLock()
	defer b.RUnlock()
	for i := range blocks {
		for _, offset := range b.offsets[blocks[i]] {
			rowID := types.NewRowid(&blocks[i], uint32(offset))
			appendTo(rowID)
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
	txn := &Transaction{
		proc:         proc,
		engine:       eng,
		idGen:        eng.idGen,
		tnStores:     eng.GetTNServices(),
		tableCache:   new(sync.Map),
		databaseOps:  newDbOps(),
		tableOps:     newTableOps(),
		tablesInVain: make(map[uint64]int),
		deletedBlocks: &deletedBlocks{
			offsets: map[types.Blockid][]int64{},
		},
		cnObjsSummary:   map[types.Objectid]Summary{},
		batchSelectList: make(map[*batch.Batch][]int64),
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),

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

	return fmt.Sprintf("Transaction{writes: %v, batchSelectList: %v, tableOps:%v, tablesInVain: %v,  tableCache: %v, insertCount: %v, snapshotWriteOffset: %v, rollbackCount: %v, statementID: %v, offsets: %v, transferStatements: %v}",
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
		txn.snapshotWriteOffset.Load(),
		txn.rollbackCount,
		txn.statementID,
		stringifySlice(txn.offsets, func(a any) string { return fmt.Sprintf("%v", a) }),
		stringifySlice(txn.transfer.statements, func(a any) string { return fmt.Sprintf("%+v", a) }))
}

func (txn *Transaction) StartStatement() {
	if txn.startStatementCalled {
		logutil.Fatal("BUG: StartStatement called twice")
	}
	// r5 (issue #25590 round-21 review, finding G1): the lifecycle ordinal a
	// retained cleanup entry stamps — advanced HERE, at the START of the
	// statement ceremony, so the admission dump's boundary classifies
	// entries against the ordinal of the statement now OPENING, and an
	// in-flight publication of the PREVIOUS statement is already
	// quiesced at the admission that ends it.
	txn.statementLifecycle.Add(1)
	txn.startStatementCalled = true
	txn.incrStatementCalled = false
	if callbacks, ok := txn.op.(client.StatementCallbackOperator); ok {
		callbacks.BeginStatementCallbacks()
	}
}

func (txn *Transaction) EndStatement() {
	if !txn.startStatementCalled {
		logutil.Fatal("BUG: StartStatement not called")
	}

	txn.startStatementCalled = false
	txn.incrStatementCalled = false
}

func (txn *Transaction) IncrStatementID(ctx context.Context, commit bool) error {
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
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
	// A failed admission must leave the bookkeeping exactly as a successful
	// one published it, for BOTH fallible maintenance phases of admission
	// (issue #25590): the merge (pending-mark application, retirements and
	// the fallible object-compaction resolve) and the s3 dump. The
	// statement boundary is appended — and retired nil slots compacted —
	// even when either fails, so the RollbackLastStatement the frontend
	// issues on the admission error
	// (pkg/frontend/txn.go) rolls back THIS statement's post-compaction
	// boundary instead of popping a previous successful statement's
	// boundary and truncating that statement's writes. The dump only runs
	// when the merge succeeded: a workspace that just failed maintenance
	// must not be published to storage half-maintained.
	txn.admissionInterrupted = false
	mergeErr := txn.mergeTxnWorkspaceLocked(ctx)
	var dumpErr error
	if mergeErr == nil {
		// dump batch to s3, starting from 0 (begining of the workspace).
		dumpErr = txn.dumpBatchLocked(ctx, 0)
	} else {
		// The boundary's pending-cleanup handoff rides inside the dump's
		// opening call, so the failing merge above skipped it, yet the
		// failed maintenance already retained its orphaned replacements
		// as transaction-owned pendings the engine's scheduled pass never
		// collects. Left behind the admission they would own members of
		// the bound forever, and every retried merge would refuse its
		// fresh replacement reservations before this boundary runs again,
		// wedging the still-open transaction even after the storage
		// service recovered (issue #25590 round-34 review finding; design
		// decision 3: a failed statement transfers cleanup ownership with
		// no storage call). Hand them off under the dump's own statement-
		// boundary classification (offset 0, so not quiesced) - recorded
		// names, borrowed clones and current-epoch in-flight publications
		// stay behind exactly as they do on every successful boundary.
		txn.handoffPendingObjectCleanupLocked(false)
	}
	txn.compactNilWorkspaceSlotsLocked()
	txn.offsets = append(txn.offsets, len(txn.writes))

	txn.statementID++

	if txn.op.Txn().IsRCIsolation() {
		if txn.transfer.lastTransferred.IsEmpty() {
			txn.start = time.Now()
			txn.transfer.lastTransferred = types.TimestampToTS(txn.op.SnapshotTS())
		}
		// Save the transfer state paired with this statement's write offset,
		// before advancing the snapshot or appending replacement tombstones.
		txn.transfer.statements = append(txn.transfer.statements, statementTransferState{
			lastTransferred: txn.transfer.lastTransferred,
			pendingTransfer: txn.transfer.pendingTransfer,
		})

		updated, err := txn.handleRCSnapshot(ctx, commit)
		if err != nil {
			txn.admissionInterrupted = true
			return err
		}

		if err := txn.transferTombstonesByStatement(ctx, updated, commit); err != nil {
			// Post-publication failures are as interrupting as the deferred
			// ones below: the rollback that follows must keep the pending
			// delete state of earlier successful statements.
			txn.admissionInterrupted = true
			return err
		}
	}

	// Bookkeeping is fully published by now; the deferred maintenance
	// error only affects the caller's admission outcome, never the
	// recorded boundaries rollback may reuse (issue #25590). It does mark
	// the admission as interrupted, so the statement rollback that
	// follows knows any pending delete state belongs to earlier
	// statements and must be kept for the next admission to apply.
	if mergeErr != nil {
		txn.admissionInterrupted = true
		return mergeErr
	}
	if dumpErr != nil {
		txn.admissionInterrupted = true
	}
	return dumpErr
}

func (txn *Transaction) AdvanceSnapshot(ctx context.Context, ts timestamp.Timestamp) error {
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
	txn.op.EnterIncrStmt()
	defer txn.op.ExitIncrStmt()

	txn.Lock()
	defer txn.Unlock()

	if !txn.op.Txn().IsRCIsolation() {
		return txn.advanceSnapshot(ctx, ts)
	}

	if txn.transfer.lastTransferred.IsEmpty() {
		txn.start = time.Now()
		txn.transfer.lastTransferred = types.TimestampToTS(txn.op.SnapshotTS())
	}

	if err := txn.advanceSnapshot(ctx, ts); err != nil {
		return err
	}

	return txn.transferTombstones(ctx)
}

// writeOffset returns the offset of the first write in the workspace
func (txn *Transaction) WriteOffset() uint64 {
	txn.Lock()
	defer txn.Unlock()
	return uint64(len(txn.writes))
}

// Adjust adjust writes order after the current statement finished.
func (txn *Transaction) Adjust(writeOffset uint64) error {
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

	return nil
}

// The current implementation, update's delete and insert are executed concurrently, inside workspace it
// may be the order of insert+delete that needs to be adjusted.
func (txn *Transaction) adjustUpdateOrderLocked(writeOffset uint64) error {

	if txn.statementID > 0 {
		if txn.adjustWriteOffset < int(writeOffset) {
			writeOffset = uint64(txn.adjustWriteOffset)
		}
		if writeOffset > uint64(len(txn.writes)) {
			writeOffset = uint64(len(txn.writes))
		}
		compare := func(a, b Entry) int {
			// expected in descending order

			aIsCatalog := a.isCatalog()
			bIsCatalog := b.isCatalog()

			// INSERT < DELETE < ALTER

			if aIsCatalog && !bIsCatalog {
				// a > b
				return -1
			}

			if !aIsCatalog && bIsCatalog {
				// a < b
				return 1
			}

			// (!aIsCatalog && !bIsCatalog) or (aIsCatalog && bIsCatalog)
			if a.typ == b.typ {
				return 0
			}

			return b.typ - a.typ
		}
		if !slices.IsSortedFunc(txn.writes[writeOffset:], compare) {
			txn.beginCatalogMutation()
			defer txn.endCatalogMutation()
			slices.SortStableFunc(txn.writes[writeOffset:], compare)
		}
	}

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

// gcFilesAfterAdmit is the issue-25590 round-14 regression seam: nil in
// production, armed only by tests to park a real gcFiles caller inside
// the window between its admission snapshot and its first pool
// submission — the descheduled-caller state whose handoff Close's
// coordinated final drain must wait for before concluding the queue is
// empty.
var gcFilesAfterAdmit func()

// filterGCNamesLocked applies the clone-scope admission filter and must
// run under gcMu: names a clone transaction's committed state or an
// intermediate ALTER still references survive, everything else stays
// dead. It preserves the original condition — only clone transactions
// filter — and only the transaction-rollback scope keeps txn-local
// shared files unconditionally unshared.
func filterGCNamesLocked(
	e *Engine, txn *Transaction, scope cloneGCScope, names []string,
) []string {
	if !txn.isCloneTxn {
		return names
	}
	return readutil.RemoveIf(names, func(name string) bool {
		txnID := txn.op.Txn().ID
		if e.cloneTxnCache.IsSharedFile(txnID, name) {
			return true
		}
		return scope == cloneGCIntermediate &&
			e.cloneTxnCache.IsTxnLocalSharedFile(txnID, name)
	})
}

// borrowsCommittedCloneShare reports whether name is a
// CLONE-COMMITTED-BORROWED object for this transaction — the exact
// class gcFiles' filter protects at every scope: this clone
// transaction borrowed the object from committed state
// (cloneTxnCache.IsSharedFile), so it is owned OUTSIDE the transaction
// by the clone completion and the gcFiles scope rules. The orphan
// machinery — pendingObjectCleanup and the engine's unpublished queue
// — must never touch these names (issue #25590 round-24 finding F1,
// design r11 INV1): their prior owners still reference them, and a
// failed record preparation or a saturated admission bound must not
// delete committed data the txn merely borrowed.
//
// TRANSACTION-LOCAL shared names (IsTxnLocalSharedFile) are NOT this
// class: this transaction materialized them, statement-failure
// atomicity still owns them, and the terminal full rollback still
// deletes them — exactly the r21/r22 definite-orphan semantics they
// keep through every retain and handoff path.
func borrowsCommittedCloneShare(e *Engine, txn *Transaction, name string) bool {
	if e == nil || !txn.isCloneTxn {
		return false
	}
	return e.cloneTxnCache.IsSharedFile(txn.op.Txn().ID, name)
}

func gcFiles(txn *Transaction, scope cloneGCScope, names ...string) error {
	if len(names) == 0 {
		return nil
	}
	// A cleanup request admitted before close retains ownership of all its batches.
	e := txn.engine
	if e.dynamicCtx.closed.Load() {
		return moerr.NewInvalidStateNoCtx("engine GC is closed")
	}
	// The admission decision under gcMu stays SHORT — the closed flag,
	// the pool and the file service are all that talk. The blocking part,
	// the pool admission below, runs OUTSIDE gcMu: gcPool.Submit parks
	// its caller in ants' unbounded worker wait whenever every GC worker
	// is stalled, and gcMu held across that wait chains every gcMu user
	// onto the storage stall — the terminal ownership transfer's
	// scheduling and Engine.Close's entry among them, neither of which
	// touches this storage (issue #25590 round-13 review). A pool
	// released between admission and submit reports itself through the
	// submit error, exactly like the admission checks above.
	e.gcMu.Lock()
	closed := e.dynamicCtx.closed.Load()
	pool, fs := e.gcPool, e.fs
	admitted := !closed && pool != nil
	if admitted {
		names = filterGCNamesLocked(e, txn, scope, names)
		// Register as an outstanding producer for the whole window between
		// this snapshot and every name entering an owner: a caller
		// descheduled here still owns names no owner sees. The count moves
		// in the same section that observed the engine open, so every
		// caller a later Close can miss was registered with its snapshot,
		// and anyone registering after Close began sees the closed flag
		// and never submits (issue #25590 round-14 review).
		e.gcProducers++
	}
	e.gcMu.Unlock()
	if admitted {
		defer func() {
			e.gcMu.Lock()
			e.gcProducers--
			e.gcMu.Unlock()
		}()
	}
	if closed {
		return moerr.NewInvalidStateNoCtx("engine GC is closed")
	}
	if pool == nil {
		return moerr.NewInvalidStateNoCtx("engine GC pool is unavailable")
	}

	if len(names) == 0 {
		return nil
	}

	if gcFilesAfterAdmit != nil {
		// The round-14 producer regression parks a real caller inside
		// exactly this window; nil in production.
		gcFilesAfterAdmit()
	}

	logutil.Info("GC-WORKSPACE-FILES", zap.Strings("names", names),
		zap.String("txn-info", txn.op.Txn().DebugString()))
	for start := 0; start < len(names); start += GCBatchOfFileCount {
		end := min(start+GCBatchOfFileCount, len(names))
		batch := append([]string(nil), names[start:end]...)
		if err := pool.Submit(func() {
			// Rollback cleanup must survive query cancellation. The existing helper
			// bounds each storage attempt; engine close waits for the actual return
			// within the round-11 shutdown budget (issue #25590).
			if _, err := ioutil.DeleteUnpublishedObjects(context.Background(), fs, batch...); err != nil {
				logutil.Warnf("failed to delete objects:%v, err:%v", batch, err)
			}
			// The batch left the caller's hands for good — deleted, or
			// dropped with this worker's own pre-existing loss class — so
			// its ledger membership retires here and the capacity the
			// names held at creation returns to the bound (issue #25590
			// round-18 review).
			txn.engine.settleUnpublishedPendingDeleted(batch)
		}); err != nil {
			// A pool released between admission and submit — most
			// commonly Engine.Close racing a capacity-parked GC caller
			// left the submit wait at the pool-closed error (issue
			// #25590 round-13) — must not abandon proven-dead names:
			// the whole unsent tail transfers to the engine queue, the
			// same lossless ownership the terminal paths use. Close's
			// shutdown tail drains what storage already accepts
			// before its deadline and the queue's own machinery keeps
			// the rest owned, exactly like a handoff at finalization.
			e.enqueueUnpublishedPending(names[start:])
			logutil.Warn(
				"GC-WORKSPACE-FILES-HANDOFF",
				zap.Int("names", len(names)-start),
				zap.Error(err),
			)
			return nil
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

// [start, end]
func (txn *Transaction) GCObjsByIdxRange(start, end int) (err error) {
	return txn.gcObjsByIdxRange(start, end, cloneGCIntermediate)
}

// recordedWorkspaceNames selects the storage-object names the workspace
// entries in [start, end] recorded — every write entry that carries a
// published file — and retires tombstone objects from the CN summary
// list the moment their GC is scheduled (their producer can no longer
// flush them). Both GC front doors share this selection so the clone
// filters downstream see identical name sets.
func (txn *Transaction) recordedWorkspaceNames(start, end int) []string {
	var objsName []string
	for i := start; i <= end; i++ {
		if txn.writes[i].bat == nil ||
			txn.writes[i].bat.RowCount() == 0 {
			continue
		}
		//1. Remove blocks from txn.cnObjsSummary lazily till txn commits or rollback.
		//2. Remove the segments generated by this statement lazily till txn commits or rollback.
		//3. Now, GC the s3 objects(data objects and tombstone objects) asynchronously.
		if txn.writes[i].fileName != "" && txn.writes[i].typ != SOFT_DELETE_OBJECT {
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
	return objsName
}

func (txn *Transaction) gcObjsByIdxRange(start, end int, scope cloneGCScope) (err error) {
	objsName := txn.recordedWorkspaceNames(start, end)

	defer func() {
		if err != nil {
			logutil.Warn("gc objects by index range failed",
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.Strings("names", objsName),
				zap.Error(err),
			)
		}
	}()

	return gcFiles(txn, scope, objsName...)
}

// gcObjsByIdxRangeToOwner selects exactly the same recorded names and
// applies exactly the same clone/CCPR scoping as gcObjsByIdxRange, but
// hands them to the engine's lossless unpublished-pending queue instead
// of submitting them to the GC pool (issue #25590 round-14 review): the
// statement-rollback and transaction-rollback paths run under the
// transaction's locks, and with every GC worker parked in stalled
// storage the pool submission would hold the caller — and behind it the
// transaction's final moment, the transaction locks and the request's
// cancellation — in ants' unbounded worker wait. The queue carries the
// names to the engine's own scheduled drain, whose retry ladder owns
// everything until a delete actually succeeds.
func (txn *Transaction) gcObjsByIdxRangeToOwner(start, end int, scope cloneGCScope) (err error) {
	objsName := txn.recordedWorkspaceNames(start, end)

	defer func() {
		if err != nil {
			logutil.Warn("gc objects by index range failed",
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.Strings("names", objsName),
				zap.Error(err),
			)
		}
	}()

	if len(objsName) == 0 {
		return nil
	}
	return gcFilesToOwner(txn, scope, objsName...)
}

// gcFilesToOwner is the nonblocking twin of gcFiles for callers that hold
// transaction locks: it runs the same admission snapshot — the closed
// check, the clone-scope filter, the producer registration — but performs
// no pool submission at all, transferring the names straight to the
// engine's lossless queue. Every name is owned the moment the transfer
// completes; Close's coordinated final drain and the GC scheduler tick
// are the only retries that follow, so nothing on the caller's critical
// path can touch the file service.
func gcFilesToOwner(txn *Transaction, scope cloneGCScope, names ...string) error {
	if len(names) == 0 {
		return nil
	}
	e := txn.engine
	if e.dynamicCtx.closed.Load() {
		return moerr.NewInvalidStateNoCtx("engine GC is closed")
	}
	e.gcMu.Lock()
	closed := e.dynamicCtx.closed.Load()
	filtered := names
	if !closed {
		filtered = filterGCNamesLocked(e, txn, scope, filtered)
		e.gcProducers++
	}
	e.gcMu.Unlock()
	if closed {
		return moerr.NewInvalidStateNoCtx("engine GC is closed")
	}
	// The producer reservation ends only after the enqueue to the queue
	// completed — a caller between this snapshot and the queue keeps
	// count, so Close's drain cannot conclude before its names arrive.
	defer func() {
		e.gcMu.Lock()
		e.gcProducers--
		e.gcMu.Unlock()
	}()
	logutil.Info("GC-WORKSPACE-FILES-OWNER",
		zap.Strings("names", filtered),
		zap.String("txn-info", txn.op.Txn().DebugString()))
	e.enqueueUnpublishedPending(filtered)
	return nil
}

func (txn *Transaction) RollbackLastStatement(ctx context.Context) (err error) {
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
	txn.op.EnterRollbackStmt()
	defer txn.op.ExitRollbackStmt()
	// This defer runs after the workspace mutex is released. Cache retirement
	// can wait for allocator work that needs the workspace.
	defer func() {
		if callbacks, ok := txn.op.(client.StatementCallbackOperator); ok {
			err = errors.Join(err, callbacks.RollbackStatementCallbacks(ctx))
		}
	}()
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
	deletedLoadFiles, loadCleanupErr := txn.deleteLoadFiles(ctx, &txn.statementID)

	txn.Lock()
	defer txn.Unlock()

	// An interrupted admission (issue #25590) published its bookkeeping but
	// returned a deferred merge/dump error, and this rollback ends THAT
	// failed statement. The failed statement never executed, so any pending
	// delete state still belongs to earlier successful statements — the
	// failed admission is the one that would have applied it, and the next
	// admission reapplies it. Wiping it here would resurrect the rows those
	// statements deleted and let a later commit publish the damage.
	interrupted := txn.admissionInterrupted
	txn.admissionInterrupted = false

	beforeEntries = len(txn.writes)

	txn.rollbackCount++
	if txn.statementID > 0 {
		txn.clearTableCache()
		txn.rollbackTableOpLocked()

		txn.statementID--
		end := txn.offsets[txn.statementID]
		// Defensive clamp (issue #25590): nil-slot compaction before a
		// previous admission's dump success, or an admission that failed
		// inside an older IncrStatementID, may leave a recorded boundary
		// past the live length. Re-slicing past len would resurrect
		// compacted-out entries (duplicated batches, double-Clean), so
		// the truncation target is clamped to the live workspace; with
		// failure-atomic admission bookkeeping the clamp never changes a
		// boundary the rollback should honor (that boundary is always
		// appended after compaction).
		if end > len(txn.writes) {
			end = len(txn.writes)
		}
		// Skip GC for CCPR transactions - CCPRTxnCache handles GC to avoid deleting shared objects
		if !txn.isCCPRTxn {
			// The recorded files transfer to the engine's lossless queue
			// instead of a pool submission: this rollback runs under the
			// transaction's locks, and a saturated pool would otherwise
			// park the caller — and the locks — behind stalled storage
			// (issue #25590 round-14 review).
			if err := txn.gcObjsByIdxRangeToOwner(end, len(txn.writes)-1, cloneGCIntermediate); err != nil {
				panic("to gc objects generated by CN failed")
			}
		}
		for i := end; i < len(txn.writes); i++ {
			if txn.writes[i].bat == nil {
				continue
			}
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
		txn.writes = txn.writes[:end]
		txn.offsets = txn.offsets[:txn.statementID]

		// transfer stuff
		if txn.op.Txn().IsRCIsolation() {
			entry := txn.transfer.statements[txn.statementID]
			txn.transfer.statements = txn.transfer.statements[:txn.statementID]
			txn.transfer.lastTransferred = entry.lastTransferred
			// SnapshotTS does not rewind. Removed replacements must be recovered
			// even when a young transaction would otherwise skip commit transfer.
			snapshot := types.TimestampToTS(txn.op.SnapshotTS())
			txn.transfer.pendingTransfer = entry.pendingTransfer || entry.lastTransferred.LT(&snapshot)
		}
	}

	// round-33 review, finding 2: the quiesced STATEMENT-ROLLBACK boundary
	// recycles the aborted statement's unrecorded reservations to cleanup
	// ownership exactly design r11's decision 3 shapes the failure path —
	// ownership transfers HERE with no storage call, and the queue handoff
	// at the next boundary plus the engine's scheduled drain collect the
	// files afterwards. The pre-fix rollback left the reservations booked
	// until a transaction/fragment terminal, so repeated failed statements
	// of an open transaction accumulated in-flight units until the shared
	// admission bound rejected other writers' spills, with healthy storage
	// and an empty cleanup queue. The drain's recorded-membership recheck
	// keeps prior recorded writes on their own lifecycle, the
	// committed-borrowed exemption keeps clone-cache shares owned, and the
	// retained names are definite orphans deferred-for-collection, so the
	// transaction keeps its full admission budget for the retry.
	txn.drainUnpublishedDumpReservationsLocked(
		"statement rollback reservation recycle: the rolled-back statement's records can never land, so the quiesced boundary transfers each unrecorded reservation to cleanup ownership and releases its in-flight unit before the transaction retries (issue #25590 round-33 review, finding 2)")

	txn.assertWorkspaceAccountingLocked()
	// rollback current statement's writes info. After an interrupted
	// admission the pending state belongs to earlier successful statements
	// and must survive (see the flag read above).
	if !interrupted {
		for b := range txn.batchSelectList {
			delete(txn.batchSelectList, b)
		}
	}

	afterEntries = len(txn.writes)

	for i := len(txn.restoreTxnTableFunc) - 1; i >= 0; i-- {
		txn.restoreTxnTableFunc[i]()
	}
	txn.restoreTxnTableFunc = txn.restoreTxnTableFunc[:0]
	//rollback the deleted blocks for the current statement.
	// After an interrupted admission the pending block deletions belong to
	// earlier successful statements and must survive for the next
	// admission's object compaction (issue #25590).
	if !interrupted {
		txn.deletedBlocks.clean()
	}

	// current statement has been rolled back, make can call IncrStatementID again.
	txn.incrStatementCalled = false
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
		return true, txn.advanceSnapshot(ctx, timestamp.Timestamp{})
	}

	return false, nil
}

// insertKeyRange is the byte-wise min/max envelope of an insert entry's
// primary keys, canonical-encoded exactly as MemPKFilter filters encode keys.
// inited=false leaves the entry on the unpruned full-scan path.
type insertKeyRange struct {
	inited bool
	min    []byte
	max    []byte
}

// insertKeyOffsets maps an insert entry's canonical-encoded primary keys to the
// row offsets of the entry's CURRENT batch vector layout (issue 26796). It is
// hydrated lazily by exact single-key probes and invalidated at every batch
// mutation (merge, shrink, release), so a hydrated map always describes the
// batch it was built from. Repeated primary keys hold all of their live row
// offsets.
type insertKeyOffsets struct {
	offsets map[string][]uint32
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
	bat *batch.Batch
	// pkRange is the canonical-encoded primary-key envelope of an insert
	// entry's rows (issue 26796). Writers compute it once from the entry's
	// primary-key column; readers use it to skip the whole batch when the
	// statement's primary-key filter provably misses the envelope. It is a
	// conservative superset forever: in-transaction row deletions only shrink
	// the live rows inside the envelope, never widen it.
	pkRange insertKeyRange
	// keyOffsets is the per-batch exact-key probe index of an insert entry.
	// Readers hydrate it lazily for exact single-key point probes and bypass
	// the whole-batch FilterVector pass; every invalidation drops it (through
	// unaccountInsertKeyOffsetsLocked, together with its budget charge) so a
	// hydrated map can never describe a stale row layout or outlive its
	// charge, and no duplicated entry such as an ALTER transfer ever shares
	// another entry's map. A nil keyOffsets keeps the entry on the unpruned
	// FilterVector path.
	keyOffsets *insertKeyOffsets
	// keyIndexAccountedSize is the workspace-budget estimate charged for the
	// hydrated keyOffsets (deterministic upper bound: one copied string key
	// and one amortized map bucket per row, one uint32 offset slot per row,
	// plus the map base). It is charged before the map is allocated and
	// refunded — with the map dropped — on every invalidation or entry
	// retirement (merge, shrink, release, dump, ALTER transfer, workspace
	// compaction, transaction teardown), so workspaceSize and
	// approximateInMemInsertSize always include exactly the retained index
	// bytes that the spill thresholds consume. Envelope bytes (pkRange min/max
	// clones) are bounded below two key lengths per batch and stay outside
	// this budget. The invariant keyOffsets != nil implies a nonzero charge
	// is validated by checkWorkspaceAccountingLocked.
	keyIndexAccountedSize uint64
	// accountedSize is the batch size currently included in workspaceSize.
	// Keeping it on the entry lets in-place mutations remove the old
	// contribution before accounting for the batch's new state.
	accountedSize uint64
	tnStore       DNStore
	pkChkByTN     int8
	// autoIncrEpoch is the allocator epoch used to plan this user-table write.
	// autoIncrEpochKnown distinguishes a valid initial zero epoch from an
	// old CN that did not send the dependency.
	autoIncrEpoch      uint32
	autoIncrEpochKnown bool

	// skipTransfer indicates this entry should skip transfer processing
	// Used by CCPR to avoid transfer errors for cross-cluster tombstones
	skipTransfer bool

	// pkCheckPos is the primary-key vector position resolved at write time.
	// pkCheckReady indicates whether write-time resolution was reliable.
	// When it is false, duplicate checks must fall back to legacy table-def lookup.
	pkCheckPos   int
	pkCheckReady bool
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

type tableOp struct {
	kind        int
	tableId     uint64
	statementId int
	payload     *txnTable
}

type tableOpsChain struct {
	sync.RWMutex
	names       map[tableKey][]tableOp
	creatdInTxn map[uint64]int // tableId -> statementId
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
