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

	gcMu              sync.Mutex
	gcSchedulerCancel context.CancelFunc
	gcSchedulerDone   chan struct{}

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
	defer e.gcMu.Unlock()
	if e.dynamicCtx.closed.Load() {
		return moerr.NewInvalidStateNoCtx("engine GC is closed")
	}
	if e.gcPool != nil {
		if err := e.gcPool.ReleaseContext(context.Background()); err != nil {
			return err
		}
	}
	e.gcPool = pool
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
	// pkMoveLinkStagedBytes tracks the retained workspace bytes of staged
	// pk-move link batches. Link-table inserts are excluded from every
	// pre-commit dump, so the producer's spill discipline alone cannot
	// bound them: successive ferry waves accumulate in the coordinator's
	// workspace until the final commit. account/unaccountWorkspaceEntryLocked
	// keep this exact on every mutation path (stage, retarget re-balance,
	// prune shrink, statement rollback); the admission check in writeBatch
	// fails a statement whose staging would cross
	// pkMoveLinkCoordinatorBudget instead of growing the transaction
	// without bound.
	pkMoveLinkStagedBytes int64
	// The pin-retained workspace measure (user INSERT batches the dump
	// gate keeps because a staged link cell, an origin entry or a heal
	// binding pins one of their rows) carries no field: every budget
	// check derives it from the live workspace through
	// pkMovePinnedRetainedBytesLocked so a statement rollback that
	// truncates its writes or a dump that releases the batches drops
	// the charge with them instead of leaving a stale cached charge
	// that would spuriously fail the transaction's later small updates.
	// pkMoveRowidOrigins maps a replacement identity that a same-key
	// (pk-unchanged) UPDATE produced back to the identity its lineage
	// began with: a pk move recorded against that earlier identity must
	// keep naming it, because the committed change stream only shows the
	// departure of the lineage's FIRST generation. Populated when the
	// fill hook processes a same-key continuity marker; read when later
	// statements stage their own link rows. A statement-boundary dump
	// shadows any spilling key through the just-written object and
	// re-keys the map onto the derived object identity (see
	// recordPkMoveDumpShadowLocked), so ordinary payload updates keep
	// spilling instead of being pin-retained (r24 review F3).
	// Transaction-scoped: workspace row ids restart every transaction.
	pkMoveRowidOrigins map[types.Rowid]types.Rowid
	// pkMoveHarvestGen counts workspace compaction epochs: every
	// mergeTxnWorkspaceLocked re-key bumps it, invalidating the folded
	// harvest caches below. Zero means "no compaction ran yet".
	pkMoveHarvestGen uint64
	// pkMoveHarvestCache folds staged INSERT identities (encoded pk ->
	// staged row id) incrementally per user table so staging N link rows
	// costs O(new inserts) instead of O(N x workspace). Populated lazily
	// by the link fill hook; both fields stay zero/nil for transactions
	// that never record provenance.
	pkMoveHarvestCache map[pkMoveAcctTbl]*pkMoveHarvestCacheEntry
	// pkMoveLinkTables tracks (account, table) pairs that staged at least
	// one link row in this transaction. The delete path consults it as a
	// cheap gate before encoding pk death records, so transactions without
	// provenance pay nothing.
	pkMoveLinkTables map[pkMoveAcctTbl]struct{}
	// pkMoveLinkBirth maps a staged link row's link_id to the statement id
	// that staged it. The S3-death evaluation compares statement ids to
	// decide whether a diverted death post-dates the link's replacement
	// generation. Entries of rolled-back statements are dropped; pruned
	// rows leave orphan keys that never match again.
	pkMoveLinkBirth map[string]int
	// pkMoveDeadPks maps (account, table) -> encoded pk -> the statement
	// ids whose delete paths diverted a row of that key staged in an
	// uncommitted CN object. Such deaths never reach the committed
	// stream (the object is rewritten without the row), so this is the
	// only liveness evidence for replacements of the S3 buffering route,
	// whose links carry a NULL new_row_id. A key can divert-die in
	// several statements of one transaction (a first edit, an object
	// spill, then a second edit); EVERY recorded death must be explained
	// for a lineage to survive, so each key retains the whole set of
	// statements instead of only the latest.
	pkMoveDeadPks map[pkMoveAcctTbl]map[string]map[int]struct{}
	// pkMoveMarkerStmts maps (account, table) -> encoded pk -> the
	// statement ids that processed a same-key continuity marker for
	// that key. A marker in statement s explains a diverted death in
	// the same statement (the edit consumed its own predecessor
	// generation), so the death evaluation must not kill a link the
	// marker continued. Successive edits at one key each record their
	// own statement: the evaluation matches each death against the
	// per-statement marker evidence, so a later edit can neither erase
	// an earlier edit's explanation nor let a reincarnation marker
	// cover a death it did not consume.
	pkMoveMarkerStmts map[pkMoveAcctTbl]map[string]map[int]struct{}
	// pkMoveRetargetUndo / pkMoveOriginUndo log the current statement's
	// cross-statement provenance mutations (retargets rewrite link rows
	// staged by EARLIER statements; origin writes feed later fills):
	// RollbackLastStatement truncates only the tail of the writes slice
	// and would otherwise leave those mutations pointing at rolled-back
	// rows. Cleared at every IncrStatementID: once a statement boundary
	// passes, its mutations sit beyond rollback reach. The paired "seen"
	// sets make the first-record-wins dedup constant-time: an N-row
	// pk-unchanged update stages one continuity marker per row, and a
	// linear pre-append scan per marker made bulk non-key updates
	// quadratic under txn.Lock.
	pkMoveRetargetUndo []pkMoveRetargetUndoRec
	pkMoveOriginUndo   []pkMoveOriginUndoRec
	// pkMoveRetargetUndoSeen mirrors pkMoveRetargetUndo membership by
	// linkID; retarget and heal records share the slice (first record of
	// either kind wins), so they share the set.
	pkMoveRetargetUndoSeen map[string]struct{}
	// pkMoveOriginUndoSeen mirrors pkMoveOriginUndo membership by row id.
	pkMoveOriginUndoSeen map[types.Rowid]struct{}
	// pkMoveNullHeals binds NULL-identity (S3-route) link rows to the
	// workspace identity a resolved continuity marker established for the
	// same key, letting the keep evaluation track a lineage whose
	// replacement the S3 route never staged as a row entry. Entries carry
	// their binding statement for rollback cleanup; the committed link
	// image keeps its NULL column.
	pkMoveNullHeals map[string]pkMoveNullHeal
	// pkMoveKeyLineage records, per (account, table, encoded pk), the
	// durable-ORIGIN lineage an S3-route same-key replacement established.
	// When a continuity marker cannot resolve its replacement from the
	// workspace (the S3 buffering route stages none), the exact identity
	// map (pkMoveRowidOrigins) cannot be built for the fresh generation.
	// A later statement of this transaction that moves the row still names
	// the S3-buffered identity in old_row_id, while the committed stream
	// shows the durable pre-image departing; the binding lets the fill
	// rewrite that recorded old side back to the durable origin the
	// stream actually names. Entries are statement-tagged append-only
	// stacks (one entry per binding statement) so RollbackLastStatement
	// can truncate them; 'consumed' marks that a recorded departure
	// already moved the lineage off the key, so a later, unrelated
	// generation re-arriving at the same key is never healed onto the
	// dead lineage.
	pkMoveKeyLineage map[pkMoveAcctTbl]map[string][]pkMoveLineageBind
	// pkMoveDeleteScan is the incremental DELETE-side scan of txn.writes
	// used to prove a lineage binding still governs its key: a plain
	// delete of the bound generation after the bind statement vacates the
	// binding. Built lazily (only transactions with bindings pay for it),
	// folded with the same watermark discipline as the harvest cache, and
	// dropped wholesale on statement rollback.
	pkMoveDeleteScan map[pkMoveAcctTbl]*pkMoveDeleteScanEntry
	// pkMoveRetireDeaths records, per (account, table, encoded pk), the
	// (statement, rowid) pairs of every delete this transaction DIVERTED
	// out of the staged stream because the victim lived in an uncommitted
	// CN object. Diverted deletes never appear in txn.writes, so the
	// binding-validity delete scan cannot vacate a by-key lineage binding
	// whose bound generation such a delete murdered. The deaths are
	// recorded unconditionally — even before this transaction stages its
	// first move link — so the fill retires the murdered binding instead
	// of healing a later, unrelated generation at the same key onto the
	// dead lineage (an S3 same-key edit, a delete, a spilled reinsert,
	// then a move would otherwise cascade the original row's children
	// onto the unrelated row). pkMoveStagedOldRids tells the deaths that
	// describe a RECORDED departure (a diverted rowid some staged link or
	// marker names as old_row_id left the key through that row) from the
	// unrecorded kills that retire the lineage. Statement-tagged, and
	// truncated by RollbackLastStatement like the binding stacks.
	pkMoveRetireDeaths map[pkMoveAcctTbl]map[string][]pkMoveRetireDeath
	// pkMoveStagedOldRids contains every old_row_id cell this
	// transaction's fixup fills have observed on staged link and marker
	// rows, stamped with the first observing fill's statement id. The set
	// remembers identities as staged (a later heal rewrites the cell, not
	// the set), and the retirement evaluation consults it to decide which
	// diverted deaths a recorded departure explains. Statement rollback
	// rebuilds it from the surviving link rows.
	pkMoveStagedOldRids map[types.Rowid]int
	// pkMoveKilledLinks records, per link_id, the statement whose fill
	// proved the link's lineage dead before the generation it moved (a
	// diverted delete no staged departure explains). The commit prune
	// drops killed link rows, so the apply replays the delete semantics
	// the source history actually committed — an unresolved provenance
	// must never masquerade as a foldable move.
	pkMoveKilledLinks map[string]int
	// batchSelectListGen versions txn.batchSelectList: the pk-move
	// replacement harvest folds staged INSERT rows incrementally and must
	// drop its cached fold when a selection logically deletes an
	// already-folded row (an S3-route same-key edit removes the replaced
	// workspace generation only when the commit applies the selections,
	// long after the fold would otherwise harvest it).
	batchSelectListGen uint64
	// retiredTransfers records the persisted tombstone entries an object-route
	// transfer superseded while this transaction held pk-move provenance
	// (r26 review F3, D113). The replacement objects it publishes carry every
	// input row — re-keyed or verbatim — so the retired entries must never
	// appear in commit serialization: a twin publication breaks the
	// chain-evidence uniqueness the data-branch apply relies on. Retirement
	// keeps the writes slice indices stable (slots release in place) so
	// statement offsets stay valid; RollbackLastStatement resurrects the
	// records of the rolled-back statements, and delTransaction garbage
	// collects the surviving records' files (they are never committed in any
	// outcome).
	retiredTransfers []retiredTransferRecord
	// retiredTransfersSettled is the position-bounded SETTLED PREFIX
	// boundary of retiredTransfers (r39 review F1, D145; r40 review F1,
	// D147): records below it belong to adopted, now-irrevocable statement
	// boundaries and are ineligible for resurrection — their replacements
	// can never die in a later tail cut, so resurrecting them would
	// duplicate live tombstones; the close drain still enumerates the
	// whole collection. The adoption walk in IncrStatementID advances it
	// over every record stamped at or below the adopted statement seq,
	// and retirement deposits in append order keep the settled records a
	// consecutive prefix.
	retiredTransfersSettled int
	// pkMoveCarryJournal is the transaction-level, statement-tagged
	// chronological journal of every rollback-reachable provenance re-key
	// (r38 review F1/F2, D143/D144): the dirty-object compaction wave's
	// migration pairs (r36 review F1, D141), the boundary dump's raw ->
	// spill translations (r35 review F1, D139), and the object-route
	// tombstone transfer's re-mint rounds (r37 review F1, D142), retained
	// effective-only with one immutable entry per carried round (D150 —
	// no composition: chained rounds each journal their own entry
	// stamped with the current statement's seq, naming the intermediate
	// identity exactly), appended at carry time BEFORE the mutation it
	// records. The in-memory transfer leg and the merge/shrink in-place
	// re-keys journal nothing: their re-minted identities live in
	// retained workspace batches that outlast any rollback (sheltered,
	// r26/D110). RollbackLastStatement unwinds entries stamped at or
	// after the rolled statement in strict reverse order through the
	// forward engine with swapped pair lists; IncrStatementID's adoption
	// walk drops entries of statements the widened cut made irrevocable;
	// the ledger dies with the transaction.
	pkMoveCarryJournal []pkMoveCarryJournalEntry
	// pkMoveGenesisStates is the txn-private registration-epoch map of the
	// retention-ledger genesis protocol (D161(B)/D163): keyed by moved-table
	// address, an entry in state OWNED means this transaction staged the
	// address's identity row; EXTERNAL means the first-touch keyed probe
	// found a committed registration (read-only memo — no row, no cancel or
	// refresh authority). Map absence is the ABSENT resting state; statement
	// rollback deletes the rolled statement's entries so a retried statement
	// re-probes and re-resolves under its own snapshot. The map is published
	// nowhere beyond the owning transaction and dies with it.
	pkMoveGenesisStates map[pkMoveAcctTbl]*pkMoveGenesisEpoch
	// pkMoveGenesisJournal records the in-flight statement's registration
	// transitions for statement-atomic rollback (D161(B)): undo deletes the
	// map entry (the staged row dies with the positional workspace cut).
	// IncrStatementID settles the finished statement and clears the journal,
	// keeping it bounded by the in-flight statement.
	pkMoveGenesisJournal []pkMoveGenesisJournalEntry
	// pendingBoundaryFinalization is the staged destructive tail of one
	// successful compaction wave (r29 review F1, D123): original-entry
	// releases, superseded-object reclamation, and the deletion ledger the
	// wave drained. The enclosing boundary still has fallible work after
	// the wave (the workspace dump, the RC snapshot transfer), and a
	// failure there rolls the boundary statement back, truncating the
	// wave's own replacement registrations — applying the tail earlier
	// would destroy acknowledged originals whose replacements no longer
	// exist. IncrStatementID applies it only after every fallible step
	// succeeded; RollbackLastStatement discards it, restoring the drained
	// ledger so the retry consumes exactly the same deletions (D124).
	pendingBoundaryFinalization *boundaryFinalization
	// boundaryAdoptScans and boundaryAdoptReleases are r57 review F1 test
	// instrumentation: applyPendingBoundaryFinalizationLocked resets both
	// at entry and counts every batch-identity examination (scans) and
	// every staged-original release (releases) of one adoption, so the
	// regression suite bounds the staged-original locator's work
	// deterministically. Production logic never reads them.
	boundaryAdoptScans    int
	boundaryAdoptReleases int
	// maintWindow is the open boundary-maintenance window (r31 review F1,
	// D129): while it is armed — from the establishment of the rollback
	// cut in IncrStatementID until the boundary's adoption or rollback —
	// every destructive step of the merge/dump maintenance stays
	// APPEND-ONLY against txn.writes. The dump records its spilled slots
	// here instead of splicing them out, and the merge-fold husks keep
	// their (nil) slots; the positional cut therefore separates the
	// frozen acknowledged region from the boundary's tail appends
	// exactly, for ANY layout of preceding dumpable entries, quorum
	// merges, or RC transfer retirement. Nothing acknowledged moves while
	// the window is open, so RollbackLastStatement restores by position
	// what pre-D129 code lost by position.
	maintWindow *maintenanceWindow
	// failedWaveOrphans retains unpublished replacement object names whose
	// local cleanup owners all failed: first the rewrite writer's own
	// reclaim (D118/D122) and then the failed wave's cleanup pass itself
	// (r29 review F3, D125). Transaction close reclaims the retained
	// inventory through the GC drain, so a doubly failed cleanup cannot
	// leak an ownerless file.
	failedWaveOrphans []string
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
	// stmts runs parallel to offsets: stmts[bid][i] is the statement id
	// that deleted offsets[bid][i] (r29 review F2, D124). The tag decides
	// which pending deletions a statement rollback drops and which a
	// failed maintenance boundary must keep alive for its retry. Entries
	// staged without tags (legacy tests) belong to no removable statement
	// and are always kept.
	stmts map[types.Blockid][]int
}

func (b *deletedBlocks) size() int {
	if b == nil {
		return 0
	}
	b.RLock()
	defer b.RUnlock()
	return len(b.offsets)
}

func (b *deletedBlocks) addDeletedBlocks(blockID *types.Blockid, offsets []int64, stmt int) {
	b.Lock()
	defer b.Unlock()
	if b.offsets == nil {
		b.offsets = make(map[types.Blockid][]int64)
	}
	if b.stmts == nil {
		b.stmts = make(map[types.Blockid][]int)
	}
	b.offsets[*blockID] = append(b.offsets[*blockID], offsets...)
	for range offsets {
		b.stmts[*blockID] = append(b.stmts[*blockID], stmt)
	}
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

// cleanForStatement drops the pending deletions of statement stmtID and
// of any later statement; earlier statements keep theirs. A failed
// statement boundary rolls back only the boundary's own statement, so the
// finished statements' pending deletions survive for the retry to consume
// (r29 review F2, D124). Untagged entries belong to no removable statement
// and are always kept.
func (b *deletedBlocks) cleanForStatement(stmtID int) {
	b.Lock()
	defer b.Unlock()
	for bid, tags := range b.stmts {
		offsets := b.offsets[bid]
		keptOffsets := offsets[:0]
		keptTags := tags[:0]
		for i, tag := range tags {
			if tag < stmtID {
				keptOffsets = append(keptOffsets, offsets[i])
				keptTags = append(keptTags, tag)
			}
		}
		if len(keptTags) == 0 {
			delete(b.offsets, bid)
			delete(b.stmts, bid)
			continue
		}
		b.offsets[bid] = keptOffsets
		b.stmts[bid] = keptTags
	}
}

// drain moves the whole pending-deletion ledger out and leaves an empty
// ledger behind: a successful compaction wave baked those deletions into
// its replacements, so ownership of the consumed marks passes to the
// staged boundary finalization (r29 review F1, D123). If the boundary
// later rolls back, restore returns them verbatim (r29 review F2, D124).
func (b *deletedBlocks) drain() (
	offsets map[types.Blockid][]int64,
	stmts map[types.Blockid][]int,
) {
	b.Lock()
	defer b.Unlock()
	offsets, stmts = b.offsets, b.stmts
	b.offsets = make(map[types.Blockid][]int64)
	b.stmts = make(map[types.Blockid][]int)
	return
}

// restore returns a previously drained ledger, appending so that entries
// recorded after the drain (unreachable on the boundary path, but cheap
// to keep correct) are never clobbered.
func (b *deletedBlocks) restore(
	offsets map[types.Blockid][]int64,
	stmts map[types.Blockid][]int,
) {
	b.Lock()
	defer b.Unlock()
	if b.offsets == nil {
		b.offsets = make(map[types.Blockid][]int64)
	}
	if b.stmts == nil {
		b.stmts = make(map[types.Blockid][]int)
	}
	for bid, offs := range offsets {
		b.offsets[bid] = append(b.offsets[bid], offs...)
	}
	for bid, tags := range stmts {
		b.stmts[bid] = append(b.stmts[bid], tags...)
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
			stmts:   map[types.Blockid][]int{},
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
	// Retire pk-move link rows whose staged replacement died earlier in
	// this transaction BEFORE this transition compacts or spills staged
	// rows: mergeTxnWorkspaceLocked re-keys merged inserts and
	// dumpBatchLocked may move staged inserts to S3, and the prune matches
	// recorded replacement identities against live workspace rows. Runs at
	// every statement boundary; idempotent and a no-op without link rows.
	txn.pruneDeadPkMoveLinkRowsLocked()
	// r28 review F1 (D120): the undo-record clears and the finished-
	// statement retirements below are only valid once the finished
	// statement is beyond statement-rollback reach. They are infallible,
	// and so is the establishment immediately after them, and both run
	// under the workspace lock, so no fallible step or rollback can
	// interpose between a retirement and the boundary that makes it
	// irrevocable: from the establishment on, RollbackLastStatement
	// truncates only this boundary's own tail appends, never the finished
	// statement whose provenance history was just retired.
	txn.pkMoveRetargetUndo = nil
	txn.pkMoveOriginUndo = nil
	txn.pkMoveRetargetUndoSeen = nil
	txn.pkMoveOriginUndoSeen = nil
	// The retention-genesis journal settles with the finished statement: a
	// completed statement is beyond statement-rollback reach, so the journal
	// stays bounded to the in-flight statement.
	txn.pkMoveGenesisJournal = nil
	// r26 review F5 (D112): retire the finishing statement's marker-only
	// continuity ledger entries; same-statement death/marker pairs stay
	// for the commit prune.
	txn.retirePkMoveFinishedMarkerStmtsLocked(txn.statementID)
	// r27 review F6 (D119): same boundary discipline for the lineage
	// binding stacks and their staged-old coverage — collapse each key to
	// its single reachable binding, drop permanently consumed keys, and
	// retire coverage no retained death can name, or repeated unresolved
	// (S3-route) same-key updates retain one history entry per statement
	// forever.
	txn.sweepPkMoveFinishedContinuityLocked(txn.statementID)

	// r28 review F1 (D120): ESTABLISH the rollback boundary before the
	// fallible merge/dump maintenance. finishStart preserves the
	// workspace force-dump safety valve's statement-start derivation,
	// which would otherwise read the establishment point through the
	// incremented statementID. The offset records the finished
	// statement's pre-maintenance end; on success the adopt step below
	// widens it to include the maintenance appends, reproducing the
	// pre-r28 success shape exactly, while a maintenance failure rolls
	// back only this boundary's own tail.
	finishStart := 0
	if txn.statementID > 0 {
		finishStart = txn.offsets[txn.statementID-1]
	}
	txn.offsets = append(txn.offsets, len(txn.writes))
	established := len(txn.offsets) - 1
	txn.statementID++

	isRC := txn.op.Txn().IsRCIsolation()
	if isRC {
		if txn.transfer.lastTransferred.IsEmpty() {
			txn.start = time.Now()
			txn.transfer.lastTransferred = types.TimestampToTS(txn.op.SnapshotTS())
		}
		// Placeholder captured BEFORE the fallible maintenance: a failed
		// boundary's statement rollback restores exactly this transfer
		// state. On success the adopt step rewrites it with the values
		// the maintenance produced, matching the pre-r28 capture point.
		txn.transfer.statements = append(txn.transfer.statements, statementTransferState{
			lastTransferred: txn.transfer.lastTransferred,
			pendingTransfer: txn.transfer.pendingTransfer,
		})
	}

	// r31 review F1 (D129): open the boundary-maintenance window. From
	// here until the adoption below (or RollbackLastStatement on
	// failure), the merge/dump maintenance is APPEND-ONLY against
	// txn.writes: the dump records its spilled slots on the window and
	// the merge fold's husks keep their slots, so the offsets entry
	// recorded above keeps naming the exact boundary between the frozen
	// acknowledged region and this boundary's tail appends — no matter
	// which preceding entries the dump spills or the quorum fold merges.
	txn.maintWindow = &maintenanceWindow{}

	//merge writes for the last statement
	if err := txn.mergeTxnWorkspaceLocked(ctx); err != nil {
		return err
	}
	// dump batch to s3, starting from 0 (begining of the workspace)
	if err := txn.dumpBatchLocked(ctx, 0, finishStart); err != nil {
		return err
	}

	if isRC {
		last := &txn.transfer.statements[len(txn.transfer.statements)-1]
		last.lastTransferred = txn.transfer.lastTransferred
		last.pendingTransfer = txn.transfer.pendingTransfer

		if msg, injected := objectio.CNIncrStmtRCSnapshotFailedInjected(); injected {
			return moerr.NewInternalErrorNoCtx(
				"rc statement-boundary snapshot refresh failed by injection: " + msg)
		}
		updated, err := txn.handleRCSnapshot(ctx, commit)
		if err != nil {
			return err
		}

		if msg, injected := objectio.CNIncrStmtRCTransferFailedInjected(); injected {
			return moerr.NewInternalErrorNoCtx(
				"rc statement-boundary tombstone transfer failed by injection: " + msg)
		}
		if err := txn.transferTombstonesByStatement(ctx, updated, commit); err != nil {
			return err
		}
	}

	// r29 review F1 (D123): every fallible step of the boundary has now
	// succeeded — the merge, the dump, and the RC transfer — so the staged
	// compaction tail (original-entry releases, superseded-object
	// reclamation, and the wave-consumed ledger's drain, D124) becomes
	// irrevocable too. A boundary that failed earlier leaves the tail
	// staged for RollbackLastStatement to discard, keeping the finished
	// statements' originals and pending deletions intact for the retry.
	txn.applyPendingBoundaryFinalizationLocked()
	// Maintenance succeeded AND its staged tail is applied — close the
	// window (r31 review F1, D129): the dump-recorded slots are husked
	// and the deferred merge husks compacted, making the layout final.
	txn.adoptMaintenanceWindowLocked()
	// Only NOW does the cut widen (r30 review F2, D127, re-anchored by
	// r31 review F1, D129): before this point the boundary's appends are
	// the tail beyond the pre-maintenance offsets entry, and a failure
	// anywhere above rolls them back exactly; widening earlier cohabited
	// replacements with the originals a discard restores (D127), and
	// widening before husking/compaction would leave the offsets entry
	// naming positions the adoption just shifted (D129). On a failure
	// anywhere above, the offsets slot keeps its pre-maintenance value,
	// so the rollback cut truncates the boundary's whole tail —
	// replacements and spill files included — and the retry re-runs the
	// boundary on byte-identical input.
	txn.offsets[established] = len(txn.writes)

	// r39 review F1 (D145, refined by r40 review F1, D147): the widened
	// cut makes every earlier statement irrevocable, so both roll-forward
	// ledgers settle in ONE adoption walk against the current statement
	// seq — the retirement records minted at or below it become the
	// settled prefix of txn.retiredTransfers (ineligible for resurrection,
	// still enumerated by the close drain), and the carry journal drops
	// the same seqs' entries (the cut guarantees no later rollback's cut
	// end can reach their identities; the r38 premise that seqs never
	// revisit a completed statement is false — this boundary's own
	// maintenance stamped the NEW seq this walk settles). Work the
	// ensuing statement mints from here enters the ACTIVE set and rolls
	// back by seq exactly as before. Runs on every boundary — window or
	// not — because an AdvanceSnapshot transfer carries and retires with
	// no window at all.
	if err := txn.settlePkMoveAdoptedLedgersLocked(); err != nil {
		return err
	}

	return nil
}

// boundaryFinalization is the staged destructive tail of one successful
// compaction wave (r29 review F1, D123): release of the original INSERT
// entries, reclamation of the superseded dirty object files, and the
// deletion ledger the wave consumed. The enclosing boundary still had
// fallible work after the wave — the workspace dump and, on RC, the
// snapshot transfer — and a failure there rolls the boundary statement
// back, truncating the wave's replacement registrations. Applying this
// tail before that point would destroy acknowledged originals whose
// replacements no longer exist, so it waits staged until the boundary is
// irrevocable.
type boundaryFinalization struct {
	// releaseBats holds the IDENTITY of every original INSERT batch the
	// wave stages for release — never a positional index (r30 review F1,
	// D126). Between staging and adoption the boundary dump splices
	// dumped entries out of txn.writes and appends their spilled
	// replacements at the end, and the workspace-merge compaction drops
	// nil slots in place; a stored position silently points at a
	// neighbour — the replacement itself — by adoption time.
	releaseBats []*batch.Batch
	dirtyObject []objectio.ObjectStats
	ledger      map[types.Blockid][]int64
	ledgerStmts map[types.Blockid][]int
	// The wave's provenance re-key carries no rollback state here. r36
	// review F1 (D141) replaced D123's wholesale pre-carry snapshot of
	// the origin/heal/lineage maps with an exact inverse application
	// against the current maps, and r38 review F1 (D143) folded that
	// inverse into the transaction-wide carry journal the wave appends
	// to before its remap: RollbackLastStatement's journal walker
	// unwinds it in reverse chronological order (composing with the dump
	// and transfer carries instead of overwriting them), so a discard
	// restores only the drained deletion ledger below.
}

// maintenanceWindow is the boundary-maintenance journal of r31 review F1
// (D129): the merge/dump maintenance appends everything it produces at
// the txn.writes tail and records here every acknowledged slot it would
// otherwise have destroyed mid-window, so the rollback cut recorded
// before the maintenance keeps separating "acknowledged" from "boundary
// tail" no matter how the maintenance reshaped earlier layouts.
type maintenanceWindow struct {
	// dumpedIdxs are the workspace slots the boundary dump spilled to
	// S3. Their raw batches stay live, accounted, and in place for the
	// whole window — the appended spill-file entries carry the boundary
	// tail's copy instead, so a rollback truncates the tail and leaves
	// these slots byte-identical to the pre-maintenance state; the
	// adoption husks them (release, unaccount, nil) before compacting.
	dumpedIdxs []int
	// spilledRows is the pkCount contribution of the dumped slots,
	// subtracted at adoption when the raw slots are really gone (the
	// pre-D129 dump subtracted it eagerly).
	spilledRows int
	// mergeHusked notes that the workspace-merge quorum fold nil-ed a
	// slot whose compaction was deferred to the window exit.
	mergeHusked bool
	// The window journals nothing of its own for provenance rollback:
	// D139's dump translations and D141's wave pairs moved to the
	// transaction-level carry journal at r38 review F1 (D143), which owns
	// carries on every rollback-reachable route — this window and the
	// AdvanceSnapshot route alike. RollbackLastStatement's journal walker
	// still runs in exactly the splice region the window unwinds occupied
	// (after the tail cut, before the statement-undo replay).
}

// compactNilHusksLocked drops nil-batch slots left by deferred
// maintenance work (merge folds, retirement detachment, adoption
// husking). Runs ONLY at a window exit — adoption after the widen point
// has been made layout-proof, or rollback after the tail cut — never
// while a positional record could still name a slot.
func (txn *Transaction) compactNilHusksLocked() {
	n := 0
	for i := range txn.writes {
		if txn.writes[i].bat != nil {
			txn.writes[n] = txn.writes[i]
			n++
		}
	}
	for i := n; i < len(txn.writes); i++ {
		txn.writes[i] = Entry{}
	}
	txn.writes = txn.writes[:n]
}

// adoptMaintenanceWindowLocked closes the open boundary-maintenance
// window on the success path (r31 review F1, D129): the dump-recorded
// slots are husked for real now that the boundary is irrevocable, their
// pkCount contribution is retired, and every nil husk the maintenance
// deferred is compacted out. Positions only move here, AFTER every
// fallible step succeeded, so no stale positional record remains to
// alias a neighbour.
func (txn *Transaction) adoptMaintenanceWindowLocked() {
	window := txn.maintWindow
	if window == nil {
		return
	}
	txn.maintWindow = nil
	for _, idx := range window.dumpedIdxs {
		txn.releaseWorkspaceEntryBatchLocked(idx)
	}
	txn.pkCount -= window.spilledRows
	if len(window.dumpedIdxs) != 0 || window.mergeHusked {
		txn.compactNilHusksLocked()
	}
}

// applyPendingBoundaryFinalizationLocked runs the staged compaction tail
// at the point the enclosing boundary becomes irrevocable (r29 review F1,
// D123). A no-op when no wave is staged.
func (txn *Transaction) applyPendingBoundaryFinalizationLocked() {
	txn.boundaryAdoptScans = 0
	txn.boundaryAdoptReleases = 0
	pending := txn.pendingBoundaryFinalization
	if pending == nil {
		return
	}
	txn.pendingBoundaryFinalization = nil
	// Release by batch identity, re-locating each staged original at its
	// CURRENT position (r30 review F1, D126). A staged original is never
	// removed from the workspace on any adoption-reaching path — the
	// merge keeps BlockMeta entries, the dump only splices raw INSERTs —
	// so a miss means the staging contract itself broke and is loud, not
	// silently skipped. Re-scanning txn.writes for every staged identity
	// costs a quadratic 1+2+...+N identity comparisons when one wave
	// stages N originals (r57 review F1), so index every live batch's
	// FIRST position in a single pass — matching the old loop's
	// break-on-first-match — and consult the index once per staged
	// identity. The recorded positions stay valid through the release
	// loop: releases husk slots in place without shifting a neighbour,
	// and deleting each released identity keeps a duplicate staging from
	// re-releasing the husk (its consult then misses and warns, exactly
	// like the old rescan over the nil slot).
	posIdx := make(map[*batch.Batch]int, len(txn.writes))
	for i := range txn.writes {
		txn.boundaryAdoptScans++
		bat := txn.writes[i].bat
		if bat == nil {
			continue
		}
		if _, ok := posIdx[bat]; !ok {
			posIdx[bat] = i
		}
	}
	for _, staged := range pending.releaseBats {
		txn.boundaryAdoptScans++
		i, ok := posIdx[staged]
		if !ok {
			logutil.Warn("boundary adoption found no workspace entry for a staged original batch",
				zap.String("txn", txn.op.Txn().DebugString()))
			continue
		}
		txn.releaseWorkspaceEntryBatchLocked(i)
		txn.boundaryAdoptReleases++
		delete(posIdx, staged)
	}
	txn.unprotectUnreferencedTxnLocalSharedFilesLocked(pending.dirtyObject, nil)
	_ = txn.GCObjsByStats(pending.dirtyObject...)
	pending.ledger = nil
	pending.ledgerStmts = nil
}

// discardPendingBoundaryFinalizationLocked abandons the staged compaction
// tail of a boundary that failed: the originals were never released and
// their files stay owned, so only the wave-consumed ledger needs to
// return to txn.deletedBlocks for the retry to re-apply exactly those
// deletions (r29 review F2, D124). Any entries the rolled-back statement
// itself deleted are dropped by the cleanForStatement that follows. The
// wave's provenance re-key needs no restore here: D141 (r36 review F1)
// replaced D123's staged-snapshot restore with the journaled exact
// inverse, and D143 (r38 review F1) folded that journal into the
// transaction-wide carry journal — RollbackLastStatement's walker
// already unwound it against the CURRENT maps before this discard runs,
// in the splice region the window unwinds occupied, right after the
// positional tail cut and BEFORE the statement undo replay, which then
// reverts the rolled statement's own writes onto the unwound maps.
func (txn *Transaction) discardPendingBoundaryFinalizationLocked() {
	pending := txn.pendingBoundaryFinalization
	if pending == nil {
		return
	}
	txn.pendingBoundaryFinalization = nil
	txn.deletedBlocks.restore(pending.ledger, pending.ledgerStmts)
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

func gcFiles(txn *Transaction, scope cloneGCScope, names ...string) error {
	if len(names) == 0 {
		return nil
	}
	// A cleanup request admitted before close retains ownership of all its batches.
	e := txn.engine
	if e.dynamicCtx.closed.Load() {
		return moerr.NewInvalidStateNoCtx("engine GC is closed")
	}
	e.gcMu.Lock()
	defer e.gcMu.Unlock()
	if e.dynamicCtx.closed.Load() {
		return moerr.NewInvalidStateNoCtx("engine GC is closed")
	}
	if e.gcPool == nil {
		return moerr.NewInvalidStateNoCtx("engine GC pool is unavailable")
	}

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

	logutil.Info("GC-WORKSPACE-FILES", zap.Strings("names", names),
		zap.String("txn-info", txn.op.Txn().DebugString()))
	fs := e.fs
	for start := 0; start < len(names); start += GCBatchOfFileCount {
		end := min(start+GCBatchOfFileCount, len(names))
		batch := append([]string(nil), names[start:end]...)
		if err := e.gcPool.Submit(func() {
			// Rollback cleanup must survive query cancellation. The existing helper
			// bounds each storage attempt; engine close waits for the actual return.
			if _, err := ioutil.DeleteUnpublishedObjects(context.Background(), fs, batch...); err != nil {
				logutil.Warnf("failed to delete objects:%v, err:%v", batch, err)
			}
		}); err != nil {
			return err
		}
	}
	// r30 review F3 (D128), re-keyed by r31 review F2/F3 (D130/D131):
	// any earlier close whose reclamation could not be confirmed still
	// owns its inventory through the durable orphan registry, keyed by
	// this storage; piggyback one bounded, single-flight retry sweep on
	// this admission so retained ownership converges with ordinary GC
	// traffic.
	retryDurableOrphanGC(e)
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

func (txn *Transaction) gcObjsByIdxRange(start, end int, scope cloneGCScope) (err error) {
	var objsName []string

	defer func() {
		if err != nil {
			logutil.Warn("gc objects by index range failed",
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.Strings("names", objsName),
				zap.Error(err),
			)
		}
	}()

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

	// r28 review F2 (D121): never schedule a file for deletion while an
	// entry OUTSIDE the collected range still references it. A cut
	// registration can clone an earlier entry's object — e.g. a
	// compaction carry published into the statement tail while the
	// original batch naming the same file is retained — and deleting
	// the shared file would sever that earlier, acknowledged entry's
	// data. The tombstone bookkeeping removals above stay unconditional:
	// they drop only lazy-GC hints for entries that are going away.
	if len(objsName) > 0 {
		live := make(map[string]struct{})
		collectLive := func(e *Entry) {
			if e.bat == nil || e.bat.RowCount() == 0 ||
				e.fileName == "" || e.typ == SOFT_DELETE_OBJECT {
				return
			}
			var vec *vector.Vector
			if e.typ == DELETE {
				vec = e.bat.Vecs[0]
			} else {
				if len(e.bat.Vecs) < 2 {
					return
				}
				vec = e.bat.Vecs[1]
			}
			if vec == nil {
				return
			}
			for j := range vec.Length() {
				ss := objectio.ObjectStats(vec.GetBytesAt(j))
				live[ss.ObjectName().String()] = struct{}{}
			}
		}
		for i := 0; i < start && i < len(txn.writes); i++ {
			collectLive(&txn.writes[i])
		}
		for i := end + 1; i < len(txn.writes); i++ {
			collectLive(&txn.writes[i])
		}
		if len(live) > 0 {
			kept := objsName[:0]
			for _, name := range objsName {
				if _, ok := live[name]; !ok {
					kept = append(kept, name)
				}
			}
			objsName = kept
		}
	}

	return gcFiles(txn, scope, objsName...)
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
	var pkMoveErr error

	txn.Lock()
	defer txn.Unlock()

	beforeEntries = len(txn.writes)

	txn.rollbackCount++
	rolledStmt := txn.statementID
	if txn.statementID > 0 {
		txn.clearTableCache()
		txn.rollbackTableOpLocked()

		txn.statementID--
		end := txn.offsets[txn.statementID]
		// Skip GC for CCPR transactions - CCPRTxnCache handles GC to avoid deleting shared objects
		if !txn.isCCPRTxn {
			if err := txn.GCObjsByIdxRange(end, len(txn.writes)-1); err != nil {
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
		// r31 review F1 (D129): the failed boundary's maintenance window
		// dies here — but ONLY here, after the positional cut just ran.
		// The window kept every acknowledged entry frozen in place while
		// the merge/dump appended at the tail, so this truncation
		// restored the pre-maintenance layout exactly: spill files and
		// replacement registrations died in the tail cut, dump-recorded
		// slots still hold their live raw batches, and no finalization
		// was ever applied.
		// r38 review F1 (D143, folding r34-r37's D137/D139/D141/D142):
		// every rollback-reachable provenance carry — the boundary dump's
		// raw -> spill translations, the dirty-object wave's migration
		// pairs, and the object-route transfer's re-mint rounds, on the
		// window route and the AdvanceSnapshot route alike — is journaled
		// in the transaction-level carry journal at carry time. Unwind it
		// here, immediately after the positional tail cut removed the
		// identities those carries re-minted, in strict reverse
		// chronological order through the forward engine with swapped
		// pair lists, and BEFORE rollbackPkMoveStmtStateLocked: the undo
		// records the rolled statement wrote before its carries name
		// pre-carry identities, so the inverse must restore the state
		// they describe first. The inverse is total and applied to the
		// CURRENT maps, so in-window contributions the cut preserved —
		// above all a completed in-memory transfer carry, whose re-minted
		// cells live in retained tombstone batches (sheltered, r26/D110,
		// never journaled) — survive untouched; the merge fold's re-keys
		// are sheltered the same way. The pending wave discard below
		// restores only its drained deletion ledger: the walker already
		// ran the wave pairs' inverse (D141). A failure to unwind must
		// surface like any provenance restore failure, not vanish into a
		// silently dangling identity.
		pkMoveErr = errors.Join(pkMoveErr, txn.unwindPkMoveCarryJournalLocked(rolledStmt))
		txn.maintWindow = nil

		// r29 review F1/F2 (D123/D124): the failed boundary's staged
		// compaction tail dies here — its replacement registrations were
		// just truncated, its originals were never released, and the
		// deletion ledger the wave drained returns so the retried boundary
		// re-applies exactly those deletions. The rolled statement's own
		// deletions are dropped by the cleanForStatement at the end.
		txn.discardPendingBoundaryFinalizationLocked()

		// Undo the statement's provenance side effects the truncation
		// cannot reach: retargets it wrote into EARLIER statements' link
		// batches, origin-map writes, harvest-cache contents, and
		// statement-tagged death/marker/birth records. A restore failure
		// must surface, not vanish into a silently unrestored identity —
		// JOINED with any unwind failure above (r43 review F3): writing
		// the result directly over pkMoveErr could erase the already
		// recorded failed carry inverse behind this (usually nil) replay.
		pkMoveErr = errors.Join(pkMoveErr, txn.rollbackPkMoveStmtStateLocked())

		// r26 review F3 (D113): resurrection follows the same rule for the
		// transfer retirement ledger — retirements that ran under the cut
		// region's statements lost their replacement publications to the
		// tail cut, so their superseded originals must return to press.
		if resErr := txn.resurrectRetiredTransferEntriesLocked(rolledStmt); resErr != nil {
			pkMoveErr = errors.Join(pkMoveErr, resErr)
		}

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
	// r31 review F1 (D129): merge-fold husks inside the retained region
	// — nil slots the window's deferral left behind — are compacted only
	// now, after every positional record above (offsets, tail cut) has
	// been consumed or discarded, so no recorded position can alias a
	// shifted neighbour. The resurrection above already re-appended the
	// rolled statement's retired transfers; they are live batches and
	// unaffected by this sweep.
	txn.compactNilHusksLocked()
	txn.assertWorkspaceAccountingLocked()
	// rollback current statement's writes info
	for b := range txn.batchSelectList {
		delete(txn.batchSelectList, b)
	}

	afterEntries = len(txn.writes)

	for i := len(txn.restoreTxnTableFunc) - 1; i >= 0; i-- {
		txn.restoreTxnTableFunc[i]()
	}
	txn.restoreTxnTableFunc = txn.restoreTxnTableFunc[:0]
	//rollback the deleted blocks for the current statement: only the rolled
	//statement's own pending deletions are dropped — the finished
	//statements' deletions must survive a failed maintenance boundary so
	//the retry can consume them (r29 review F2, D124).
	txn.deletedBlocks.cleanForStatement(rolledStmt)

	// current statement has been rolled back, make can call IncrStatementID again.
	txn.incrStatementCalled = false
	txn.removeLoadFileProtectionsLocked(deletedLoadFiles)
	return errors.Join(loadCleanupErr, pkMoveErr)
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
// retiredTransferEntry preserves one workspace DELETE entry's publication
// identity (metadata + retired object stats) so a statement rollback can
// restore it exactly; the stats slices deep-copy the marshaled bytes out of
// the retired batches, which statement rollback may have already released.
type retiredTransferEntry struct {
	accountId          uint32
	databaseId         uint64
	tableId            uint64
	databaseName       string
	tableName          string
	fileName           string
	note               string
	tnStore            DNStore
	pkChkByTN          int8
	pkCheckPos         int
	pkCheckReady       bool
	pkMovePkPos        int
	skipTransfer       bool
	autoIncrEpoch      uint32
	autoIncrEpochKnown bool
	stats              []objectio.ObjectStats
}

// retiredTransferRecord groups the entries one transfer round retired while
// running under statementSeq (the txn.statementID at retirement time).
// RollbackLastStatement resurrects every record with
// statementSeq >= rolledStmt: the tail cut just discarded that statement's
// replacement entries, so the superseded originals must return.
type retiredTransferRecord struct {
	statementSeq int
	entries      []retiredTransferEntry
}

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

	// pkMovePkPos is the pk vector position the pk-move provenance readers
	// use; unlike pkCheckPos it also resolves composite primary keys (the
	// materialized __mo_cpkey column), because duplicate checks deliberately
	// reject composite batches while provenance must still harvest their
	// identities (r26 review F2). <= 0 means "unknown".
	pkMovePkPos int
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
