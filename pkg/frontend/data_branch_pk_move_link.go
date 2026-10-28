// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

// A pk-moving UPDATE on a branch arrives in the change stream as an
// independent tombstone (old key) plus insert (new key), indistinguishable
// from a bare DELETE followed by an INSERT. Replaying that pair on the
// destination breaks foreign-key ON UPDATE actions: the parent delete either
// fails (RESTRICT) or cascades destructive deletes before the replacement
// insert lands. To recover update provenance safely, the UPDATE executor
// persists one row per moved primary key into
// mo_catalog.mo_data_branch_update_link (see the multi_update operator), and
// the data-branch diff consults those rows here to fold the tombstone+insert
// pair back into one native destination UPDATE.
//
// Design: docs/design/20261016-data-branch-pk-move-update-provenance.md.
//
// Provenance is evidence-driven, never occupancy-driven. Every link names
// the deleted row version exactly (old_pk + old_row_id) and the key the row
// moved to (new_pk); the replacement row's identity is additionally recorded
// when knowable (new_row_id). One identity cannot be known at write time:
// CN workspace-staged inserts get their committed row id assigned by TN at
// precommit, long after the link row was written, so a workspace-harvested
// new_row_id names the PRE-commit identity (a TxnWorkspaceSegment row id),
// never the committed one. The chain walk below therefore binds versions by
// commit timestamp, the one durable fact shared by the link's own anchor
// tombstone and every stream row its transaction produced:
//
//   - the anchor tombstone (old_pk, old_row_id) is matched to an exact link
//     and contributes the committing transaction's timestamp T;
//   - the version the move landed on is the unique stream data row under
//     new_pk born at exactly T (primary-key uniqueness at every committed
//     instant plus one row per key per transaction make this tie as strong
//     as a row-id match, for staged and S3-persisted replacements alike);
//   - if that version died inside the diff window, its tombstone at T' > T
//     must pair with the unique successor link naming the tombstoned row id
//     exactly (a cross-transaction continuation); T becomes T';
//   - if that version's tombstone records no move at all, the key kept the
//     row: T becomes the tombstone's timestamp and the walk re-verifies the
//     re-born image at the SAME key, folding a stable-key update tail (and
//     an equivalent same-transaction delete+insert pair, exactly like the
//     legacy stable-key update matcher does) into the lineage;
//   - if it left no stream trace at all, the generation was netted inside
//     one transaction, and the unique successor link must chain off the
//     previous link's recorded workspace-stage identity with the same
//     transaction create timestamp (a same-transaction continuation);
//     T is kept.
//
// A live row at the right key with the WRONG birth timestamp is an
// unrelated occupant: it keeps its plain insert meaning, and the lineage,
// having no proven continuation, fails closed to legacy delete+insert
// replay - which stays loud on referenced parents instead of re-parenting
// children to an unrelated row. Ambiguous successors, timestamp
// contradictions, cycles, cap overflows, and every form of incomplete
// evidence fail closed the same way.

const (
	// pkMoveLinkMaxChain bounds the old->new hop walk for consecutive moves
	// of one row (k1->k2->k3). Longer walks are treated as corrupt and fail
	// closed.
	pkMoveLinkMaxChain = 32
	// pkMoveOccupancyProbeChunk bounds the OR list width of one batched
	// destination-occupancy probe SQL.
	pkMoveOccupancyProbeChunk = 512
	// pkMoveLinkProbeChunk bounds the IN list width of prefetch probes.
	pkMoveLinkProbeChunk = 512
	// pkMovePickClosureProbeQueryMax bounds how many per-key round trips
	// one overflowed pick-closure window spends per expansion round. The
	// cap guards planning time against corrupt or pathological frontiers,
	// not semantics: past it the closure stops expanding with whatever
	// keys already proved sound.
	pkMovePickClosureProbeQueryMax = 1024
)

var (
	// pkMoveLinkMaxCachedRows bounds the catalog rows one diff caches, and
	// the bound rides every probe SQL as a LIMIT so oversized history is
	// never materialized. Past the cap the failing scope narrows instead
	// of the whole provider disabling: a whole-window scan that crosses
	// it switches that window to per-key probing, a single key whose own
	// history crosses it fails that key closed, and a saturated cache
	// leaves only the unprobed keys incomplete (r23 review F4).
	// Variable (not const) so unit tests can drive the cap deterministically.
	pkMoveLinkMaxCachedRows = 1 << 20
	// pkMoveClosureMaxAdmittedKeys bounds how many keys the PICK closure may
	// add along link topology. The bound is a guard against corrupt links,
	// not a semantic limit; past it the closure stops expanding (fail
	// closed) instead of growing without bound. Test hook, as above.
	pkMoveClosureMaxAdmittedKeys = 1 << 20
)

// pkMoveLinkReadBudget returns the reader budget one probe rides as its
// LIMIT: normally pkMoveLinkMaxCachedRows; the test-only
// fj/cn/data_branch_pk_move_link_probe_budget fault shrinks it (for one
// table via its sarg selector, or every table) so BVT can exercise the
// overflow-narrowing paths without a million rows of history. The hard
// provider-disable cap keeps its production value; only probe LIMITs
// consult this.
func pkMoveLinkReadBudget(tableID uint64) int {
	if armed, ok := objectio.CNDataBranchPkMoveLinkProbeBudgetInjected(tableID); ok {
		return int(armed)
	}
	return pkMoveLinkMaxCachedRows
}

// pkMoveLinkTableWindow scopes provenance reads to one physical relation of
// a diff side. tableID is the relation the links belong to (their link_id
// prefix), and endPhys is the side's collect-window upper bound in physical
// nanoseconds: links with write_ts > endPhys are future history to this
// diff and must stay invisible (snapshot and PICK BETWEEN reads, and the
// per-generation windows of multi-generation branches).
type pkMoveLinkTableWindow struct {
	tableID uint64
	// startPhys records the window's collect start; it participates only
	// in the full-window cache signature, never in the probe predicate.
	// write_ts is the recording transaction's CREATE timestamp, so a
	// transaction created before this start can still commit moves
	// inside the window: a lower-bound predicate would make those
	// omitted departures read as complete absence, and stream commit
	// instants decide real window membership during the walk.
	startPhys uint64
	endPhys   uint64
	// from and end carry the collector's per-generation full-types.TS
	// bounds for the D156 capture (r51 F2): two timestamps with equal
	// physical and different logical parts are distinct commit instants
	// with different boundary membership, which the truncated probe
	// values above cannot express. The physical view keeps its
	// probe-budget role and is never used to derive capture bounds.
	from types.TS
	end  types.TS
}

// captureBounds projects this window to the inclusive [from, end] the
// link commit capture reads, applying the r52 zero-start normalization:
// a whole-history window's empty lower bound would route the engine to
// the end-stamping checkpoint state reader, so [MinTs(), end] reads as
// [MinTs().Next(), end] — identical real commits, exact stamps. The
// unit-fixture fallback (an endPhys of math.MaxInt64 with no recorded
// full-type bound) reads as unbounded upper.
func (w pkMoveLinkTableWindow) captureBounds() (from, end types.TS) {
	from = w.from
	if from.IsEmpty() {
		zero := types.MinTs()
		from = zero.Next()
	}
	end = w.end
	if end.IsEmpty() {
		end = types.MaxTs()
	}
	return from, end
}

// appendPkMoveLinkWindow registers tableID's closure window bounded by
// endPhys. A table reachable from BOTH branch paths is registered once
// per side; the closure must stay able to read every link either side
// can fold, so a duplicate registration raises the surviving window to
// the GREATEST required endpoint instead of dropping the later one
// (r51 review F1). First registration order is preserved. These windows
// bound closure expansion only; each diff side's data-collection bounds
// are computed from its own snapshot elsewhere and never consulted here.
func appendPkMoveLinkWindow(
	windows []pkMoveLinkTableWindow, seen map[uint64]int, tableID uint64, endPhys uint64,
) []pkMoveLinkTableWindow {
	if tableID == 0 {
		return windows
	}
	if i, dup := seen[tableID]; dup {
		if endPhys > windows[i].endPhys {
			windows[i].endPhys = endPhys
		}
		return windows
	}
	seen[tableID] = len(windows)
	return append(windows, pkMoveLinkTableWindow{tableID: tableID, endPhys: endPhys})
}

// pkMoveLink records one row-level pk move: the row identified by oldRowID
// had its primary key changed from oldPK to newPK by a transaction whose
// creation timestamp was writeTS, producing the replacement version the
// writer attributed as newRowID. For workspace-staged replacements that
// attribution is the PRE-commit (TxnWorkspaceSegment) row id, because the
// committed row id is assigned by TN only at precommit; the walk uses it
// exclusively to chain same-transaction generations, never to match stream
// rows. oldPK/newPK hold the branch-hashmap key encoding (types.Packer over
// the pk column), so they key dataHashmap lookups verbatim. hasNewRowID is
// false when the writer could not attribute the replacement at all (for
// example the S3 persist path); such a link still folds the generation it
// names via the commit-timestamp tie and still expands PICK keys, but a
// netted same-transaction continuation cannot chain off it.
type pkMoveLink struct {
	oldRowID    types.Rowid
	newRowID    types.Rowid
	hasNewRowID bool
	oldPK       []byte
	newPK       []byte
	writeTS     types.TS
	// txnID is the recording transaction's durable identity; hasTxnID is
	// false when the column carries the all-zero "identity unknown" form
	// (a defensive fallback keeps matching on the write timestamp then).
	txnID    [catalog.DataBranchUpdateLinkTxnIDBytes]byte
	hasTxnID bool
}

// pkMoveTxnScope is the transaction identity the walk currently stands
// in: links of the recording transaction match it by the durable
// transaction id when both sides carry one, falling back to the create
// timestamp otherwise. The write timestamp alone is never the
// transaction namespace: two transactions created inside one physical
// instant share it (hlc.go advances the logical component).
type pkMoveTxnScope struct {
	writeTS types.TS
	txnID   [catalog.DataBranchUpdateLinkTxnIDBytes]byte
	hasTxn  bool
}

func (s pkMoveTxnScope) matches(link *pkMoveLink) bool {
	if s.hasTxn && link.hasTxnID {
		return s.txnID == link.txnID
	}
	return link.writeTS.EQ(&s.writeTS)
}

func pkMoveLinkTxnScope(link *pkMoveLink) pkMoveTxnScope {
	return pkMoveTxnScope{writeTS: link.writeTS, txnID: link.txnID, hasTxn: link.hasTxnID}
}

// samePkMoveLinkIdentity reports whether two links describe the exact same
// move; idempotent probe merges rely on it to skip duplicates, while two
// DIFFERENT links claiming one (row id, old pk) pair must poison that key.
func samePkMoveLinkIdentity(a, b pkMoveLink) bool {
	if a.hasTxnID && b.hasTxnID && a.txnID != b.txnID {
		return false
	}
	return a.oldRowID == b.oldRowID &&
		a.hasNewRowID == b.hasNewRowID &&
		(!a.hasNewRowID || a.newRowID == b.newRowID) &&
		string(a.oldPK) == string(b.oldPK) &&
		string(a.newPK) == string(b.newPK) &&
		a.writeTS.EQ(&b.writeTS)
}

// pkMoveLinkRowKey builds the byRow map key: rowid bytes + old pk bytes.
// The fixed-width rowid prefix keeps the split point unambiguous.
func pkMoveLinkRowKey(rowID types.Rowid, oldPK []byte) string {
	buf := make([]byte, 0, len(rowID)+len(oldPK))
	buf = append(buf, rowID[:]...)
	buf = append(buf, oldPK...)
	return string(buf)
}

// pkMoveLinkProbedKey scopes the negative-probe cache to one table window:
// "no link for key K" is only valid for the table it was probed on.
func pkMoveLinkProbedKey(tableID uint64, oldPK []byte) string {
	buf := make([]byte, 0, 8+len(oldPK))
	putUint64BE(buf[:8], tableID)
	// putUint64BE writes through the zero-valued backing array; append the
	// remaining bytes after marking the length.
	buf = buf[:8]
	buf = append(buf, oldPK...)
	return string(buf)
}

// pkMoveLinkPrefixRange returns the half-open [start, end) link_id bounds
// covering every link row of tableID whose old-pk sha equals sha (an empty
// sha selects the whole table). Bounds are full 64-byte values with zeroed
// trailing row-id bytes, so plain binary comparison works. ok is false only
// when incrementing the prefix overflows (tableID == MaxUint64, or an
// all-0xff sha there), where probing must fail closed. Range probing rides
// the link_id primary key: the writer records rows through a raw relation
// Write that does not maintain secondary indexes, so no probe may rely on
// one.
func pkMoveLinkPrefixRange(tableID uint64, sha []byte) (start, end []byte, ok bool) {
	prefixLen := 8
	if len(sha) != 0 {
		if len(sha) != catalog.DataBranchUpdateLinkPKShaBytes {
			return nil, nil, false
		}
		prefixLen += catalog.DataBranchUpdateLinkPKShaBytes
	}
	start = make([]byte, catalog.DataBranchUpdateLinkIDBytes)
	end = make([]byte, catalog.DataBranchUpdateLinkIDBytes)
	putUint64BE(start[:8], tableID)
	copy(end, start)
	if len(sha) != 0 {
		copy(start[8:], sha)
		copy(end, start)
	}
	for i := prefixLen - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return start, end, true
		}
	}
	return nil, nil, false
}

func putUint64BE(buf []byte, v uint64) {
	_ = buf[7]
	buf[0] = byte(v >> 56)
	buf[1] = byte(v >> 48)
	buf[2] = byte(v >> 40)
	buf[3] = byte(v >> 32)
	buf[4] = byte(v >> 24)
	buf[5] = byte(v >> 16)
	buf[6] = byte(v >> 8)
	buf[7] = byte(v)
}

// pkMoveLinkProvider serves catalog link rows to one diff on demand. Anchor
// (old-key, row-id) probes are prefetch-batched per tombstone chunk;
// continuation probes resolve lazily and cache. Every SELECT carries a
// LIMIT derived from the remaining row budget, so arbitrarily large link
// history is never materialized. Crossing the budget narrows the failure:
// a crossed whole-window scan discards its truncated page and serves the
// window per key (only the requested keys' ranges), a single key crossing
// it fails only that key closed, and a saturated cache leaves only the
// unprobed keys incomplete — never truncated-but-silent. Tombstone shards
// of a diff run in parallel, so every map access below is mutex-guarded;
// SQL probes run outside the lock and merge idempotently.
type pkMoveLinkProvider struct {
	ctx     context.Context
	ses     *Session
	bh      BackgroundExec
	windows []pkMoveLinkTableWindow

	mu       sync.Mutex
	byRow    map[string]pkMoveLink
	byPK     map[string][]pkMoveLink
	probed   map[string]struct{}
	poisoned map[string]struct{}
	// fullWindows marks a table generation whose whole link_id range has
	// been scanned into the caches: every later key of that window —
	// anchors, continuation hops, successors — resolves without more SQL.
	// The stored [start, end] signature detects a same-table window
	// arriving with different bounds; the cached coverage would
	// otherwise silently claim an interval it never scanned.
	fullWindows map[uint64][2]uint64
	// overflowWindows marks a table generation whose whole-range scan
	// crossed the row budget: the truncated prefix was DISCARDED, and
	// the window serves the requested keys through per-key (sha, sha+1)
	// prefix probes instead. The r23 review's F4: unrelated keys' history
	// must never disable a small PICK, so overflow narrows the evidence
	// requirement to exactly the keys the diff asks about instead of
	// failing the entire provider. The signature guard matches
	// fullWindows: same-table evidence under different time bounds
	// disables (fail closed).
	overflowWindows map[uint64][2]uint64
	// overflowKeys marks the (table, old-pk) probes whose OWN history
	// crossed the row budget even in per-key mode: that key — and only
	// that key — fails closed as incomplete evidence, like a poisoned
	// row key but scoped to one probed key rather than one anchor row.
	overflowKeys map[string]struct{}
	disabled     bool
	rows         int
	// arrivals caches grounded-arrival evidence for the continuity tie:
	// one build per (cache state, stream view) pair indexes every cached
	// arrival chain by its LANDED key, so an anchor's tie check is an O(1)
	// lookup plus a tiny candidate walk instead of a rescan of the whole
	// provenance cache per anchor (r53 F2: N disjoint-key moves with N
	// ties made acceptance quadratic in the cache size). arrivalsRows is
	// p.rows at build time — a later probe merge invalidates the index.
	arrivals     map[string]*pkMoveArrivalEntry
	arrivalsView pkMoveStreamView
	arrivalsRows int
	// arrivalBuilds and arrivalScans instrument the evidence build so the
	// scaling regression can prove independent anchors never rescan the
	// complete cache: builds must stay 1 across anchor checks and scans
	// must stay linear in the link count.
	arrivalBuilds uint64
	arrivalScans  uint64
	// depIdx caches each consulted old-pk key's departure index so many
	// chains through one shared key — and many same-key hops of a long
	// walk — never rescan the key's whole link bucket per hop (r55
	// review F2). Entries self-invalidate on length change: mergeProbed-
	// LinksLocked only appends to a key's slice.
	depIdx map[string]*pkMoveDepKeyIndex
	// depBuilds and depScans instrument the per-key DEPARTURE indexes the
	// verdict walk's successor lookups consult (r55 review F2): builds
	// must stay bounded by the consulted keys and scans linear in the
	// consulted candidates — never a full byPK bucket rescan per hop.
	depBuilds uint64
	depScans  uint64
	// arrivalCommitIdx is the D156 scope->commit capture index built
	// with the arrival evidence: a scope present names the exact instant
	// its chain committed in-window, and a scope absent names nothing
	// (r50 (B)). arrivalCapFailed marks a poisoned build (engine error,
	// truncation, corruption, cancellation): every tie query of that
	// build answers Incomplete (r50 (C), D157), never a silent absence.
	arrivalCommitIdx *pkMoveLinkCommitIndex
	arrivalCapFailed bool
	// streamLinkCommits, when non-nil (unit-test fabric), replaces the
	// engine capture for one window: it feeds (scope, commit instant)
	// records through the same admission fold the engine path uses.
	streamLinkCommits func(
		ctx context.Context,
		w pkMoveLinkTableWindow,
		from, end types.TS,
		accept func(scope pkMoveTxnScope, cts types.TS) error,
	) error
}

// pkMoveLinkCommitIndex records, per transaction scope, the exact commit
// instant the D156 capture observed for it inside the apply windows.
// The two lanes replicate pkMoveTxnScope.matches' asymmetry: byTxn holds
// only durable-txn scopes, byTSNone only unknown-transaction scopes
// keyed by create timestamp. One transaction commits once, so a lane
// re-observing a scope at a DIFFERENT instant is corruption and poisons
// the build (r50 (C)); a boundary commit shared by adjacent generations
// dedupes to the one true instant (r51 (F)).
type pkMoveLinkCommitIndex struct {
	byTxn    map[[catalog.DataBranchUpdateLinkTxnIDBytes]byte]types.TS
	byTSNone map[types.TS]types.TS
}

// add folds one captured scope->commit record; it reports a conflict
// when a lane already bound the scope to a different instant.
func (idx *pkMoveLinkCommitIndex) add(scope pkMoveTxnScope, cts types.TS) (conflict bool) {
	if scope.hasTxn {
		if prev, ok := idx.byTxn[scope.txnID]; ok {
			if !prev.EQ(&cts) {
				return true
			}
		} else {
			idx.byTxn[scope.txnID] = cts
		}
		return false
	}
	if prev, ok := idx.byTSNone[scope.writeTS]; ok {
		if !prev.EQ(&cts) {
			return true
		}
	} else {
		idx.byTSNone[scope.writeTS] = cts
	}
	return false
}

// commits names the in-window commit instant(s) a FAILED chain scope can
// still resolve to: the txn lane the scope's durable identity binds,
// plus the unknown-transaction lane its create timestamp binds — the
// same asymmetric pairing pkMoveTxnScope.matches applies to links. A
// scope present in neither lane committed outside the apply windows and
// names nothing.
func (idx *pkMoveLinkCommitIndex) commits(scope pkMoveTxnScope, out map[types.TS]struct{}) {
	if idx == nil || out == nil {
		return
	}
	if scope.hasTxn {
		if cts, ok := idx.byTxn[scope.txnID]; ok {
			out[cts] = struct{}{}
		}
	}
	if cts, ok := idx.byTSNone[scope.writeTS]; ok {
		out[cts] = struct{}{}
	}
}

// pkMoveArrivalVerdict is the continuity tie's answer to "did a RECORDED
// pk move land at the anchor key committed at exactly the anchor
// version's birth instant".
type pkMoveArrivalVerdict int

const (
	// pkMoveArrivalNone: complete evidence shows no recorded grounded
	// arrival at (key, instant) — the bare D87/D94 tie keeps its meaning.
	pkMoveArrivalNone pkMoveArrivalVerdict = iota
	// pkMoveArrivalRecorded: a recorded, grounded arrival chain lands at
	// the anchor key in the tie's transaction — the standing version
	// descends from the chain's origin lineage, not from the key's own
	// LCA lineage.
	pkMoveArrivalRecorded
	// pkMoveArrivalIncomplete: arrival evidence exists but cannot be
	// fully attributed at the queried instant (ambiguous predecessors,
	// contradictory generations, a cycle, more hops than the walk cap,
	// or a window whose links were never scanned whole so arrivals are
	// unenumerable) — the caller must fail closed for that instant
	// rather than accept or reject the tie.
	pkMoveArrivalIncomplete
)

// pkMoveArrivalEntry records, for one landed key, every grounded commit
// instant at which a recorded arrival chain reached it, plus the
// instants whose arrival evidence is unattributable (r58 review F2:
// scoping replaces the former key-wide incomplete flag — attribution
// evidence is instant-shaped, so a failure deciding one commit instant
// must not poison verdicts for instants the evidence never touches).
type pkMoveArrivalEntry struct {
	instants    map[types.TS]struct{}
	undecidable map[types.TS]struct{}
}

// newPkMoveLinkProvider returns nil when the link source is unusable for
// this diff (no executor or no window), meaning every anchor fails closed.
func newPkMoveLinkProvider(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	windows []pkMoveLinkTableWindow,
) *pkMoveLinkProvider {
	if bh == nil || len(windows) == 0 {
		return nil
	}
	usable := windows[:0]
	seen := make(map[uint64]struct{}, len(windows))
	for _, w := range windows {
		if w.tableID == 0 {
			continue
		}
		if _, dup := seen[w.tableID]; dup {
			continue
		}
		seen[w.tableID] = struct{}{}
		usable = append(usable, w)
	}
	if len(usable) == 0 {
		return nil
	}
	return &pkMoveLinkProvider{
		ctx:             ctx,
		ses:             ses,
		bh:              bh,
		windows:         append([]pkMoveLinkTableWindow(nil), usable...),
		byRow:           make(map[string]pkMoveLink),
		byPK:            make(map[string][]pkMoveLink),
		probed:          make(map[string]struct{}),
		poisoned:        make(map[string]struct{}),
		fullWindows:     make(map[uint64][2]uint64),
		overflowWindows: make(map[uint64][2]uint64),
		overflowKeys:    make(map[string]struct{}),
	}
}

// disable flips the provider off after a soft failure (row cap overflow,
// probe-set overflow); every remaining anchor resolves as unproven and all
// successors report incomplete evidence.
func (p *pkMoveLinkProvider) disable(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.disableLocked(reason)
}

// disableLocked releases the caches under the lock so a disabled provider
// stops holding catalog history alive (resource bound), and records the
// tables it served for diagnostics.
func (p *pkMoveLinkProvider) disableLocked(reason string) {
	if p.disabled {
		return
	}
	p.disabled = true
	p.byRow = make(map[string]pkMoveLink)
	p.byPK = make(map[string][]pkMoveLink)
	p.probed = make(map[string]struct{})
	p.poisoned = make(map[string]struct{})
	p.fullWindows = make(map[uint64][2]uint64)
	p.overflowWindows = make(map[uint64][2]uint64)
	p.overflowKeys = make(map[string]struct{})
	ids := make([]uint64, 0, len(p.windows))
	for _, w := range p.windows {
		ids = append(ids, w.tableID)
	}
	logutil.Warn(
		"DataBranch-PkMoveLink-ProviderDisabled",
		zap.Uint64s("table-ids", ids),
		zap.String("reason", reason),
	)
}

// pkMoveLinkTableExists probes whether this account carries the catalog
// table; tenants created before v4.0.15 fail closed with no error.
func pkMoveLinkTableExists(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
) (bool, error) {
	probe, err := runSql(ctx, ses, bh, fmt.Sprintf(
		"select relname from mo_catalog.mo_tables where reldatabase = '%s' and relname = '%s'",
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK,
	), nil, nil)
	if err != nil {
		return false, err
	}
	exists := false
	probe.ReadRows(func(rowCnt int, _ []*vector.Vector) bool {
		exists = rowCnt > 0
		return false
	})
	probe.Close()
	return exists, nil
}

// keyAnsweredLocked reports whether one (window, old-pk) pair already has
// a terminal probe answer: the window is fully cached, the key probed
// clean, or the key marked overflow (terminal-incomplete evidence that no
// re-probe can repair within this diff).
func (p *pkMoveLinkProvider) keyAnsweredLocked(w pkMoveLinkTableWindow, key []byte) bool {
	if _, ok := p.fullWindows[w.tableID]; ok {
		return true
	}
	pk := pkMoveLinkProbedKey(w.tableID, key)
	if _, done := p.probed[pk]; done {
		return true
	}
	_, over := p.overflowKeys[pk]
	return over
}

// prefetch batch-loads the links whose old key sits in keys (the chunk's
// tombstone keys), so hop-1 anchor probes and successor lookups become map
// lookups. Every window is probed for every key; a multi-generation branch
// keeps each generation's links under that generation's table id.
func (p *pkMoveLinkProvider) prefetch(keys [][]byte) error {
	if p == nil {
		return nil
	}
	var todo [][]byte
	p.mu.Lock()
	if p.disabled {
		p.mu.Unlock()
		return nil
	}
	for _, k := range keys {
		if len(k) == 0 {
			continue
		}
		allAnswered := true
		for _, w := range p.windows {
			if !p.keyAnsweredLocked(w, k) {
				allAnswered = false
				break
			}
		}
		if allAnswered {
			continue
		}
		todo = append(todo, append([]byte(nil), k...))
	}
	p.mu.Unlock()
	for _, w := range p.windows {
		for start := 0; start < len(todo); start += pkMoveLinkProbeChunk {
			end := start + pkMoveLinkProbeChunk
			if end > len(todo) {
				end = len(todo)
			}
			if err := p.probeOldPKs(w, todo[start:end]); err != nil {
				return err
			}
		}
	}
	return nil
}

// prefetchContinuations prefetches the anchor keys and then, round by
// round, the new keys of every link the cache picked up: a chain walk
// probes the provenance of each hop's LANDED key, and those keys are not
// among the anchor old keys. Without this, every hop's successor lookup
// falls back to an unbatched single-key probe — one SQL round trip per
// anchor — which at diff scale (tens of thousands of moves) costs far
// longer than the rest of the merge combined. Rounds are capped at the
// walk's hop cap plus one: any key still un-probed beyond that keeps the
// single-key fallback, which stays correct (and is unreachable in
// practice because walks give up at the same cap).
func (p *pkMoveLinkProvider) prefetchContinuations(keys [][]byte) error {
	if p == nil {
		return nil
	}
	front := keys
	for round := 0; round <= pkMoveLinkMaxChain && len(front) > 0; round++ {
		if err := p.prefetch(front); err != nil {
			return err
		}
		p.mu.Lock()
		if p.disabled {
			p.mu.Unlock()
			return nil
		}
		next := make([][]byte, 0, len(front))
		seen := make(map[string]struct{})
		for _, links := range p.byPK {
			for _, link := range links {
				if len(link.newPK) == 0 {
					continue
				}
				ks := string(link.newPK)
				if _, dup := seen[ks]; dup {
					continue
				}
				allAnswered := true
				for _, w := range p.windows {
					if !p.keyAnsweredLocked(w, link.newPK) {
						allAnswered = false
						break
					}
				}
				if allAnswered {
					continue
				}
				seen[ks] = struct{}{}
				next = append(next, link.newPK)
			}
		}
		p.mu.Unlock()
		front = next
	}
	return nil
}

// probeOldPKs fetches the links of one window covering the old pks in keys
// by scanning the window's whole link_id range once (the SELECT comment
// below explains why per-key prefix ranges were dropped). Identity
// re-checking stays as before: rows merge under their full old-pk bytes,
// so sha collisions can never alias another key's lineage into a probe
// answer. The SQL read runs lock-free; merging is idempotent, so
// concurrent probes of the same window stay correct. The SELECT is
// LIMITed to the remaining row budget + 1: hitting the limit means
// history continues past what this reader may hold, so the provider
// disables itself (fail closed) rather than treating a truncated read as
// complete evidence.
func (p *pkMoveLinkProvider) probeOldPKs(window pkMoveLinkTableWindow, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	var todo [][]byte
	p.mu.Lock()
	if p.disabled {
		p.mu.Unlock()
		return nil
	}
	sig := [2]uint64{window.startPhys, window.endPhys}
	if have, ok := p.fullWindows[window.tableID]; ok {
		if have == sig {
			p.mu.Unlock()
			return nil
		}
		// Same physical table probed with different time bounds: the
		// cache covers a different interval than this request needs.
		p.disableLocked(fmt.Sprintf(
			"table %d probed with conflicting windows [%d,%d) vs [%d,%d)",
			window.tableID, have[0], have[1], window.startPhys, window.endPhys))
		p.mu.Unlock()
		return nil
	}
	overflowMode := false
	if have, ok := p.overflowWindows[window.tableID]; ok {
		if have != sig {
			// Per-key probing answered this table under different time
			// bounds; those answers cannot cover this interval either.
			p.disableLocked(fmt.Sprintf(
				"table %d probed with conflicting windows [%d,%d) vs [%d,%d)",
				window.tableID, have[0], have[1], window.startPhys, window.endPhys))
			p.mu.Unlock()
			return nil
		}
		overflowMode = true
	}
	for _, k := range keys {
		if len(k) == 0 {
			continue
		}
		pk := pkMoveLinkProbedKey(window.tableID, k)
		if _, done := p.probed[pk]; done {
			continue
		}
		if _, over := p.overflowKeys[pk]; over {
			// Terminal-incomplete evidence: no probe bound within this
			// diff can complete this key, so re-reading the same
			// oversized history only wastes the round trip.
			continue
		}
		todo = append(todo, k)
	}
	budget := pkMoveLinkReadBudget(window.tableID) - p.rows
	p.mu.Unlock()
	if len(todo) == 0 {
		return nil
	}
	if overflowMode {
		return p.probeKeysIndividually(window, todo)
	}
	if budget < 0 {
		// The caches are full: leave these keys UNPROBED — their
		// verdicts report incomplete evidence — instead of disabling
		// the provider and dragging every already-answered key down
		// with them (r23 review F4).
		return nil
	}
	// One whole-window range scan covers every requested old key — and the
	// continuation keys every chain hop lands on. Keying one link_id
	// prefix range per old key (see pkMoveLinkPrefixRange for why
	// secondary indexes are off limits) concentrates the cost in
	// predicate planning, which grows superlinearly with the number of
	// ranges: a 512-key chunk of (sha, sha+1) ranges cost ~80s a query
	// here (tens of thousands of anchors made such merges effectively
	// unbounded), while one window scan reads the same rows in ~0.3s.
	lo, hi, ok := pkMoveLinkPrefixRange(window.tableID, nil)
	if !ok {
		p.disable(fmt.Sprintf("table %d link_id prefix overflows", window.tableID))
		return nil
	}
	// The + 1 makes "history truncated at the budget" observable.
	// The scan spans the table's FULL history through endPhys: write_ts is
	// the recording transaction's CREATE timestamp, so a lower bound here
	// would filter out a straddling transaction (created before the
	// window's collect start, committed inside it) and its departures
	// would then read as complete absence downstream. Stream commit
	// instants decide window membership during the walk, so the wider
	// scan only adds provable evidence.
	links, err := p.runProbeSQL(pkMoveLinkProbeSQL(lo, hi, window.endPhys, budget+1))
	if err != nil {
		return err
	}

	if len(links) > budget {
		// The LIMIT fired: history continues past the reader budget.
		// Never mistake this truncated prefix for complete evidence —
		// discard it wholesale and narrow the window to per-key probes
		// of exactly the requested keys instead of disabling the whole
		// provider: unrelated keys' history must not abort a diff asking
		// about only a few of them (r23 review F4).
		p.mu.Lock()
		if p.disabled {
			p.mu.Unlock()
			return nil
		}
		p.overflowWindows[window.tableID] = sig
		p.mu.Unlock()
		logutil.Warn(
			"DataBranch-PkMoveLink-WindowOverflow",
			zap.Uint64("table-id", window.tableID),
			zap.Int("budget", budget),
			zap.String("action", "narrowing to per-key probing"),
		)
		return p.probeKeysIndividually(window, todo)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.disabled {
		return nil
	}
	p.mergeProbedLinksLocked(links)
	if p.rows > pkMoveLinkMaxCachedRows || len(p.probed)+len(todo) > pkMoveLinkMaxCachedRows {
		p.disableLocked(fmt.Sprintf("cached link rows exceed %d", pkMoveLinkMaxCachedRows))
		return nil
	}
	// The scan is all-or-nothing: the LIMIT did not fire, so the window
	// answered completely. Every later key of this window — anchors,
	// continuation hops, successors — is served from this cache.
	p.fullWindows[window.tableID] = sig
	for _, k := range todo {
		p.probed[pkMoveLinkProbedKey(window.tableID, k)] = struct{}{}
	}
	return nil
}

// pkMoveLinkProbeSQL builds one bounded link probe over a link_id prefix
// range [lo, hi). The +1 on the LIMIT makes "history truncated at the
// budget" observable to the caller. The scan spans the table's FULL
// history through endPhys: write_ts is the recording transaction's CREATE
// timestamp, so a lower bound here would filter out a straddling
// transaction (created before the window's collect start, committed
// inside it) and its departures would then read as complete absence
// downstream. Stream commit instants decide window membership during the
// walk, so the wider scan only adds provable evidence.
func pkMoveLinkProbeSQL(lo, hi []byte, endPhys uint64, limit int) string {
	return fmt.Sprintf(
		"select %s, %s, %s, %s, %s, %s, %s from %s.%s where %s >= x'%x' and %s < x'%x' and %s <= %d order by %s limit %d",
		catalog.DataBranchUpdateLinkColOldRow,
		catalog.DataBranchUpdateLinkColNewRow,
		catalog.DataBranchUpdateLinkColOldPK,
		catalog.DataBranchUpdateLinkColNewPK,
		catalog.DataBranchUpdateLinkColWriteTS,
		catalog.DataBranchUpdateLinkColTxnID,
		catalog.DataBranchUpdateLinkColTableID,
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK,
		catalog.DataBranchUpdateLinkColLinkID, lo,
		catalog.DataBranchUpdateLinkColLinkID, hi,
		catalog.DataBranchUpdateLinkColWriteTS, endPhys,
		catalog.DataBranchUpdateLinkColLinkID, limit,
	)
}

// runProbeSQL issues one bounded link probe and decodes its rows.
func (p *pkMoveLinkProvider) runProbeSQL(sql string) ([]pkMoveLink, error) {
	ret, err := runSql(p.ctx, p.ses, p.bh, sql, nil, nil)
	if err != nil {
		return nil, err
	}
	defer ret.Close()

	var links []pkMoveLink
	var loadErr error
	ret.ReadRows(func(rowCnt int, cols []*vector.Vector) bool {
		if len(cols) != 7 {
			loadErr = moerr.NewInternalErrorNoCtxf(
				"unexpected pk-move link probe width %d", len(cols))
			return false
		}
		for i := 0; i < rowCnt; i++ {
			link, ok := p.decodeRow(cols, i, &loadErr)
			if !ok {
				return false
			}
			links = append(links, link)
		}
		return true
	})
	if loadErr != nil {
		return nil, loadErr
	}
	return links, nil
}

// mergeProbedLinksLocked folds one decoded probe page into the caches:
// duplicate (row id, old pk) rows claiming different lineage poison the
// anchor; every row joins the by-old-pk successor list. The merge is
// idempotent for identical rows so concurrent probes of one window —
// whole-range scans and per-key fallback pages alike — fold cleanly.
func (p *pkMoveLinkProvider) mergeProbedLinksLocked(links []pkMoveLink) {
	for _, link := range links {
		rowKey := pkMoveLinkRowKey(link.oldRowID, link.oldPK)
		if prev, dup := p.byRow[rowKey]; dup {
			if !samePkMoveLinkIdentity(prev, link) {
				// Two tables claim different lineage for the same
				// (row id, old pk): the stream row this key pairs with is
				// not attributable, so the anchor poisons (fail closed).
				delete(p.byRow, rowKey)
				p.poisoned[rowKey] = struct{}{}
			}
		} else if _, bad := p.poisoned[rowKey]; !bad {
			p.byRow[rowKey] = link
		}
		p.byPK[string(link.oldPK)] = append(p.byPK[string(link.oldPK)], link)
		p.rows++
	}
}

// probeKeysIndividually loads the requested keys' links one key at a time
// through the (sha, sha+1) link_id prefix range of each old pk. It is the
// overflow mode of a window whose whole-range scan crossed the row
// budget: the diff then pays one tight range scan per requested key —
// exactly the anchor and continuation keys it asks about — instead of
// materializing the table's entire history to answer them (r23 review
// F4). Per-key results keep the whole-window contract: the row-budget
// LIMIT rides every scan (+1 to observe truncation), a completed scan
// marks the key probed (a clean scan with zero rows proves absence for
// that key), and a scan whose LIMIT fires poisons ONLY that key — its
// own history crosses the budget — as terminal-incomplete evidence. The
// whole-range scan stays the primary plan for cost reasons (per-key
// predicate planning grows superlinearly with range count; see
// probeOldPKs), which is why this path serves only the overflow
// fallback's small requested sets.
func (p *pkMoveLinkProvider) probeKeysIndividually(
	window pkMoveLinkTableWindow, keys [][]byte,
) error {
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		sha := sha256.Sum256(key)
		lo, hi, ok := pkMoveLinkPrefixRange(window.tableID, sha[:])
		probedKey := pkMoveLinkProbedKey(window.tableID, key)
		if !ok {
			p.mu.Lock()
			if !p.disabled {
				p.overflowKeys[probedKey] = struct{}{}
			}
			p.mu.Unlock()
			continue
		}
		p.mu.Lock()
		if p.disabled {
			p.mu.Unlock()
			return nil
		}
		if _, done := p.probed[probedKey]; done {
			p.mu.Unlock()
			continue
		}
		if _, over := p.overflowKeys[probedKey]; over {
			p.mu.Unlock()
			continue
		}
		budget := pkMoveLinkReadBudget(window.tableID) - p.rows
		p.mu.Unlock()
		if budget < 0 {
			// Cache space is exhausted: this key stays unprobed and its
			// verdicts report incomplete evidence, while every
			// already-answered key keeps its answer.
			continue
		}
		links, err := p.runProbeSQL(pkMoveLinkProbeSQL(lo, hi, window.endPhys, budget+1))
		if err != nil {
			return err
		}
		p.mu.Lock()
		if p.disabled {
			p.mu.Unlock()
			return nil
		}
		if len(links) > budget {
			// This key's OWN history crosses the budget: it alone fails
			// closed; the provider and every other key stay served.
			p.overflowKeys[probedKey] = struct{}{}
			p.mu.Unlock()
			logutil.Warn(
				"DataBranch-PkMoveLink-KeyOverflow",
				zap.Uint64("table-id", window.tableID),
				zap.Int("budget", budget),
				zap.String("action", "key fails closed as incomplete"),
			)
			continue
		}
		p.mergeProbedLinksLocked(links)
		p.probed[probedKey] = struct{}{}
		p.mu.Unlock()
	}
	return nil
}

// anyPkMoveLinkInWindow reports whether ANY link row exists inside the
// provider's windows. The full-window cache answers for generations a
// probe already scanned completely (an un-LIMIT-ed scan with zero rows
// proves absence), one LIMIT 1 range scan covers every other window with
// the same bounds probeOldPKs uses, and any cached link proves existence
// outright. A disabled provider can no longer disprove existence and
// answers as though links exist: paired with capped stream evidence (see
// resolveAndBuildPkMoveFolds) that is the fail-closed arm.
func (p *pkMoveLinkProvider) anyPkMoveLinkInWindow() (bool, error) {
	if p == nil {
		return false, nil
	}
	p.mu.Lock()
	if p.disabled {
		// The disable log already recorded why the evidence space is
		// incomplete; existence can no longer be disproved from here.
		p.mu.Unlock()
		return true, nil
	}
	if len(p.byPK) > 0 {
		p.mu.Unlock()
		return true, nil
	}
	uncovered := make([]pkMoveLinkTableWindow, 0, len(p.windows))
	for _, w := range p.windows {
		sig, ok := p.fullWindows[w.tableID]
		if !ok || sig != [2]uint64{w.startPhys, w.endPhys} {
			uncovered = append(uncovered, w)
		}
	}
	p.mu.Unlock()
	for _, w := range uncovered {
		exists, err := p.probeAnyLinkInWindow(w)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
	}
	return false, nil
}

// probeAnyLinkInWindow runs the LIMIT 1 existence scan for one window.
// The bounds mirror probeOldPKs exactly: the full link_id prefix range of
// the window's table with write_ts capped at the window's end, because a
// lower bound would hide straddling transactions whose commits landed
// inside the window.
func (p *pkMoveLinkProvider) probeAnyLinkInWindow(window pkMoveLinkTableWindow) (bool, error) {
	lo, hi, ok := pkMoveLinkPrefixRange(window.tableID, nil)
	if !ok {
		// The prefix overflow shape disables ordinary probing too;
		// existence cannot be disproved.
		return true, nil
	}
	sql := fmt.Sprintf(
		"select %s from %s.%s where %s >= x'%x' and %s < x'%x' and %s <= %d limit 1",
		catalog.DataBranchUpdateLinkColTableID,
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK,
		catalog.DataBranchUpdateLinkColLinkID, lo,
		catalog.DataBranchUpdateLinkColLinkID, hi,
		catalog.DataBranchUpdateLinkColWriteTS, window.endPhys,
	)
	ret, err := runSql(p.ctx, p.ses, p.bh, sql, nil, nil)
	if err != nil {
		return false, err
	}
	defer ret.Close()
	exists := false
	ret.ReadRows(func(rowCnt int, _ []*vector.Vector) bool {
		exists = rowCnt > 0
		return false
	})
	return exists, nil
}

// decodeRow parses one link row of the probe projection
// (old_row_id, new_row_id, old_pk, new_pk, write_ts, txn_id, table_id),
// verifying the row-id and transaction-id widths; new_row_id may be NULL
// (identity unrecorded), which the walk treats as unprovable, and an
// all-zero txn_id marks "identity unknown", degrading same-transaction
// matching to the create-timestamp fallback. Full-bytes equality of the
// old pk against the probed sha list is implicit: the range select
// already restricted to the requested shas.
func (p *pkMoveLinkProvider) decodeRow(
	cols []*vector.Vector, i int, loadErr *error,
) (link pkMoveLink, ok bool) {
	rowIDBytes := cols[0].GetBytesAt(i)
	if len(rowIDBytes) != types.RowidSize {
		*loadErr = moerr.NewInternalErrorNoCtxf(
			"unexpected pk-move link rowid width %d", len(rowIDBytes))
		return link, false
	}
	copy(link.oldRowID[:], rowIDBytes)
	if !cols[1].IsNull(uint64(i)) {
		newRowBytes := cols[1].GetBytesAt(i)
		if len(newRowBytes) != types.RowidSize {
			*loadErr = moerr.NewInternalErrorNoCtxf(
				"unexpected pk-move link new rowid width %d", len(newRowBytes))
			return link, false
		}
		copy(link.newRowID[:], newRowBytes)
		link.hasNewRowID = true
	}
	link.oldPK = append([]byte(nil), cols[2].GetBytesAt(i)...)
	link.newPK = append([]byte(nil), cols[3].GetBytesAt(i)...)
	writeTS := vector.MustFixedColNoTypeCheck[uint64](cols[4])[i]
	link.writeTS = types.BuildTS(int64(writeTS), 0)
	txnBytes := cols[5].GetBytesAt(i)
	if len(txnBytes) != catalog.DataBranchUpdateLinkTxnIDBytes {
		*loadErr = moerr.NewInternalErrorNoCtxf(
			"unexpected pk-move link txn-id width %d", len(txnBytes))
		return link, false
	}
	copy(link.txnID[:], txnBytes)
	link.hasTxnID = link.txnID != [catalog.DataBranchUpdateLinkTxnIDBytes]byte{}
	return link, true
}

// byRowProbe resolves the exact hop-1 link for one anchor. A poisoned key
// (two tables claiming different lineage for one (row id, old pk)) and a
// disabled provider both resolve as unproven.
func (p *pkMoveLinkProvider) byRowProbe(oldPK []byte, rowID types.Rowid) (pkMoveLink, bool) {
	link, hit, _ := p.byRowProbeVerdict(oldPK, rowID)
	return link, hit
}

// keyCompleteLocked reports whether the probe space of one old-pk key is
// fully decided: each window answered completely (a full-window cache,
// or a clean per-key probe in overflow mode), while ANY window's
// terminal overflow mark on this key — or a probe gap on a window that
// never answered — makes the key's evidence INCOMPLETE. Completeness is
// scoped per key (r23 review F4): unrelated overflow history narrows
// nothing here.
func (p *pkMoveLinkProvider) keyCompleteLocked(key []byte) bool {
	for _, w := range p.windows {
		if _, ok := p.fullWindows[w.tableID]; ok {
			continue
		}
		pk := pkMoveLinkProbedKey(w.tableID, key)
		if _, over := p.overflowKeys[pk]; over {
			return false
		}
		if _, done := p.probed[pk]; !done {
			return false
		}
	}
	return true
}

// byRowProbeVerdict is byRowProbe plus the probe-space completeness the
// apply-level taxonomy needs: a disabled provider, a poisoned key, or a
// key whose own evidence could not be fully read means the answer must
// NOT be treated as "no update intent" — recorded provenance may exist
// that the probe could not see.
func (p *pkMoveLinkProvider) byRowProbeVerdict(
	oldPK []byte, rowID types.Rowid,
) (link pkMoveLink, hit bool, complete bool) {
	if p == nil {
		return pkMoveLink{}, false, true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.disabled {
		return pkMoveLink{}, false, false
	}
	rowKey := pkMoveLinkRowKey(rowID, oldPK)
	if _, bad := p.poisoned[rowKey]; bad {
		return pkMoveLink{}, false, false
	}
	if !p.keyCompleteLocked(oldPK) {
		return pkMoveLink{}, false, false
	}
	link, ok := p.byRow[rowKey]
	return link, ok, true
}

// anchorArrivalVerdict reports whether recorded provenance shows a pk
// move CHAIN landing at the anchor key committed at exactly the anchor
// version's birth instant, attributing the arrival through recorded
// same-transaction identity hops — including intermediate keys NETTED
// inside the transaction that never reached the committed stream (r53
// F1: a 2->4->1 chain grounds its arrival at key 1 on the committed
// departure at key 2 even though key 4 carries no tombstone). It runs
// only at the load-bearing continuity tie, where the anchor version was
// born at the anchor key at the previous key-holder's death instant. A
// version born there by same-key re-creation (a stable-key update, or
// the matching same-transaction delete+insert of D87/D94) is never the
// destination of a recorded move, while a version born by another
// lineage's pk move always is: the chain's origin deletes a committed
// image whose deleting tombstone commits in the same instant, and one
// committed image per key per instant makes the (key, instant) pairing
// exact — the anchor version IS the landed version, and its actual LCA
// lineage continues at the arrival's origin, whose own anchor walks
// through this very point. Chain hops bind the recorded workspace
// identity (new_row_id of the predecessor == old_row_id of the
// successor), which one committed image per key per commit keeps
// unambiguous; a link without a staged replacement identity (the S3
// route) binds by (key, scope) alone, unique under the same rule. A
// chain whose origin never committed a tombstone here — a
// same-transaction insert-then-move — stays the D87-approximated case
// the stream cannot attribute (fail same as the unrecorded
// reincarnation, never wrong data). A miss proves "no recorded grounded
// arrival" for every tie the walk can accept: a grounded chain's origin
// departure commits in the birth instant, hence inside the collect
// window, hence among the anchor prefetch keys, and the continuation
// prefetch rounds its landed keys into the cache — when every window was
// scanned WHOLE. An overflow-narrowed (or never-completed) window holds
// only per-key OLD-key probes, which cannot enumerate the moves LANDING
// at a probed key (link_id's prefix is reachable in the old-key
// direction only), so a cache miss cannot prove the tie's absence:
// arrivalWindowsCompleteLocked fails the check closed instead
// (r54 review F1). Ambiguous, contradictory, cyclic, or
// over-long evidence marks the in-window instants its recording
// transaction could still have committed at — derived from committed
// tombstones at the chain's own recorded departure keys (r59 review F1:
// the recorded write_ts is a creation time, never a commit instant) —
// and, for corrupt grounding, the competing tombstone instants as well
// (r58 review F2) — undecidable, failing
// that tie closed while instants the evidence cannot decide keep their
// verdict; attribution never guesses.
func (p *pkMoveLinkProvider) anchorArrivalVerdict(view pkMoveStreamView, anchorPK []byte, anchorBirth types.TS) pkMoveArrivalVerdict {
	if p == nil {
		return pkMoveArrivalNone
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.disabled {
		// Unreachable: the anchor probe verdict fails closed first. Keep
		// the fail-same answer so a hypothetical reorder cannot turn a
		// dark provider into a proven-disqualifying one either.
		return pkMoveArrivalNone
	}
	if !p.arrivalWindowsCompleteLocked() {
		// Arrival evidence cannot rule on this tie. Under narrowed
		// probing a miss is not a proven absence (an incoming chain's
		// departures were never scanned), and a partial chain cannot
		// prove its own completeness either — so a recorded hit is not
		// trusted in this mode; both answer incomplete (r54 review F1).
		return pkMoveArrivalIncomplete
	}
	p.ensureArrivalIndexLocked(view)
	if p.arrivalCapFailed {
		// The scope->commit capture behind this build could not be
		// certified complete (engine error, truncation, corruption, or
		// cancellation): poisoning is build-wholesale (r50 (C)) — never
		// a silent downgrade to absence.
		return pkMoveArrivalIncomplete
	}
	ent, ok := p.arrivals[string(anchorPK)]
	if !ok {
		return pkMoveArrivalNone
	}
	if _, und := ent.undecidable[anchorBirth]; und {
		// Evidence at exactly the queried instant could not be
		// attributed: fail closed for this instant alone (r58 F2).
		return pkMoveArrivalIncomplete
	}
	if _, hit := ent.instants[anchorBirth]; hit {
		return pkMoveArrivalRecorded
	}
	return pkMoveArrivalNone
}

// arrivalWindowsCompleteLocked reports whether the provider saw every
// link of every registered window: only a whole-window scan (marked in
// fullWindows with a matching signature) enumerates both the departures
// AND the arrivals a tie can ask about. An overflow-narrowed or
// never-completed window caches only per-key OLD-key probes — arrivals
// AT a probed key are unenumerable through link_id's old-key prefix — so
// the arrival index built from it cannot prove the ABSENCE the D87/D94
// tie would rely on (r54 review F1). A provider carrying no windows
// (unit-test fabrications) is complete by construction.
func (p *pkMoveLinkProvider) arrivalWindowsCompleteLocked() bool {
	for _, w := range p.windows {
		sig := [2]uint64{w.startPhys, w.endPhys}
		if have, ok := p.fullWindows[w.tableID]; !ok || have != sig {
			return false
		}
	}
	return true
}

// arrivalEvidenceStats exposes the evidence-build instrumentation for
// the scaling regression.
func (p *pkMoveLinkProvider) arrivalEvidenceStats() (builds, scans uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.arrivalBuilds, p.arrivalScans
}

// departureEvidenceStats exposes the successor-lookup instrumentation for
// the chain-walk scaling regression.
func (p *pkMoveLinkProvider) departureEvidenceStats() (builds, scans uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.depBuilds, p.depScans
}

// departureLinkCount reports how many departure links the probe space
// currently holds under key. The pairing index pins this count at build
// time: the probe cache is append-only, so any later growth marks the
// built index stale and forces a rebuild (r56 review F2).
func (p *pkMoveLinkProvider) departureLinkCount(key []byte) int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.byPK[string(key)])
}

// pkMoveArrivalBuild is one arrival-index build's scratch: the landed-key
// grouping plus the two sub-indexes that keep ONE build linear (r54
// review F3). predIdx subdivides each landed key's candidates by
// transaction scope (byTS for every link, byTxn for durable-txn links —
// together replicating pkMoveTxnScope.matches' asymmetric lanes), so an
// attribution consults only its own scope's candidates instead of every
// move that ever landed at a shared temporary key. tombs lazily indexes
// one key's tombstone references by exact row identity at most once per
// build, so repeated departures from a high-churn key cost one lookup
// per chain hop instead of a full history rescan per link.
type pkMoveArrivalBuild struct {
	lands   map[string][]pkMoveLink
	predIdx map[string]*pkMoveArrivalPredIndex
	tombs   map[string]map[types.Rowid][]types.TS
	// failedArrivals memoizes each failed chain scope's feasible commit
	// instants for the build's lifetime: the link cache and stream view
	// are immutable while the build stands (r59 review F1).
	failedArrivals map[pkMoveTxnScope]map[types.TS]struct{}
}

// pkMoveArrivalPredIndex subdivides each landed key's predecessor
// candidates by transaction scope lane AND by exact replacement identity
// (r55 review F1): the three lanes replicate pkMoveTxnScope.matches'
// asymmetry (txnIdx holds only durable-txn links, tsNoTxnIdx only
// unknown-transaction links, tsAllIdx every link of one timestamp), and
// inside a lane the predecessor lookup binds by the hop's exact consumed
// identity, while the ambiguous and contradicted verdicts read
// precomputed counts — never a per-chain scan of a shared scratch key's
// whole scope bucket.
type pkMoveArrivalPredIndex struct {
	txnIdx     map[[catalog.DataBranchUpdateLinkTxnIDBytes]byte]*pkMoveArrivalScopeBucket
	tsAllIdx   map[types.TS]*pkMoveArrivalScopeBucket
	tsNoTxnIdx map[types.TS]*pkMoveArrivalScopeBucket
}

// pkMoveArrivalScopeBucket indexes one scope lane's landed links by their
// staged replacement identity, preserving the classification inputs of
// the raw candidate scan: identity-less (S3-route) candidates separately,
// and the lane's total identity-carrying count for the contradicted
// verdict. Duplicates land side by side, so competing exact recordings
// stay visible and fail closed.
type pkMoveArrivalScopeBucket struct {
	byID      map[types.Rowid][]int
	idless    []int
	carryingN int
}

func (bk *pkMoveArrivalScopeBucket) add(pos int, link *pkMoveLink) {
	if !link.hasNewRowID {
		bk.idless = append(bk.idless, pos)
		return
	}
	bk.carryingN++
	if bk.byID == nil {
		bk.byID = make(map[types.Rowid][]int)
	}
	bk.byID[link.newRowID] = append(bk.byID[link.newRowID], pos)
}

// ensureArrivalIndexLocked builds (or reuses) the per-key arrival
// evidence index. One linear pass groups cached links by landed key;
// each landed key's candidates are then attributed backward through
// their recorded same-transaction chains. Historical links unrelated to
// any anchor cost one comparison each here, once per cache state —
// never another full pass per anchor (r53 F2). The index is rebuilt only
// when a probe merge changed the cache or the stream view differs.
// arrivalBuildCtxErr observes provider-context cancellation at the
// arrival-build iteration boundaries D157 (A) names: the per-key grouping
// walk, the failed-scope attribution loop, and the chain hops inside one
// attribution.
func (p *pkMoveLinkProvider) arrivalBuildCtxErr() error {
	if p.ctx == nil {
		return nil
	}
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	default:
		return nil
	}
}

func (p *pkMoveLinkProvider) ensureArrivalIndexLocked(view pkMoveStreamView) {
	if p.arrivals != nil && p.arrivalsRows == p.rows && p.arrivalsView == view {
		return
	}
	p.arrivalBuilds++
	p.captureArrivalCommitsLocked()
	abandonCancelledBuild := func() {
		// D157 (A): a cancelled build aborts fail-safe and adopts NO
		// partial verdict state — nothing publishes, the build-owned
		// state dies with this frame, and a live-context retry rebuilds
		// wholesale (capture re-arms and clears the poison).
		p.arrivalCommitIdx = nil
		p.arrivalCapFailed = true
	}
	b := &pkMoveArrivalBuild{
		lands:   make(map[string][]pkMoveLink, len(p.byPK)),
		predIdx: make(map[string]*pkMoveArrivalPredIndex, len(p.byPK)),
		tombs:   make(map[string]map[types.Rowid][]types.TS),
	}
	for _, links := range p.byPK {
		if err := p.arrivalBuildCtxErr(); err != nil {
			abandonCancelledBuild()
			return
		}
		for i := range links {
			p.arrivalScans++
			link := links[i]
			if len(link.newPK) == 0 {
				continue
			}
			k := string(link.newPK)
			b.lands[k] = append(b.lands[k], link)
			pi := b.predIdx[k]
			if pi == nil {
				pi = &pkMoveArrivalPredIndex{
					txnIdx:     make(map[[catalog.DataBranchUpdateLinkTxnIDBytes]byte]*pkMoveArrivalScopeBucket),
					tsAllIdx:   make(map[types.TS]*pkMoveArrivalScopeBucket),
					tsNoTxnIdx: make(map[types.TS]*pkMoveArrivalScopeBucket),
				}
				b.predIdx[k] = pi
			}
			pos := len(b.lands[k]) - 1
			all := pi.tsAllIdx[link.writeTS]
			if all == nil {
				all = &pkMoveArrivalScopeBucket{}
				pi.tsAllIdx[link.writeTS] = all
			}
			all.add(pos, &link)
			if link.hasTxnID {
				tb := pi.txnIdx[link.txnID]
				if tb == nil {
					tb = &pkMoveArrivalScopeBucket{}
					pi.txnIdx[link.txnID] = tb
				}
				tb.add(pos, &link)
			} else {
				nb := pi.tsNoTxnIdx[link.writeTS]
				if nb == nil {
					nb = &pkMoveArrivalScopeBucket{}
					pi.tsNoTxnIdx[link.writeTS] = nb
				}
				nb.add(pos, &link)
			}
		}
	}
	idx := make(map[string]*pkMoveArrivalEntry, len(b.lands))
	for key, cands := range b.lands {
		if err := p.arrivalBuildCtxErr(); err != nil {
			abandonCancelledBuild()
			return
		}
		ent := &pkMoveArrivalEntry{
			instants:    make(map[types.TS]struct{}),
			undecidable: make(map[types.TS]struct{}),
		}
		for i := range cands {
			if err := p.attributeArrivalLocked(view, b, key, i, ent); err != nil {
				abandonCancelledBuild()
				return
			}
		}
		if len(ent.instants) == 0 && len(ent.undecidable) == 0 {
			// Only ungrounded approximations reached this key: nothing
			// the tie may consult.
			continue
		}
		idx[key] = ent
	}
	if p.arrivalCapFailed {
		// A poisoned capture leaves no usable scope->commit evidence: an
		// engine error, truncation, corruption, or cancellation cannot
		// silently downgrade to absence either (r50 (C)).
		p.arrivalCommitIdx = nil
	}
	p.arrivals, p.arrivalsRows, p.arrivalsView = idx, p.rows, view
}

// captureArrivalCommitsLocked builds the D156 scope->commit index for
// this arrival build: every registered window's exact [from, end] is
// streamed once from the link relation's own committed change capture
// (or the unit-test fabric), admitted records fold INCREMENTALLY into
// the per-scope lanes, and the retained state is bounded by relevant
// scopes rather than relation-wide history (r51 (G)). Producer lanes:
// a provider with no capture source (no windows, or no engine and no
// fabric — the walk fixtures) adopts a complete EMPTY index whose
// absence is silence (r50 (D)); an engine error, ctx cancellation, or
// a scope observed committing twice poisons the build wholesale; a
// capacity-class error closes the capture and disables the provider
// with the capacity reason, taking the approved D51/D56 lane (r53
// amendment of (G)(v)). Cancellation or failure releases both the
// capture handle and the partial index; nothing survives the build.
func (p *pkMoveLinkProvider) captureArrivalCommitsLocked() {
	p.arrivalCommitIdx = nil
	p.arrivalCapFailed = false
	poison := func() {
		p.arrivalCommitIdx = nil
		p.arrivalCapFailed = true
	}
	// The capture context must serve recorded commit timestamps (r51
	// (E)): no snapshot-policy override resolves to the checkpoint
	// replay default, and no recovery resources may be attached. A
	// caller-side policy change cannot reintroduce substitution — the
	// dirty context is rejected outright (defense in depth).
	if engine.SnapshotReadPolicyFromContext(p.ctx) != engine.SnapshotReadPolicyCheckpointReplay ||
		engine.VisibleStateRecoveryResourcesFromContext(p.ctx) != nil {
		poison()
		return
	}
	idx := &pkMoveLinkCommitIndex{
		byTxn:    make(map[[catalog.DataBranchUpdateLinkTxnIDBytes]byte]types.TS),
		byTSNone: make(map[types.TS]types.TS),
	}
	scopes := make(map[pkMoveTxnScope]struct{}, len(p.byPK))
	for _, links := range p.byPK {
		for i := range links {
			scopes[pkMoveLinkTxnScope(&links[i])] = struct{}{}
		}
	}
	seenCtxCheck := func() error {
		if p.ctx == nil {
			return nil
		}
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
			return nil
		}
	}
	for _, w := range p.windows {
		from, end := w.captureBounds()
		accept := func(scope pkMoveTxnScope, cts types.TS) error {
			if err := seenCtxCheck(); err != nil {
				return err
			}
			if _, relevant := scopes[scope]; !relevant {
				return nil
			}
			if idx.add(scope, cts) {
				return moerr.NewInternalErrorNoCtxf(
					"pk-move link scope commits at two instants in-window")
			}
			return nil
		}
		var err error
		if p.streamLinkCommits != nil {
			err = p.streamLinkCommits(p.ctx, w, from, end, accept)
		} else {
			err = p.captureWindowCommitsLocked(w, from, end, accept)
		}
		if err != nil {
			if engine.IsChangeRangeCapacityError(err) {
				p.disableLocked(fmt.Sprintf(
					"pk-move link commit capture exceeded its attached bound: %v", err))
				return
			}
			poison()
			return
		}
	}
	if err := seenCtxCheck(); err != nil {
		poison()
		return
	}
	p.arrivalCommitIdx = idx
}

// pkMoveLinkCaptureLimit returns the attached engine bound one capture
// rides: the provenance cache budget of I6's family, both fields set
// because persisted replay rejects rows without bytes for the link
// table's variable-width columns (r51 (G)(i)).
func pkMoveLinkCaptureLimit() engine.ChangeRangeLimit {
	return engine.ChangeRangeLimit{
		MaxInMemoryRows:  pkMoveLinkMaxCachedRows,
		MaxInMemoryBytes: pkMoveLinkMaxCachedRows * 512,
	}
}

// captureWindowCommitsLocked streams one window's link rows through the
// link relation's own committed change capture and feeds each admitted
// (scope, commit instant) pair to accept. The engine call is direct —
// databranchutils.CollectChanges is expressly unsuitable here because it
// forces the visible-state snapshot policy (r51 (E)).
func (p *pkMoveLinkProvider) captureWindowCommitsLocked(
	w pkMoveLinkTableWindow,
	from, end types.TS,
	accept func(scope pkMoveTxnScope, cts types.TS) error,
) error {
	if p.ctx == nil || p.ses == nil || p.bh == nil {
		// No engine to capture through: the walk-fixture posture, a
		// complete absence (no link rows exist for this build).
		return nil
	}
	rel, err := resolvePkMoveLinkRelation(p.ctx, p.ses, p.bh)
	if err != nil {
		return err
	}
	if rel == nil {
		return nil
	}
	capCtx := engine.WithChangeRangeLimit(p.ctx, pkMoveLinkCaptureLimit())
	mp := p.ses.proc.Mp()
	handle, err := rel.CollectChanges(capCtx, from, end, true, mp)
	if err != nil {
		return err
	}
	defer func() { _ = handle.Close() }()
	return drainPkMoveLinkCaptureHandle(capCtx, handle, mp, func(data *batch.Batch) error {
		return foldPkMoveLinkCaptureBatch(w, data, accept)
	})
}

// drainPkMoveLinkCaptureHandle streams one opened link-relation capture
// handle to TRUE exhaustion: a page is live while EITHER side carries
// rows. The engine legitimately serves standalone tombstone pages before
// later data — a size-bounded tombstone page (logtailreplay
// ChangeHandler.Next), a range whose next sub-handle opens with
// tombstones (PartitionChangesHandle.nextReplay returns on either
// batch), or link reclamation rows from an unrelated dropped generation
// — and skipDeletes only steers net-effect merging; it never suppresses
// the tombstone stream. data == nil alone is therefore NOT the end of
// the capture: stopping there would certify a later scope's absence
// that the stream still carries. Both returned batches are caller-owned
// and released on every path, success or failure, and a read failure
// propagates unchanged so the caller keeps its incomplete (fail-closed)
// classification.
func drainPkMoveLinkCaptureHandle(
	ctx context.Context,
	handle engine.ChangesHandle,
	mp *mpool.MPool,
	fold func(data *batch.Batch) error,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		data, tombstone, _, nextErr := handle.Next(ctx, mp)
		if nextErr != nil {
			if data != nil {
				data.Clean(mp)
			}
			if tombstone != nil {
				tombstone.Clean(mp)
			}
			return nextErr
		}
		if data != nil {
			err := fold(data)
			data.Clean(mp)
			if err != nil {
				if tombstone != nil {
					tombstone.Clean(mp)
				}
				return err
			}
		}
		if tombstone != nil {
			tombstone.Clean(mp)
		}
		if data == nil && tombstone == nil {
			return nil
		}
	}
}

// foldPkMoveLinkCaptureBatch decodes one capture batch positionally by
// the link table's DDL order (link_id, table_id, old_pk_sha, old_row_id,
// old_pk, new_pk_sha, new_pk, new_row_id, write_ts, txn_id, commit-ts
// appended last) and admits only rows of this window's relation: every
// other table's history streams through the bounded engine
// materialization without being retained (r51 (G)(ii)). Wrong widths or
// counts are corruption and fail the capture.
func foldPkMoveLinkCaptureBatch(
	w pkMoveLinkTableWindow,
	data *batch.Batch,
	accept func(scope pkMoveTxnScope, cts types.TS) error,
) error {
	if len(data.Vecs) != 11 {
		return moerr.NewInternalErrorNoCtxf(
			"unexpected pk-move link capture width %d", len(data.Vecs))
	}
	if data.RowCount() == 0 {
		return nil
	}
	tableIDs := vector.MustFixedColNoTypeCheck[uint64](data.Vecs[1])
	writeTSs := vector.MustFixedColNoTypeCheck[uint64](data.Vecs[8])
	commitTSs := vector.MustFixedColNoTypeCheck[types.TS](data.Vecs[10])
	for i := 0; i < data.RowCount(); i++ {
		if tableIDs[i] != w.tableID {
			continue
		}
		txnBytes := data.Vecs[9].GetBytesAt(i)
		if len(txnBytes) != catalog.DataBranchUpdateLinkTxnIDBytes {
			return moerr.NewInternalErrorNoCtxf(
				"unexpected pk-move link capture txn-id width %d", len(txnBytes))
		}
		scope := pkMoveTxnScope{writeTS: types.BuildTS(int64(writeTSs[i]), 0)}
		copy(scope.txnID[:], txnBytes)
		scope.hasTxn = scope.txnID != [catalog.DataBranchUpdateLinkTxnIDBytes]byte{}
		if err := accept(scope, commitTSs[i]); err != nil {
			return err
		}
	}
	return nil
}

// resolvePkMoveLinkRelation resolves the account's link relation for the
// engine capture; an account created before the link table existed
// resolves to nil, which the capture records as complete absence (D5).
func resolvePkMoveLinkRelation(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
) (engine.Relation, error) {
	probe, err := runSql(ctx, ses, bh, fmt.Sprintf(
		"select rel_id from mo_catalog.mo_tables where reldatabase = '%s' and relname = '%s'",
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK,
	), nil, nil)
	if err != nil {
		return nil, err
	}
	var relid uint64
	probe.ReadRows(func(rowCnt int, cols []*vector.Vector) bool {
		if rowCnt > 0 && len(cols) == 1 {
			relid = vector.MustFixedColNoTypeCheck[uint64](cols[0])[0]
		}
		return false
	})
	probe.Close()
	if relid == 0 {
		return nil, nil
	}
	return getRelationById(ctx, ses, bh, relid, nil)
}

// groundTombstoneLocked answers the committed tombstone instant(s)
// deleting the exact version (key, rowID), reading each key's
// reference list at most ONCE per arrival build (r54 review F3). More
// than one instant reports the corrupt stream case (two committed
// tombstones deleting one version), matching the verdict walk's
// fail-closed rule; the full slice is returned so the caller can scope
// the corruption to exactly those instants (r58 review F2).
func (p *pkMoveLinkProvider) groundTombstoneLocked(
	view pkMoveStreamView,
	b *pkMoveArrivalBuild,
	key []byte,
	rowID types.Rowid,
) (refs []types.TS) {
	k := string(key)
	bucket, ok := b.tombs[k]
	if !ok {
		bucket = make(map[types.Rowid][]types.TS)
		for _, ref := range view.tombstonesAt(key) {
			p.arrivalScans++
			bucket[ref.rowID] = append(bucket[ref.rowID], ref.cts)
		}
		b.tombs[k] = bucket
	}
	return bucket[rowID]
}

// pkMovePredOutcome classifies one predecessor search inside a chain
// attribution.
type pkMovePredOutcome int

const (
	// pkMovePredFound: exactly one viable predecessor.
	pkMovePredFound pkMovePredOutcome = iota
	// pkMovePredMissing: no same-scope predecessor — the chain's origin
	// staged its old version inside the transaction (the D87 ungrounded
	// lane).
	pkMovePredMissing
	// pkMovePredAmbiguous: several viable predecessors (competing exact
	// identities, or several identity-less S3-route candidates).
	pkMovePredAmbiguous
	// pkMovePredContradicted: the hop's own generation is unrecorded,
	// but a same-scope landing attributes a DIFFERENT identity to this
	// key — one committed image per key per commit makes that corrupt.
	pkMovePredContradicted
)

// arrivalPredecessorLocked finds the recorded predecessor of one chain
// hop inside the landed link's transaction scope. The scope sub-indexes
// keep the work proportional to the hop's OWN scope rather than every
// move that ever landed at this key (r54 review F3). Identity binds by
// the exact replacement the hop consumed: other generations a
// same-transaction link staged at the same (netted, temporary) key are
// neither predecessors nor contradictions once the hop's own generation
// is found (r54 review F2). The fail-closed lanes stay: competing EXACT
// identities, a different attributed identity when the hop's own is
// missing, or several identity-less candidates all leave the evidence
// unusable.
func (p *pkMoveLinkProvider) arrivalPredecessorLocked(
	b *pkMoveArrivalBuild,
	scope pkMoveTxnScope,
	cur pkMoveLink,
) (pred *pkMoveLink, outcome pkMovePredOutcome) {
	pi := b.predIdx[string(cur.oldPK)]
	if pi == nil {
		return nil, pkMovePredMissing
	}
	links := b.lands[string(cur.oldPK)]
	// The scope lanes reproduce pkMoveTxnScope.matches exactly: a durable
	// transaction matches its own txn-id links plus the unknown-
	// transaction links created at its timestamp; an unknown-transaction
	// scope matches every link of its timestamp lane. Identity binding,
	// ambiguity, and contradiction read the lane's precomputed buckets —
	// only the matching candidates themselves are examined (r55 F1).
	var b1, b2 *pkMoveArrivalScopeBucket
	if scope.hasTxn {
		b1 = pi.txnIdx[scope.txnID]
		b2 = pi.tsNoTxnIdx[scope.writeTS]
	} else {
		b1 = pi.tsAllIdx[scope.writeTS]
	}
	exactN, looseN, carrying := 0, 0, 0
	exactPos, loosePos := -1, -1
	consult := func(bk *pkMoveArrivalScopeBucket) {
		if bk == nil {
			return
		}
		hits := bk.byID[cur.oldRowID]
		p.arrivalScans += uint64(len(hits))
		exactN += len(hits)
		if len(hits) == 1 {
			exactPos = hits[0]
		}
		carrying += bk.carryingN
		// Identity-less candidates are examined only when the hop's own
		// generation is unrecorded AND nothing contradicts it: the
		// loose classification's actual input (below).
		if exactN == 0 && carrying == 0 {
			p.arrivalScans += uint64(len(bk.idless))
			looseN += len(bk.idless)
			if len(bk.idless) == 1 {
				loosePos = bk.idless[0]
			}
		}
	}
	consult(b1)
	consult(b2)
	switch {
	case exactN > 1:
		return nil, pkMovePredAmbiguous
	case exactN == 1:
		// The hop's own generation is recorded: identity-less candidates
		// can only be duplicate recordings of the same landing (one
		// committed image per key per scope), so the exact binding wins.
		return &links[exactPos], pkMovePredFound
	case carrying > 0:
		return nil, pkMovePredContradicted
	case looseN > 1:
		return nil, pkMovePredAmbiguous
	case looseN == 1:
		return &links[loosePos], pkMovePredFound
	}
	return nil, pkMovePredMissing
}

// attributeArrivalLocked walks one candidate landing link BACKWARD
// through its recorded same-transaction chain and records the chain's
// grounded commit instant — the origin's deleting tombstone — into ent.
// Ungrounded chains (a same-transaction insert-then-move) record
// nothing and keep the D87 approximation lane. A chain that fails to
// attribute (ambiguous or contradicted predecessor, cycle, or more
// hops than the walk's own cap) undecides exactly the instants its
// evidence can still answer: the landed chain's feasible IN-WINDOW
// commit instants, derived from committed-stream evidence (r59 review
// F1 — write_ts is the transaction's creation time, a strict lower
// bound of its commit, so keying undecidability on it never answers
// the commit-shaped queries the tie and the tail issue) — and, for
// corrupt grounding, the competing tombstone instants (r58 review F2).
func (p *pkMoveLinkProvider) attributeArrivalLocked(
	view pkMoveStreamView,
	b *pkMoveArrivalBuild,
	landedKey string,
	landedIdx int,
	ent *pkMoveArrivalEntry,
) error {
	landed := b.lands[landedKey][landedIdx]
	scope := pkMoveLinkTxnScope(&landed)
	poison := func() {
		for cts := range p.failedArrivalCommitsLocked(view, b, scope) {
			ent.undecidable[cts] = struct{}{}
		}
	}
	cur := landed
	seen := map[string]struct{}{pkMoveLinkRowKey(cur.oldRowID, cur.oldPK): {}}
	for hops := 0; hops <= pkMoveLinkMaxChain; hops++ {
		if err := p.arrivalBuildCtxErr(); err != nil {
			// D157 (A): cancellation aborts the whole build, not just
			// this chain; the caller discards every partial attribution.
			return err
		}
		refs := p.groundTombstoneLocked(view, b, cur.oldPK, cur.oldRowID)
		if len(refs) > 1 {
			// Two committed tombstones deleting one version: corrupt
			// stream, fail closed like the verdict walk does — scoped
			// to the only instants this evidence can answer: the
			// competing tombstone instants plus every in-window instant
			// the chain's transaction could still have committed at
			// (r58 review F2, r59 review F1).
			for _, cts := range refs {
				ent.undecidable[cts] = struct{}{}
			}
			poison()
			return nil
		}
		if len(refs) == 1 {
			if pkMoveViewInstantTainted(view, refs[0]) {
				// The grounding tombstone's instant is a synthetic
				// window end, never a recorded commit: the arrival
				// attribution cannot bind it, so this chain's feasible
				// commit instants fail closed instead (r53 D156(E)
				// amendment; synthetic data ranges answer net-effect
				// replay only, never instant queries).
				poison()
				return nil
			}
			// Grounded: the version the chain moved out of the origin
			// key was a committed stream image, so the whole chain is a
			// recorded arrival committed at this tombstone's instant.
			ent.instants[refs[0]] = struct{}{}
			return nil
		}
		pred, outcome := p.arrivalPredecessorLocked(b, scope, cur)
		switch outcome {
		case pkMovePredMissing:
			// The chain's origin staged its old version inside the
			// transaction (insert-then-move) and never committed a
			// tombstone the stream can attribute.
			return nil
		case pkMovePredAmbiguous, pkMovePredContradicted:
			// Scoped: only the in-window instants the chain's
			// transaction could still have committed at depend on this
			// evidence (r58 review F2, r59 review F1).
			poison()
			return nil
		}
		rowKey := pkMoveLinkRowKey(pred.oldRowID, pred.oldPK)
		if _, cyc := seen[rowKey]; cyc {
			// A rotation returned to a generation this attribution
			// already visited: unbounded by construction, fail closed
			// for the in-window instants the chain's transaction could
			// still have committed at (r59 review F1).
			poison()
			return nil
		}
		seen[rowKey] = struct{}{}
		cur = *pred
	}
	// More hops than the walk itself would follow: no verdict may rest
	// on an attribution the fold could never replay. Scoped to the
	// in-window instants the chain's transaction could still have
	// committed at (r59 review F1): the hit-free prefix cannot pin the
	// origin's grounding tombstone when it lies past the cap — or the
	// truncated closure allowed through — but the transaction's own
	// recorded links still bound when it could have committed through
	// their own departure and collision tombstones; creation time
	// never substitutes for commit time.
	poison()
	return nil
}

// failedArrivalCommitsLocked answers one failed chain scope's feasible
// in-window commit instants, memoized per scope for the build's
// lifetime (r59 review F1) and resolved by the D156 scope->commit
// capture index: a scope present names exactly the captured instant, a
// scope absent names nothing (r50 (B)). The r49 approximation channels
// — origin-departure tombstones and arrival-key collision deaths — are
// deleted: unrecorded-commit evidence can only approximate, and an
// approximation both over-poisons (an unrelated same-window removal
// mistaken for the chain's commit) and under-poisons (the r60
// absence-mistaken-for-resolution hole).
func (p *pkMoveLinkProvider) failedArrivalCommitsLocked(
	view pkMoveStreamView,
	b *pkMoveArrivalBuild,
	scope pkMoveTxnScope,
) map[types.TS]struct{} {
	if b.failedArrivals == nil {
		b.failedArrivals = make(map[pkMoveTxnScope]map[types.TS]struct{})
	}
	if m, ok := b.failedArrivals[scope]; ok {
		return m
	}
	m := make(map[types.TS]struct{})
	p.arrivalCommitIdx.commits(scope, m)
	b.failedArrivals[scope] = m
	return m
}

// (The r49 approximation scan of scanFailedArrivalCommitsLocked — two
// committed-tombstone channels with an unpaired-names-nothing lane — is
// deleted under D156: exact capture evidence replaces approximation.)

// pkMoveDepKeyIndex is one old-pk key's departure links prepared for
// repeated successor lookups (r55 review F2). byRow binds the exact
// vacated row identity (duplicates preserved: reincarnation ambiguity
// keeps counting every recording); the scope lanes replicate
// pkMoveTxnScope.matches' asymmetry — byTxn holds only durable-txn
// links, byTSNone only unknown-transaction links, byTSAll every link —
// so a same-transaction key lookup consults exactly the link set the
// raw scan would have matched. The *Row lanes nest one more level under
// each scope lane (r56 review F1): a transaction-scoped identity lookup
// binds the exact vacated inhabitant INSIDE its applicable lane instead
// of scanning every cross-transaction namesake of a bare workspace row
// id — workspace ids restart from the same offsets in every
// transaction, so same-shaped moves pile N namesakes onto one raw id.
// Duplicates survive inside their lanes and keep counting as genuine
// ambiguity. n is len(byPK[key]) at build time: the probe cache is
// append-only, so a length change marks the index stale.
type pkMoveDepKeyIndex struct {
	n        int
	byRow    map[types.Rowid][]int
	byTxn    map[[catalog.DataBranchUpdateLinkTxnIDBytes]byte][]int
	byTSAll  map[types.TS][]int
	byTSNone map[types.TS][]int
	// byTxnRow / byTSAllRow / byTSNoneRow subdivide the flat scope lanes
	// above by vacated row identity; the flat lanes stay for the
	// key-only same-transaction lookup (S3 route, no identity to pin).
	byTxnRow    map[[catalog.DataBranchUpdateLinkTxnIDBytes]byte]map[types.Rowid][]int
	byTSAllRow  map[types.TS]map[types.Rowid][]int
	byTSNoneRow map[types.TS]map[types.Rowid][]int
}

// departIndexLocked builds (or reuses) key's departure index and
// instruments the build so the scaling regression can assert one linear
// pass per consulted key instead of a bucket rescan per hop.
func (p *pkMoveLinkProvider) departIndexLocked(key []byte) *pkMoveDepKeyIndex {
	k := string(key)
	links := p.byPK[k]
	if idx, ok := p.depIdx[k]; ok && idx.n == len(links) {
		return idx
	}
	if p.depIdx == nil {
		p.depIdx = make(map[string]*pkMoveDepKeyIndex)
	}
	idx := &pkMoveDepKeyIndex{
		n:           len(links),
		byRow:       make(map[types.Rowid][]int, len(links)),
		byTxn:       make(map[[catalog.DataBranchUpdateLinkTxnIDBytes]byte][]int),
		byTSAll:     make(map[types.TS][]int),
		byTSNone:    make(map[types.TS][]int),
		byTxnRow:    make(map[[catalog.DataBranchUpdateLinkTxnIDBytes]byte]map[types.Rowid][]int),
		byTSAllRow:  make(map[types.TS]map[types.Rowid][]int),
		byTSNoneRow: make(map[types.TS]map[types.Rowid][]int),
	}
	p.depBuilds++
	p.depScans += uint64(len(links))
	for i := range links {
		link := &links[i]
		idx.byRow[link.oldRowID] = append(idx.byRow[link.oldRowID], i)
		idx.byTSAll[link.writeTS] = append(idx.byTSAll[link.writeTS], i)
		mAll := idx.byTSAllRow[link.writeTS]
		if mAll == nil {
			mAll = make(map[types.Rowid][]int)
			idx.byTSAllRow[link.writeTS] = mAll
		}
		mAll[link.oldRowID] = append(mAll[link.oldRowID], i)
		if link.hasTxnID {
			idx.byTxn[link.txnID] = append(idx.byTxn[link.txnID], i)
			mTxn := idx.byTxnRow[link.txnID]
			if mTxn == nil {
				mTxn = make(map[types.Rowid][]int)
				idx.byTxnRow[link.txnID] = mTxn
			}
			mTxn[link.oldRowID] = append(mTxn[link.oldRowID], i)
		} else {
			idx.byTSNone[link.writeTS] = append(idx.byTSNone[link.writeTS], i)
			mNone := idx.byTSNoneRow[link.writeTS]
			if mNone == nil {
				mNone = make(map[types.Rowid][]int)
				idx.byTSNoneRow[link.writeTS] = mNone
			}
			mNone[link.oldRowID] = append(mNone[link.oldRowID], i)
		}
	}
	p.depIdx[k] = idx
	return idx
}

// successorOf returns the unique link that departs key by moving the row
// whose identity is inhabitant. The identity pin is what makes the walk
// sound: occupants of the same key belonging to other rows never match.
// count reports how many links name (key, inhabitant): exactly one is a
// usable successor; several is ambiguous evidence and callers must fail
// closed just like incomplete evidence. complete=false means the probe
// space could not be fully read (provider over budget).
//
// The transaction scope namespaces transaction-local identities:
// workspace-stage row ids restart at the same values in every transaction,
// so a link committed by a DIFFERENT transaction that names the same (key,
// workspace inhabitant) pair is a namesake coincidence, not this lineage's
// departure. Such cross-transaction coincidences are excluded BEFORE
// counting; same-transaction duplicates still count (genuine within-txn
// ambiguity), and durable inhabitants stay unscoped because their identity
// is globally unique already. The scope matches by durable transaction id
// when both sides carry one, with the create timestamp as fallback — the
// timestamp alone is not a namespace (two transactions created inside one
// physical instant share it).
func (p *pkMoveLinkProvider) successorOf(
	key []byte, inhabitant types.Rowid, scope pkMoveTxnScope,
) (succ pkMoveLink, count int, complete bool, err error) {
	if p == nil {
		return pkMoveLink{}, 0, true, nil
	}
	if err = p.ensureProbedForKey(key); err != nil {
		return pkMoveLink{}, 0, false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.disabled {
		return pkMoveLink{}, 0, false, nil
	}
	if !p.keyCompleteLocked(key) {
		// The key's own evidence could not be fully read (overflow or a
		// probe gap on a non-cached window): count zero here would read
		// as a proven lineage end, so report incomplete and fail closed.
		return pkMoveLink{}, 0, false, nil
	}
	isWS := pkMoveIsWorkspaceRowid(&inhabitant)
	idx := p.departIndexLocked(key)
	links := p.byPK[string(key)]
	// A transaction-local identity is bound INSIDE its applicable scope
	// lanes (r56 review F1): the durable-transaction links of this exact
	// transaction plus the unknown-transaction links minted at its
	// timestamp, or — for an unknown-transaction scope — every link of
	// its timestamp lane. The lanes replicate pkMoveTxnScope.matches'
	// asymmetry exactly, so cross-transaction namesakes of the bare
	// workspace id are never scanned; the scope guard below stays the
	// defensive check it was. A durable inhabitant stays on the raw row
	// lane: its identity is globally unique, so the raw lane names only
	// genuine duplicates, which must keep counting as ambiguity.
	var candIdx []int
	switch {
	case !isWS:
		candIdx = idx.byRow[inhabitant]
	case scope.hasTxn:
		candIdx = append(candIdx, idx.byTxnRow[scope.txnID][inhabitant]...)
		candIdx = append(candIdx, idx.byTSNoneRow[scope.writeTS][inhabitant]...)
	default:
		candIdx = idx.byTSAllRow[scope.writeTS][inhabitant]
	}
	for _, i := range candIdx {
		p.depScans++
		link := &links[i]
		if isWS && !scope.matches(link) {
			// Unreachable through the lanes above; keep the guard so a
			// lane change can never silently widen the match set.
			continue
		}
		count++
		succ = *link
	}
	// Zero means the lineage vanished without a recorded departure; several
	// means reincarnation made the continuation ambiguous.
	return succ, count, true, nil
}

// successorOfCommitted finds the link moving a COMMITTED stream identity:
// a version the diff itself observed (a live row, or a tombstone paired
// with its captured birth). Unlike transaction-local identities a
// committed (key, row id, birth, death) tuple is unique in the stream —
// the recording transaction had to read that exact committed image to
// move it — so no write-timestamp namespace applies here: the successor
// of a committed departure legitimately belongs to a LATER transaction.
// Several links naming the same committed (key, row id) pair are cross-
// transaction namesakes of one raw workspace id and remain ambiguous:
// callers fail closed on count > 1 just like incomplete evidence.
func (p *pkMoveLinkProvider) successorOfCommitted(
	key []byte, inhabitant types.Rowid,
) (succ pkMoveLink, count int, complete bool, err error) {
	if p == nil {
		return pkMoveLink{}, 0, true, nil
	}
	if err = p.ensureProbedForKey(key); err != nil {
		return pkMoveLink{}, 0, false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.disabled {
		return pkMoveLink{}, 0, false, nil
	}
	if !p.keyCompleteLocked(key) {
		// The key's own evidence could not be fully read (overflow or a
		// probe gap on a non-cached window): count zero here would read
		// as a proven lineage end, so report incomplete and fail closed.
		return pkMoveLink{}, 0, false, nil
	}
	idx := p.departIndexLocked(key)
	links := p.byPK[string(key)]
	for _, i := range idx.byRow[inhabitant] {
		p.depScans++
		count++
		succ = links[i]
	}
	return succ, count, true, nil
}

// successorOfSameTxnKey returns the recorded link(s) departing key inside
// the walk's current transaction (the scope matches by durable
// transaction id when both sides carry one, create timestamp otherwise).
// Unlike successorOf it matches by KEY alone: S3-route links carry no
// replacement identity, so a NULL-identity lineage landing at key cannot
// pin its continuation by row id. The commit-time prune already resolved
// that ambiguity the same way: the death-ledger rescue keeps a NULL link
// exactly when an ALIVE same-transaction link departs its new key
// (continuers group by old key). Following that departure reproduces the
// prune's own chain decision at the diff side — but only when the
// departure did NOT consume a committed pre-image visible in the window
// (a tombstone at key naming the departure's old row id): such a
// departure vacated an older generation of the key, not the landed one,
// and following it would adopt a foreign lineage. Callers perform that
// tombstone screen before hopping. A bare commit-timestamp claim at an
// intermediate key would as happily adopt an unrelated same-instant
// reincarnation. Several departures under one key in one transaction are
// ambiguous evidence: callers fail closed on count > 1.
func (p *pkMoveLinkProvider) successorOfSameTxnKey(
	key []byte, scope pkMoveTxnScope,
) (succ pkMoveLink, count int, complete bool, err error) {
	if p == nil {
		return pkMoveLink{}, 0, true, nil
	}
	if err = p.ensureProbedForKey(key); err != nil {
		return pkMoveLink{}, 0, false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.disabled {
		return pkMoveLink{}, 0, false, nil
	}
	if !p.keyCompleteLocked(key) {
		// The key's own evidence could not be fully read (overflow or a
		// probe gap on a non-cached window): count zero here would read
		// as a proven lineage end, so report incomplete and fail closed.
		return pkMoveLink{}, 0, false, nil
	}
	idx := p.departIndexLocked(key)
	links := p.byPK[string(key)]
	// The scope lanes reproduce pkMoveTxnScope.matches exactly: a durable
	// transaction matches its own txn-id links plus the unknown-transaction
	// links created at its timestamp; an unknown-transaction scope matches
	// every link of its timestamp lane.
	var candIdx []int
	if scope.hasTxn {
		candIdx = append(candIdx, idx.byTxn[scope.txnID]...)
		candIdx = append(candIdx, idx.byTSNone[scope.writeTS]...)
	} else {
		candIdx = idx.byTSAll[scope.writeTS]
	}
	for _, i := range candIdx {
		p.depScans++
		link := &links[i]
		if !scope.matches(link) {
			// Unreachable through the lanes above; keep the guard so a
			// lane change can never silently widen the match set.
			continue
		}
		count++
		succ = *link
	}
	return succ, count, true, nil
}

// ensureProbedForKey makes the link evidence for one old-pk key complete
// across every table window, returning complete evidence or an error.
func (p *pkMoveLinkProvider) ensureProbedForKey(key []byte) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.disabled {
		p.mu.Unlock()
		return nil
	}
	probedAll := true
	for _, w := range p.windows {
		if !p.keyAnsweredLocked(w, key) {
			probedAll = false
			break
		}
	}
	p.mu.Unlock()
	if probedAll {
		return nil
	}
	for _, w := range p.windows {
		if err := p.probeOldPKs(w, [][]byte{key}); err != nil {
			return err
		}
	}
	return nil
}

// pkMoveClaimedKeys records the final keys a proven lineage claimed during
// this diff. An unseen tombstone at a claimed key is the displaced previous
// occupant of that key: the fold already installed the lineage's row there,
// so the tombstone must be absorbed (skipped), not replayed as a delete that
// would kill the newly installed row. Tombstone shards run in parallel, so
// the set is mutex-guarded.
type pkMoveClaimedKeys struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func newPkMoveClaimedKeys() *pkMoveClaimedKeys {
	return &pkMoveClaimedKeys{m: make(map[string]struct{})}
}

func (s *pkMoveClaimedKeys) add(key []byte) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[string(append([]byte(nil), key...))] = struct{}{}
}

func (s *pkMoveClaimedKeys) has(key []byte) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[string(key)]
	return ok
}

// decodePkMoveStreamRowIDAndBirth extracts the row id and commit timestamp
// from a stored change-stream row. Data rows lay out as [rowid, <cols>,
// commit ts] and tombstones as [rowid, pk, commit ts], so one decoder
// serves both payloads. A row without a trailing T_TS-shaped timestamp
// reports ok=false, which callers treat as "no identity evidence" (fail
// closed).
func decodePkMoveStreamRowIDAndBirth(
	dataHashmap databranchutils.BranchHashmap,
	row []byte,
) (types.Rowid, types.TS, bool) {
	tuple, _, err := dataHashmap.DecodeRow(row)
	if err != nil || len(tuple) < 2 {
		return types.Rowid{}, types.TS{}, false
	}
	rowIDBytes, ok := tuple[0].([]uint8)
	if !ok || len(rowIDBytes) != types.RowidSize {
		return types.Rowid{}, types.TS{}, false
	}
	var rowID types.Rowid
	copy(rowID[:], rowIDBytes)
	birth, ok := pkMoveTupleCommitTS(tuple[len(tuple)-1])
	if !ok {
		return types.Rowid{}, types.TS{}, false
	}
	return rowID, birth, true
}

// pkMoveTupleCommitTS normalizes a decoded change-stream commit-ts tuple
// element to types.TS.
func pkMoveTupleCommitTS(v any) (types.TS, bool) {
	switch val := v.(type) {
	case types.TS:
		return val, true
	case []uint8:
		if len(val) == types.TxnTsSize {
			var ts types.TS
			copy(ts[:], val)
			return ts, true
		}
	}
	return types.TS{}, false
}

// pkMoveTombRef is one decoded in-window tombstone: the deleted row id
// and the deletion's commit timestamp. Chain walks read these from a
// pre-captured index instead of the tombstone hash map: the diff's shard
// loop holds shards exclusively (ForEachShardParallel), so reading the
// map mid-iteration deadlocks on the caller's own shard and races its
// peers.
type pkMoveTombRef struct {
	rowID types.Rowid
	cts   types.TS
}

// pkMoveDepartKey names one committed tombstone exactly: the exported row
// identity it deleted plus the tombstone's own commit timestamp. Workspace
// row-id allocation restarts from the same segment in every transaction,
// so a bare row id reincarnates across transactions (one raw id can be
// born at several commit instants); pinning the death instant keeps the
// born-died pairing exact for those reincarnated identities.
type pkMoveDepartKey struct {
	rowID types.Rowid
	cts   types.TS
}

// pkMoveStreamView exposes the committed-stream evidence the chain walk
// needs. The production view serves data rows from the per-side data hash
// map (never under iteration while walks run) and tombstones from the
// index captured by capturePkMoveStreamEvidence; tests stub it directly.
type pkMoveStreamView interface {
	// liveRowsAt returns the encoded data rows currently stored under key.
	liveRowsAt(key []byte) ([][]byte, error)
	// rowIDAndBirth decodes one data row (row id + birth commit ts).
	rowIDAndBirth(row []byte) (types.Rowid, types.TS, bool)
	// tombstonesAt returns the decoded tombstones stored under key.
	tombstonesAt(key []byte) []pkMoveTombRef
	// departedBirth returns the birth commit timestamp of the version that
	// tombstone (rowID, cts) deleted, captured before the born-died merge
	// popped it. ok=false means no such pair was seen.
	departedBirth(rowID types.Rowid, cts types.TS) (types.TS, bool)
	// deathRefsOf returns every tombstone of key naming rowID: the exact
	// evidence the anchor-death check and the departure screens consult,
	// served from a per-key index shared by every anchor standing on the
	// key rather than a full-bucket rescan per anchor (r55 review F2).
	deathRefsOf(key []byte, rowID types.Rowid) []pkMoveTombRef
	// prevDeath returns the newest tombstone of key strictly older than
	// before that names a row OTHER than excludeRowID; corrupt=true when
	// another row's tombstone shares the before instant exactly (one
	// committed image per key per commit instant).
	prevDeath(key []byte, excludeRowID types.Rowid, before types.TS) (prev pkMoveTombRef, found, corrupt bool)
	// bornDiedRefs returns every tombstone of key whose DEPARTED version
	// was born at birthCTS (the born-in-the-deleting-instant
	// reincarnation screen); the caller keeps filtering the tombstone's
	// own commit instant.
	bornDiedRefs(key []byte, birthCTS types.TS) []pkMoveTombRef
	// pairingAt serves the verdict walk's tail/departure pairing at key
	// for the lineage whose current version was born at chainTS: the
	// key-wide ambiguity gates (several links naming one committed pair;
	// a second pairing for one birth instant) keep their exact
	// fail-closed meaning, while valid departures and tails are served
	// indexed by departed birth and commit instant instead of a
	// full-bucket rescan per anchor (r56 review F2). succOf counts one
	// deleted identity's committed successor links exactly as the walk's
	// per-reference lookup did; linksLen reports the provider's current
	// departure-link count at key so a stale index is rebuilt.
	pairingAt(
		key []byte, chainTS types.TS,
		succOf func(types.Rowid) (pkMoveLink, int, bool, error),
		linksLen func() int,
	) pkMovePairing
}

// pkMoveTombKeyIndex is one key's tombstone bucket prepared for repeated
// anchor queries (r55 review F2): the verdict walk resolves every anchor
// of a hot key from the same evidence, so anchor-death, previous-death,
// and born-died lookups must serve one key's history through shared
// sub-indexes — exact row identity, commit-instant order, and departed
// birth — instead of a full-bucket rescan per anchor. Building touches
// each reference once, duplicates preserved, and the caller's
// uniqueness/ambiguity rules keep their exact meanings.
type pkMoveTombKeyIndex struct {
	// refs is the bucket snapshot sorted by commit instant (stable), so
	// previous-death binary-searches the strictly-earlier range.
	refs []pkMoveTombRef
	// byRow indexes each deleted row identity's references.
	byRow map[types.Rowid][]int
	// byBorn indexes references by their DEPARTED version's birth
	// instant (only references with a recorded birth) for the
	// born-in-the-deleting-instant reincarnation screen.
	byBorn map[types.TS][]int
}

// pkMoveTombIndexCache lazily builds one tombstone index per key per
// view — the stream snapshot a walk reads is immutable while the walk
// runs — and instruments builds and scanned references so the scaling
// regression can assert bounded total work across many anchors.
type pkMoveTombIndexCache struct {
	byKey  map[string]*pkMoveTombKeyIndex
	builds uint64
	scans  uint64
}

func (c *pkMoveTombIndexCache) indexFor(
	key []byte,
	refs []pkMoveTombRef,
	departed func(types.Rowid, types.TS) (types.TS, bool),
) *pkMoveTombKeyIndex {
	k := string(key)
	if idx, ok := c.byKey[k]; ok {
		return idx
	}
	if c.byKey == nil {
		c.byKey = make(map[string]*pkMoveTombKeyIndex)
	}
	idx := buildPkMoveTombKeyIndex(refs, departed, c)
	c.byKey[k] = idx
	c.builds++
	return idx
}

func buildPkMoveTombKeyIndex(
	src []pkMoveTombRef,
	departed func(types.Rowid, types.TS) (types.TS, bool),
	c *pkMoveTombIndexCache,
) *pkMoveTombKeyIndex {
	refs := append([]pkMoveTombRef(nil), src...)
	sort.SliceStable(refs, func(i, j int) bool { return refs[i].cts.LT(&refs[j].cts) })
	idx := &pkMoveTombKeyIndex{
		refs:   refs,
		byRow:  make(map[types.Rowid][]int, len(refs)),
		byBorn: make(map[types.TS][]int),
	}
	c.scans += uint64(len(refs))
	for i, ref := range refs {
		idx.byRow[ref.rowID] = append(idx.byRow[ref.rowID], i)
		if birth, ok := departed(ref.rowID, ref.cts); ok {
			idx.byBorn[birth] = append(idx.byBorn[birth], i)
		}
	}
	return idx
}

// deathRefsOf serves the exact row identity's references from the index.
func (c *pkMoveTombIndexCache) deathRefsOf(idx *pkMoveTombKeyIndex, rowID types.Rowid) (out []pkMoveTombRef) {
	pos := idx.byRow[rowID]
	c.scans += uint64(len(pos))
	for _, i := range pos {
		out = append(out, idx.refs[i])
	}
	return out
}

// prevDeath serves the newest death of another row strictly before the
// anchor's own instant from the commit-ordered references: binary-search
// the anchor-instant range, flag a same-instant foreign death as stream
// corruption, then take the newest strictly-earlier reference naming a
// different row. The anchor-death check (a unique deleting tombstone)
// keeps the backward step O(1): at most one reference of the excluded
// identity can sit before the anchor's own instant.
func (c *pkMoveTombIndexCache) prevDeath(
	idx *pkMoveTombKeyIndex, excludeRowID types.Rowid, before types.TS,
) (prev pkMoveTombRef, found, corrupt bool) {
	refs := idx.refs
	lo := sort.Search(len(refs), func(i int) bool { return !refs[i].cts.LT(&before) })
	for hi := lo; hi < len(refs) && refs[hi].cts.EQ(&before); hi++ {
		c.scans++
		if refs[hi].rowID != excludeRowID {
			corrupt = true
		}
	}
	for i := lo - 1; i >= 0; i-- {
		c.scans++
		if refs[i].rowID == excludeRowID {
			continue
		}
		prev, found = refs[i], true
		break
	}
	return prev, found, corrupt
}

// bornDiedRefs serves the tombstones whose departed version was born at
// the walk's own birth instant from the precomputed birth index.
func (c *pkMoveTombIndexCache) bornDiedRefs(idx *pkMoveTombKeyIndex, birthCTS types.TS) (out []pkMoveTombRef) {
	pos := idx.byBorn[birthCTS]
	c.scans += uint64(len(pos))
	for _, i := range pos {
		out = append(out, idx.refs[i])
	}
	return out
}

// pkMovePairingVerdict is the tail/departure pairing outcome an anchor's
// walk consumes at one landed key (r56 review F2).
type pkMovePairingVerdict uint8

const (
	// pkMovePairingNone: no tombstone of the key pairs the walk's birth
	// certificate; the walk falls through to the netted-generation step.
	pkMovePairingNone pkMovePairingVerdict = iota
	// pkMovePairingSucc: the lineage's own version died with exactly one
	// recorded move; the walk follows it.
	pkMovePairingSucc
	// pkMovePairingTail: the lineage's own version died with no recorded
	// departure (a same-key continuation's death certificate).
	pkMovePairingTail
	// pkMovePairingCorrupt: a second tombstone pairs the very same birth
	// instant — committed one-image-per-key-per-instant admits at most
	// one, so the stream contradicts itself.
	pkMovePairingCorrupt
	// pkMovePairingAmbiguous: some tombstone of this key has several
	// recorded departures for its committed identity (reincarnation
	// ambiguity); the key's pairing evidence is unusable for every walk.
	pkMovePairingAmbiguous
	// pkMovePairingIncomplete: the departure evidence could not be fully
	// read (probe error or over-budget); nothing is cached, so a later
	// consult retries exactly like the pre-index loop did.
	pkMovePairingIncomplete
)

// pkMovePairing is one walk-pairing consult's answer: the verdict plus
// the pairing's commit instant and (for pkMovePairingSucc) the successor
// link the walk follows.
type pkMovePairing struct {
	verdict pkMovePairingVerdict
	succ    pkMoveLink
	cts     types.TS
}

// pkMovePairingCand is one tombstone's pairing potential: the deleting
// commit instant, the deleted identity's recorded successor (count==1),
// or the proof that none exists (count==0, a tail candidate).
type pkMovePairingCand struct {
	cts   types.TS
	succ  pkMoveLink
	count int
}

// pkMovePairingKeyIndex is one key's pairing evidence prepared for
// repeated walks (r56 review F2): the key-wide ambiguity gates are
// computed once over the bucket's FULL reference set — the fail-closed
// contract the pre-index loop enforced per anchor — and the usable
// departures and tails are then grouped by their departed version's
// birth instant, so a walk consults only the candidates its own birth
// certificate can pair with. linksN pins the provider's departure
// bucket length at build time (the probe cache is append-only, so a
// length change marks the index stale); refsN pins the view's bucket
// length defensively.
type pkMovePairingKeyIndex struct {
	linksN    int
	refsN     int
	ambiguous bool
	cand      map[types.TS][]pkMovePairingCand
}

// pkMovePairingIndexCache lazily builds one pairing index per key per
// view. ResolveAndBuildPkMoveFolds walks one candidate per destination
// tombstone, so N lineages commuting through one committed intermediate
// key would otherwise rescan its N-tombstone bucket per walk (the r56
// review F2 quadratic). Builds and scanned references are instrumented
// for the chain-walk scaling regression.
type pkMovePairingIndexCache struct {
	byKey  map[string]*pkMovePairingKeyIndex
	builds uint64
	scans  uint64
}

// pairingFor serves one walk's pairing consult: reuse the key's cached
// index while the evidence is unchanged (the provider's departure bucket
// is append-only, so its pinned length and the view bucket's pinned
// length together vouch for every recorded gate), otherwise rebuild from
// the full bucket — evaluating every reference's ambiguity gates BEFORE
// grouping by departed birth, exactly the pre-index loop's order: an
// unusable key poisons every walk (ambiguous), and an unreadable bucket
// poisons this walk without being cached, so a later consult retries the
// probes exactly like the pre-index loop did.
func (c *pkMovePairingIndexCache) pairingFor(
	key []byte,
	refs []pkMoveTombRef,
	departed func(types.Rowid, types.TS) (types.TS, bool),
	succOf func(types.Rowid) (pkMoveLink, int, bool, error),
	linksLen func() int,
	chainTS types.TS,
) pkMovePairing {
	k := string(key)
	if idx, ok := c.byKey[k]; ok {
		if idx.linksN == linksLen() && idx.refsN == len(refs) {
			return c.consult(idx, chainTS)
		}
		delete(c.byKey, k)
	}
	if c.byKey == nil {
		c.byKey = make(map[string]*pkMovePairingKeyIndex)
	}
	idx := &pkMovePairingKeyIndex{
		refsN: len(refs),
		cand:  make(map[types.TS][]pkMovePairingCand),
	}
	c.builds++
	c.scans += uint64(len(refs))
	for _, ref := range refs {
		// The tombstone's row id names a committed version, and a
		// genuine cross-transaction continuation belongs to a LATER
		// transaction than the hop that landed here: the successor comes
		// without a write-timestamp namespace (the walk's closure widens
		// it). Several links naming the same committed pair stay
		// ambiguous and fail closed — for the whole key at once.
		succ, count, complete, err := succOf(ref.rowID)
		if err != nil || !complete {
			return pkMovePairing{verdict: pkMovePairingIncomplete}
		}
		if count > 1 {
			// Reincarnation made this identity's departure ambiguous:
			// the gate fired for a bucket reference before any
			// instant/birth filter, so it poisons the key for every
			// consult of this evidence generation.
			idx.ambiguous = true
			break
		}
		birth, recorded := departed(ref.rowID, ref.cts)
		if !recorded {
			// No captured birth: the instant/birth pairing below can
			// never bind this reference to a walk, exactly as the
			// pre-index loop's filter skipped it.
			continue
		}
		idx.cand[birth] = append(idx.cand[birth],
			pkMovePairingCand{cts: ref.cts, succ: succ, count: count})
	}
	idx.linksN = linksLen()
	c.byKey[k] = idx
	return c.consult(idx, chainTS)
}

// consult reads a current pairing index for one walk's birth
// certificate: only the departed-birth group the certificate names can
// pair (the pre-index loop's departedBirth == chainTS filter), and
// inside it only tombstones strictly after that birth (the cts.GT
// filter). Committed one-image-per-key-per-instant makes at most one
// match sound, so a second match stays stream corruption; a pairing
// whose identity recorded no departure is the same-key tail, and one
// with exactly one recorded departure hands the walk its successor.
func (c *pkMovePairingIndexCache) consult(
	idx *pkMovePairingKeyIndex, chainTS types.TS,
) pkMovePairing {
	if idx.ambiguous {
		return pkMovePairing{verdict: pkMovePairingAmbiguous}
	}
	matched := 0
	var found pkMovePairingCand
	for _, cand := range idx.cand[chainTS] {
		c.scans++
		if !cand.cts.GT(&chainTS) {
			// A tombstone at or before the lineage version's birth names
			// a strictly older version of the key.
			continue
		}
		matched++
		if matched > 1 {
			return pkMovePairing{verdict: pkMovePairingCorrupt}
		}
		found = cand
	}
	if matched == 0 {
		return pkMovePairing{verdict: pkMovePairingNone}
	}
	if found.count == 0 {
		return pkMovePairing{verdict: pkMovePairingTail, cts: found.cts}
	}
	return pkMovePairing{verdict: pkMovePairingSucc, succ: found.succ, cts: found.cts}
}

// pairingEvidenceStats exposes the per-view pairing-index instrumentation
// (builds, scanned references) for the chain-walk scaling regression.
func (c *pkMovePairingIndexCache) pairingEvidenceStats() (builds, scans uint64) {
	return c.builds, c.scans
}

// pkMoveIsWorkspaceRowid reports whether rowID names a transaction-local
// workspace row version (allocated out of TxnWorkspaceSegment): the
// pre-commit identity a link's harvested new_row_id carries for
// workspace-staged replacements. Such identities chain same-transaction
// generations and nothing else; committed stream rows never carry them.
func pkMoveIsWorkspaceRowid(rowID *types.Rowid) bool {
	return rowID.BorrowSegmentID().EQ(&colexec.TxnWorkspaceSegment)
}

// resolvePkMoveChain walks the provenance chain anchored at (anchorPK,
// anchorRowID) and returns the final replacement row plus its key. Every
// failure mode reports ok=false, meaning "not proven" (the caller keeps the
// legacy delete+insert replay).
//
// The anchor link must name the deleting tombstone exactly (old pk + old
// row id); the anchor tombstone's commit timestamp T is the walk's birth
// certificate for the first landed generation. At each landed key k,
// holding birth timestamp T and the previous link's replacement
// attribution (inhabitant):
//
//   - CLAIM: a stream data row under k born at exactly T is the lineage's
//     version. Committed pk uniqueness at every instant and one row per key
//     per transaction make the timestamp tie as strong as an identity
//     match, including for replacements whose committed row id the writer
//     could not record (TN assigns staged row ids only at precommit). The
//     claim stands when no departure of that exact row is recorded and the
//     evidence is complete; ambiguous or incomplete evidence fails closed,
//     and several rows born at T under one key are a corrupt impossibility
//     (fail closed). A candidate with its OWN recorded departure is not a
//     contradiction: the born-died merge pops data rows in parallel shard
//     order, so the walk may still see the pre-pop version, and it defers
//     to the departure steps below so both chunk orders resolve the chain
//     identically. Rows born at other timestamps are unrelated occupants
//     and keep their plain insert meaning.
//   - CROSS-TRANSACTION DEPARTURE: otherwise, if a stream tombstone under k
//     at T' > T pairs with the unique successor link naming its exact row
//     id, and the departed version was itself born at exactly T (captured
//     before the born-died merge erased it), the walk follows that link;
//     the landed generation's birth timestamp becomes T'. Tombstones at or
//     before T belong to older versions of the key, tombstones whose
//     departed version was born at another timestamp belong to an
//     occupant's own lineage, and tombstones without paired links are
//     unrelated churn: all three are skipped.
//   - SAME-TRANSACTION CONTINUATION: otherwise, an uncommitted generation
//     was netted out of the stream by its own transaction, and the unique
//     successor link must chain off the previous link's workspace-stage
//     inhabitant while carrying the same transaction create timestamp;
//     the birth timestamp T carries over. NULL inhabitants,
//     non-workspace identities, and create-timestamp mismatches all
//     fail closed.
//
// Ambiguous successors, unreadable streams, rotation cycles (a key
// revisited without a strictly advanced birth certificate), the hop cap,
// and incomplete probe evidence (provider over budget) fail closed. A
// lineage that RETURNS to a key it vacated in an earlier transaction is
// not cyclic: every stream hop lands on a strictly newer committed
// instant, so the revisit guard requires chainTS to increase past the
// newest certificate recorded at that key. Nothing is ever inferred from
// key occupancy alone.
// pkMoveResolution is the tri-state verdict of one provenance walk. The
// distinction between the two negative outcomes is the F3 correctness
// contract: provenNone keeps the legacy DELETE+INSERT replay whose
// semantics the stream alone justifies, while unresolved means RECORDED
// or PLAUSIBLY-EXISTING update provenance could not be proven — replaying
// that as DELETE+INSERT would turn genuine updates into destructive
// deletes (cascading the wrong action onto dependent rows), so the apply
// must abort instead.
type pkMoveResolution uint8

const (
	// pkMoveResolved: the chain walked to the lineage's final landed
	// version; fold it into one native UPDATE.
	pkMoveResolved pkMoveResolution = iota
	// pkMoveProvenNone: complete evidence shows NO update intent for this
	// tombstone row (an independent delete), or affirmatively proves the
	// moved lineage is not the destination's lineage (a reincarnation
	// rejection): the delete's own semantics stand.
	pkMoveProvenNone
	// pkMoveUnresolved: recorded or plausibly-existing provenance could
	// not be resolved — hop-cap exhaustion, evidence budget overflow,
	// corrupt or ambiguous stream evidence, a poisoned key, or a disabled
	// provider. The apply aborts loudly instead of downgrading the row.
	pkMoveUnresolved
	// pkMoveProvenDeleted: recorded provenance proves the lineage moved,
	// and complete stream evidence proves the moved lineage was then
	// TERMINATED by a plain deletion: the landed row died with no
	// recorded successor and no generation reborn by the deleting
	// transaction. The transaction's net effect on the destination is
	// the anchor tombstone's own DELETE, so declared ON DELETE actions
	// apply through the legacy replay path. This is NOT the ambiguity
	// case: a move whose landed row fell out of the evidence window
	// without terminal proof stays pkMoveUnresolved.
	pkMoveProvenDeleted
)

func (p *pkMoveLinkProvider) resolvePkMoveChain(
	view pkMoveStreamView,
	anchorPK []byte,
	anchorRowID types.Rowid,
) (finalPK []byte, finalRow []byte, ok bool) {
	finalPK, finalRow, verdict := p.resolvePkMoveChainVerdict(view, anchorPK, anchorRowID)
	if verdict == pkMoveResolved {
		return finalPK, finalRow, true
	}
	return nil, nil, false
}

func (p *pkMoveLinkProvider) resolvePkMoveChainVerdict(
	view pkMoveStreamView,
	anchorPK []byte,
	anchorRowID types.Rowid,
) (finalPK []byte, finalRow []byte, verdict pkMoveResolution) {
	anchor, hit, probeComplete := p.byRowProbeVerdict(anchorPK, anchorRowID)
	if !hit {
		if !probeComplete {
			// The anchor probe itself could not be fully read: update
			// intent is UNPROVEN, not absent.
			return nil, nil, pkMoveUnresolved
		}
		return nil, nil, pkMoveProvenNone
	}
	// The anchor tombstone's commit timestamp seeds the chain's birth
	// certificate: only a version committed by the exact transaction that
	// deleted oldRowID may be claimed at the first landed key.
	deathRefs := view.deathRefsOf(anchorPK, anchorRowID)
	if len(deathRefs) != 1 {
		// The deleting tombstone must exist and be unique; anything else
		// is unexplainable stream corruption this fold refuses to
		// interpret.
		return nil, nil, pkMoveUnresolved
	}
	chainTS := deathRefs[0].cts
	if pkMoveViewInstantTainted(view, chainTS) {
		// The anchor tombstone's instant is a synthetic window end
		// stamped by the serving handle, never a recorded commit: no
		// birth certificate may bind it (r53 D156(E) amendment).
		return nil, nil, pkMoveUnresolved
	}

	// Anchor-vs-LCA continuity: the anchor row must descend from the image
	// the destination will rewrite at the old key. An earlier tombstone
	// under the anchor key deletes an older version of that lineage, and a
	// move chain continues it only when the anchor version was born by the
	// SAME transaction that deleted the previous version (a same-instant
	// re-birth, the update chain's own reincarnation shape). An anchor
	// version born strictly after the previous death reincarnated the key
	// in a later transaction: the lineage the destination holds was
	// deleted and independently re-created before it moved, so folding
	// would rewrite — and cascade destination children onto — a lineage
	// the source had already ended. Keep delete semantics for it.
	anchorBirth, anchorBorn := view.departedBirth(anchorRowID, chainTS)
	prevRef, prevFound, prevCorrupt := view.prevDeath(anchorPK, anchorRowID, chainTS)
	if prevCorrupt {
		// One committed image per key per transaction makes a second
		// tombstone at the anchor's own commit impossible.
		return nil, nil, pkMoveUnresolved
	}
	// Deletes of generations after the anchor's own death say nothing
	// about what the destination holds at the anchor key, so prevDeath
	// considers only strictly-earlier instants.
	prevTS := prevRef.cts
	if prevFound && (pkMoveViewInstantTainted(view, prevTS) ||
		(anchorBorn && pkMoveViewInstantTainted(view, anchorBirth))) {
		// The continuity comparison would key on a synthetic window
		// end, never a recorded commit: neither the reincarnation
		// answer nor the tie may bind it (r53 D156(E) amendment).
		return nil, nil, pkMoveUnresolved
	}
	if prevFound && (!anchorBorn || !anchorBirth.EQ(&prevTS)) {
		// Continuity against the destination lineage is PROVEN
		// absent here (the anchor row descends from an unrelated
		// reincarnation, not from the image the destination holds):
		// this is affirmative "no fold", and the delete's own
		// semantics stand.
		return nil, nil, pkMoveProvenNone
	}
	if prevFound && anchorBorn && anchorBirth.EQ(&prevTS) {
		switch p.anchorArrivalVerdict(view, anchorPK, anchorBirth) {
		case pkMoveArrivalRecorded:
			// The bare tie holds: the anchor version was born at the
			// very instant the previous key-holder died. But a RECORDED
			// pk move CHAIN landed at the anchor key in exactly that
			// transaction — possibly through same-transaction
			// intermediate keys netted out of the committed stream — so
			// the birth was an arrival from another lineage's key, not
			// the deleting transaction's same-key successor whose
			// re-birth is never recorded (D87/D94). The anchor's actual
			// LCA lineage continues at the arrival's origin, whose own
			// anchor walks through this very point; folding here would
			// make two chains claim one replacement row, and whichever
			// fold materialized first would let the other replay this
			// anchor as a destructive DELETE — or trip the fold
			// planner's unique-claim abort on an otherwise provable
			// chain (r53 F1). Complete evidence proves this anchor has
			// no continuity: its own delete semantics stand, exactly as
			// in the reincarnation case above.
			return nil, nil, pkMoveProvenNone
		case pkMoveArrivalIncomplete:
			// Arrival evidence exists at the tie instant but cannot be
			// fully attributed (ambiguous predecessors, contradictory
			// generations, a cycle, or an over-long chain): neither
			// accepting the tie nor treating it as disproven is sound,
			// so fail closed like every other incomplete-evidence path.
			return nil, nil, pkMoveUnresolved
		}
	}

	k := anchor.newPK
	scope := pkMoveLinkTxnScope(&anchor)
	inhabitant, hasInhabitant := anchor.newRowID, anchor.hasNewRowID
	// visited records the newest birth certificate the walk has stood on
	// at each key. A revisit with a LATER certificate is a lineage
	// returning to a vacated key through a cross-transaction departure
	// (the commit instant strictly increases at every stream hop and at
	// every same-key tail). A revisit at the SAME instant is reachable
	// only through a recorded same-transaction hop — the exact workspace
	// identity probe or the tombstone-screened same-transaction key
	// probe — and recorded hops cannot switch rows: the probe binds the
	// lineage's own replacement identity, so the lineage itself returned
	// home within one commit and the claim below resolves it (or the
	// downstream checks fail closed). A true rotation CYCLE consists of
	// distinct rows exchanging keys; each row's own walk resolves, and
	// the fold planner rejects the exchange as a rotation cycle. Monotone
	// certificates plus pkMoveLinkMaxChain bound the walk regardless:
	// each recorded hop consumes a distinct link of one transaction.
	visited := map[string]types.TS{string(anchorPK): chainTS}
	// tailTaken records that the walk consumed the landed version's own
	// link-less death (a same-key tail) and has not yet found any
	// continuation of the lineage past it. When the evidence still shows
	// nothing — no reborn generation at the deleting instant and no
	// later departure — complete evidence proves a TERMINAL deletion,
	// which is semantically different from incomplete provenance.
	tailTaken := false
	for hops := 0; hops < pkMoveLinkMaxChain; hops++ {
		ks := string(k)
		if seen, dup := visited[ks]; dup {
			// Strictly-backward certificates are stream corruption;
			// equal certificates are the recorded same-transaction
			// homecoming documented above and continue.
			if chainTS.LT(&seen) {
				return nil, nil, pkMoveUnresolved
			}
		}
		visited[ks] = chainTS

		// Recorded same-transaction continuity outranks a bare timestamp
		// claim: when the incoming link's replacement identity is a
		// workspace-stage row, an outgoing link chained on that identity
		// names the lineage's next generation directly, while a live row
		// born here at the same commit instant may be an unrelated
		// reincarnation of the vacated intermediate key. Follow the
		// recorded continuation first; fall through to the claim only
		// when no continuation exists (the chain truly ends here).
		if hasInhabitant && pkMoveIsWorkspaceRowid(&inhabitant) {
			cand, count, complete, err := p.successorOf(k, inhabitant, scope)
			if err != nil || !complete || count > 1 {
				return nil, nil, pkMoveUnresolved
			}
			if count == 1 {
				if !scope.matches(&cand) {
					return nil, nil, pkMoveUnresolved
				}
				k = cand.newPK
				inhabitant, hasInhabitant = cand.newRowID, cand.hasNewRowID
				tailTaken = false
				continue
			}
		}
		if !hasInhabitant || !pkMoveIsWorkspaceRowid(&inhabitant) {
			// The incoming link names no usable replacement identity (the
			// S3 route cannot stage one). Its survival through the commit
			// prune meant an ALIVE same-transaction link departs this key
			// (the death-ledger continuer groups by old key); that
			// departure is the lineage's own continuation — UNLESS the
			// departure consumed a committed pre-image of this key (the
			// in-window stream tombstone naming its old row id): then it
			// vacated an older generation, not the landed one, and
			// following it would adopt a foreign lineage (the
			// vacate-before-arrival shape). Follow a clean departure
			// before touching the bare timestamp claim: the claim would
			// as happily adopt an unrelated same-instant reincarnation
			// of the intermediate key, and the diverted delete that hid
			// the real continuation left no stream trace. No departure
			// here — or a departure the tombstone screen proves foreign —
			// means the claim below sees the single landed generation and
			// stays sound.
			cand, count, complete, err := p.successorOfSameTxnKey(k, scope)
			if err != nil || !complete || count > 1 {
				return nil, nil, pkMoveUnresolved
			}
			if count == 1 {
				vacatedCommitted := len(view.deathRefsOf(k, cand.oldRowID)) > 0
				if !vacatedCommitted {
					k = cand.newPK
					inhabitant, hasInhabitant = cand.newRowID, cand.hasNewRowID
					tailTaken = false
					continue
				}
			}
		}

		rows, err := view.liveRowsAt(k)
		if err != nil {
			return nil, nil, pkMoveUnresolved
		}
		var claimRow []byte
		var claimRID types.Rowid
		claimFound := false
		for _, row := range rows {
			rid, birth, hasID := view.rowIDAndBirth(row)
			if !hasID || !birth.EQ(&chainTS) {
				// Occupants of other instants and rows without identity
				// evidence are not this lineage's version.
				continue
			}
			if claimFound {
				// Two rows of one key born at one commit timestamp cannot
				// both be the committed image: corrupt stream, fail closed.
				return nil, nil, pkMoveUnresolved
			}
			claimRow, claimRID, claimFound = row, rid, true
		}
		if claimFound {
			for _, ref := range view.bornDiedRefs(k, chainTS) {
				if !ref.cts.EQ(&chainTS) {
					// A version born at the lineage's birth instant but
					// deleted in a LATER transaction is a descendant,
					// not the born-and-died reincarnation of the very
					// transaction the walk follows (bornDiedRefs already
					// pinned the departed version's birth to chainTS:
					// tombstones deleting versions born strictly EARLIER
					// are accounted-for removals of older generations,
					// not this lineage's reincarnation). Descendants
					// stay accounted in the tail/departure pairing
					// below.
					continue
				}
				// A tombstone of this key deleted a version BORN at the
				// lineage's own birth instant: some version of this key
				// was born and died inside the very transaction the walk
				// follows. Under a bare commit-timestamp tie the dead
				// version is an equally good continuation and the live
				// candidate may be an unrelated reincarnation; the
				// commit-time prune catches this only when the update
				// route recorded replacement identity (the S3 route
				// cannot). Ambiguity: fail closed.
				return nil, nil, pkMoveUnresolved
			}
			// Provenance must not contradict the stream: an ambiguous or
			// incompletely probed departure space fails closed. The claimed
			// version is a committed identity, so its departure is looked
			// up without a transaction namespace.
			_, count, complete, err := p.successorOfCommitted(k, claimRID)
			if err != nil || !complete || count > 1 {
				return nil, nil, pkMoveUnresolved
			}
			if count == 0 {
				// The probe space is complete for this candidate: a data
				// row only survives buildHashmapForTable's reconciliation
				// when its birth is at or after the newest tombstone of
				// its key, so a claimed version provably has NO later
				// tombstone under this key. Provenance is the only place
				// a departure can hide, and it shows none: lineage end.
				return k, claimRow, pkMoveResolved
			}
			// The candidate version has its OWN recorded departure. The
			// departure is evidence for this window only when its
			// tombstone is visible here: a complete window stream shows
			// every COMMITTED in-window departure of a committed
			// identity as the tombstone naming it, so an unbound
			// recorded departure provably committed outside this window
			// (its transaction was created before the endpoint and
			// committed after it — a straddler the probe's write_ts
			// upper bound necessarily includes). Future history must
			// not invalidate a valid fold: the lineage ends at the
			// claimed version for this window.
			departureInWindow := false
			for _, ref := range view.deathRefsOf(k, claimRID) {
				if ref.cts.GT(&chainTS) {
					departureInWindow = true
					break
				}
			}
			if !departureInWindow {
				return k, claimRow, pkMoveResolved
			}
			// The candidate version departs inside this window: it is
			// not the lineage end. Fall through to the departure steps,
			// which bind the hop to that version's tombstone.
		}

		// No stream-born version here: either the landed version departed
		// inside the window, or its generation was netted inside one
		// transaction and never reached the stream at all.
		var succ pkMoveLink
		var succTS types.TS
		succFound := false
		tailFound := false
		var tailTS types.TS
		// Pairing is served from the key-wide index (r56 review F2): the
		// ambiguity gates — several links naming one committed pair, and
		// a second pairing for the lineage's own version — must fire for
		// EVERY tombstone of this key before the instant/birth filters,
		// but that does not require repeating those checks per anchor.
		// The index evaluates them once over the bucket's full reference
		// set, then serves each walk only the departed-birth group its
		// own birth certificate can pair with (each candidate's successor
		// still inspected for count and commit instant), preserving every
		// fail-closed outcome while N lineages commuting through one
		// committed intermediate key no longer rescan its bucket per walk.
		{
			pk := k
			pair := view.pairingAt(pk, chainTS,
				func(rid types.Rowid) (pkMoveLink, int, bool, error) {
					// The tombstone's row id names a committed version,
					// and a genuine cross-transaction continuation belongs
					// to a LATER transaction than the hop that landed
					// here: the successor is looked up without a
					// write-timestamp namespace. several links naming the
					// same committed pair stay ambiguous and fail closed.
					return p.successorOfCommitted(pk, rid)
				},
				func() int { return p.departureLinkCount(pk) })
			switch pair.verdict {
			case pkMovePairingIncomplete, pkMovePairingAmbiguous, pkMovePairingCorrupt:
				return nil, nil, pkMoveUnresolved
			case pkMovePairingSucc:
				succ, succTS, succFound = pair.succ, pair.cts, true
			case pkMovePairingTail:
				// The version died at pair.cts with no recorded move: its
				// key kept the row (a stable-key update re-birthed it in
				// the deleting transaction) or the lineage simply ended.
				// Take the death instant as the new birth certificate and
				// re-verify at the same key: the claim accepts only a
				// live row born at exactly that instant, so a plain
				// delete or a later unrelated re-insert still fails
				// closed, while a same-transaction delete+insert at the
				// same key folds exactly like the legacy stable-key
				// update matcher folds it.
				tailTS, tailFound = pair.cts, true
			case pkMovePairingNone:
			}
		}
		if succFound {
			if pkMoveViewInstantTainted(view, succTS) {
				// The departed version died at a synthetic window end,
				// never a recorded commit: the hop's birth certificate
				// cannot bind it (r53 D156(E) amendment).
				return nil, nil, pkMoveUnresolved
			}
			k = succ.newPK
			chainTS = succTS
			scope = pkMoveLinkTxnScope(&succ)
			inhabitant, hasInhabitant = succ.newRowID, succ.hasNewRowID
			tailTaken = false
			continue
		}
		if tailFound {
			if pkMoveViewInstantTainted(view, tailTS) {
				// The tail's death instant is a synthetic window end,
				// never a recorded commit: the arrival screen and the
				// re-birth certificate cannot bind it (r53 D156(E)
				// amendment).
				return nil, nil, pkMoveUnresolved
			}
			// The re-birth that would fold this tail must pass the same
			// incoming-provenance screen the anchor continuity tie
			// applies (r58 review F1): a RECORDED move chain landing at
			// this key in the tail's own deleting transaction makes the
			// re-born row that chain's landed image — one committed
			// image per key per instant — so the walked lineage's
			// landed version provably ENDED in this deletion and its
			// terminal delete semantics stand; undecidable arrival
			// evidence at the tail instant fails closed. A linkless
			// re-birth answers None below (its delete+insert records
			// nothing), keeping the legacy stable-key fold.
			switch p.anchorArrivalVerdict(view, k, tailTS) {
			case pkMoveArrivalRecorded:
				return nil, nil, pkMoveProvenDeleted
			case pkMoveArrivalIncomplete:
				return nil, nil, pkMoveUnresolved
			}
			// Same-key continuation: chainTS strictly increases, so the
			// strictly-advancing revisit guard re-admits this key on the
			// next pass; the claimed row then re-verifies the lineage.
			// The consumed link's inhabitant named the version that just
			// died and must not seed a same-transaction continuation
			// probe afterwards. The re-birthing transaction left no link
			// rows (a stable-key delete+insert records nothing), so its
			// identity is unknown: an empty scope makes later
			// same-transaction probes inert instead of matching a STALE
			// earlier transaction's namespace.
			chainTS = tailTS
			scope = pkMoveTxnScope{}
			inhabitant, hasInhabitant = types.Rowid{}, false
			tailTaken = true
			continue
		}

		// Same-transaction netted generation: the only link that may
		// continue the walk chains off the workspace-stage identity the
		// previous link recorded, from the same transaction. The birth
		// timestamp carries over because the whole netted chain commits
		// as one transaction.
		if !hasInhabitant || !pkMoveIsWorkspaceRowid(&inhabitant) {
			if tailTaken {
				// The landed row died with no recorded successor, the
				// deleting transaction rebirthed no generation the claim
				// could adopt, and nothing departs past the tail: complete
				// evidence proves the lineage ENDED in this deletion.
				return nil, nil, pkMoveProvenDeleted
			}
			return nil, nil, pkMoveUnresolved
		}
		cand, count, complete, err := p.successorOf(k, inhabitant, scope)
		if err != nil || !complete || count != 1 {
			return nil, nil, pkMoveUnresolved
		}
		if !scope.matches(&cand) {
			return nil, nil, pkMoveUnresolved
		}
		k = cand.newPK
		inhabitant, hasInhabitant = cand.newRowID, cand.hasNewRowID
	}
	return nil, nil, pkMoveUnresolved
}

// pkMoveStreamEvidence is the pre-dedup tombstone evidence of one diff
// side. buildHashmapForTable's per-key reconciliation nets a generation
// inserted and deleted inside the same collect window out of the maps
// entirely (the stale data row is popped and the tombstone consumed as
// intermediate), which is the correct NET effect for plain replay but
// erases the exact tombstone+birth record a cross-transaction chain hop
// binds to. The capture therefore runs inside buildHashmapForTable right
// BEFORE that reconciliation and rides the diff plumbing alongside the
// hash maps.
type pkMoveStreamEvidence struct {
	tombs    map[string][]pkMoveTombRef
	departed map[pkMoveDepartKey]types.TS
	// capped marks a capture that hit pkMoveEvidenceMaxEntries: both
	// indexes were cleared, so the consumer cannot tell a move-free
	// tombstone from one whose provenance it never read.
	capped bool
	// tainted records the end instants of windows this side's drain
	// certified SYNTHETIC (r53 D156(E) amendment: the empty-lower-bound
	// state read, a Snapshot hint, or a recovery substitution): rows
	// those windows served carry the window END instead of recorded
	// commits, so instant-bearing extracts equal to these instants
	// must never bind a provenance verdict.
	tainted map[types.TS]struct{}
}

// pkMoveHashmapView is the production pkMoveStreamView: data rows come
// from the per-side data hash map populated by buildHashmapForTable (the
// data map is never under shard iteration while walks run), tombstones
// and departed births come from the indexes capturePkMoveStreamEvidence
// materialized before the diff's shard loop started (the tombstone map
// itself is exclusively shard-iterated by that loop and must not be
// queried here).
type pkMoveHashmapView struct {
	data     databranchutils.BranchHashmap
	tombs    map[string][]pkMoveTombRef
	departed map[pkMoveDepartKey]types.TS
	// capped mirrors pkMoveStreamEvidence.capped for the fold pass: the
	// tombstone index is empty because capture was CUT OFF, not because
	// the window was quiet.
	capped bool
	// tainted mirrors pkMoveStreamEvidence.tainted for the fold pass:
	// the end instants of synthetic windows never bind a verdict (r53
	// D156(E) amendment).
	tainted map[types.TS]struct{}
	// tombCache indexes one key's tombstone history at most once per
	// fold pass, so N anchors standing on a hot key never rescan its
	// history per anchor (r55 review F2). Fold planning is
	// single-threaded, so the cache needs no lock.
	tombCache pkMoveTombIndexCache
	// pairCache computes one key's tail/departure pairing ambiguity at
	// most once per evidence generation and serves each anchor's pairing
	// from the departed-birth index, so N walks commuting through one
	// committed intermediate key never rescan its tombstone bucket per
	// walk (r56 review F2). Same single-threaded fold pass: no lock.
	pairCache pkMovePairingIndexCache
}

// pkMoveViewInstantTainted reports whether ts is one of the end instants
// this side's drain certified SYNTHETIC (r53 D156(E) amendment): an
// instant equal to it is a serving-handle stamp, never a recorded
// commit, so no provenance verdict may bind it. Views that carry no
// certification (unit-test fakes and other pkMoveStreamView
// implementations) taint nothing.
func pkMoveViewInstantTainted(view pkMoveStreamView, ts types.TS) bool {
	hv, ok := view.(*pkMoveHashmapView)
	if !ok || hv.tainted == nil {
		return false
	}
	_, hit := hv.tainted[ts]
	return hit
}

func (v *pkMoveHashmapView) liveRowsAt(key []byte) ([][]byte, error) {
	ret, err := v.data.GetByEncodedKey(key)
	if err != nil || !ret.Exists {
		return nil, err
	}
	return ret.Rows, nil
}

func (v *pkMoveHashmapView) rowIDAndBirth(row []byte) (types.Rowid, types.TS, bool) {
	return decodePkMoveStreamRowIDAndBirth(v.data, row)
}

func (v *pkMoveHashmapView) tombstonesAt(key []byte) []pkMoveTombRef {
	return v.tombs[string(key)]
}

func (v *pkMoveHashmapView) departedBirth(rowID types.Rowid, cts types.TS) (types.TS, bool) {
	birth, ok := v.departed[pkMoveDepartKey{rowID: rowID, cts: cts}]
	return birth, ok
}

func (v *pkMoveHashmapView) deathRefsOf(key []byte, rowID types.Rowid) []pkMoveTombRef {
	idx := v.tombCache.indexFor(key, v.tombstonesAt(key), v.departedBirth)
	return v.tombCache.deathRefsOf(idx, rowID)
}

func (v *pkMoveHashmapView) prevDeath(
	key []byte, excludeRowID types.Rowid, before types.TS,
) (pkMoveTombRef, bool, bool) {
	idx := v.tombCache.indexFor(key, v.tombstonesAt(key), v.departedBirth)
	return v.tombCache.prevDeath(idx, excludeRowID, before)
}

func (v *pkMoveHashmapView) bornDiedRefs(key []byte, birthCTS types.TS) []pkMoveTombRef {
	idx := v.tombCache.indexFor(key, v.tombstonesAt(key), v.departedBirth)
	return v.tombCache.bornDiedRefs(idx, birthCTS)
}

func (v *pkMoveHashmapView) pairingAt(
	key []byte, chainTS types.TS,
	succOf func(types.Rowid) (pkMoveLink, int, bool, error),
	linksLen func() int,
) pkMovePairing {
	return v.pairCache.pairingFor(
		key, v.tombstonesAt(key), v.departedBirth, succOf, linksLen, chainTS)
}

// capturePkMoveStreamEvidence materializes the two tombstone indexes the
// chain walks consult during the diff:
//
//  1. tombs — every in-window tombstone decoded to (row id, commit ts),
//     keyed by encoded pk. Walks must read tombstones from here rather
//     than the tombstone hash map: the diff's shard loop iterates that
//     map with exclusive shard access (ForEachShardParallel), so any
//     GetByEncodedKey from inside a walk would deadlock on the caller's
//     own shard or race a peer worker's. The index is O(in-window
//     tombstones), the same order as the maps it decodes from.
//  2. departed — for every tombstone whose deleted version is still
//     visible, the birth commit timestamp that version carried. The
//     pairing is by birth order, NEVER by row id: across a storage
//     flush the stream names a version's export identity while the
//     delete names the flushed-block identity it targeted, and the
//     two differ for one logical version. The walk needs the birth to
//     bind a cross-transaction departure (tombstone + successor link)
//     to the exact version born at the chain's current timestamp:
//     once the born-died merge pops the paired data row, the birth
//     would otherwise be unrecoverable and the walk could mistake an
//     unrelated occupant's departure for the lineage's. Tombstones
//     without a visible deleted version (LCA-era anchors) record
//     nothing.
//
// This runs before any pairing consumes the maps, so both sets are
// complete for the whole diff regardless of chunk processing order.
// pkMoveEvidenceMaxEntries bounds the retained evidence maps: they live
// outside the hashmap allocator's spill budget, so a pathological window
// must not grow them without limit. Past the cap the capture clears BOTH
// indexes completely (never partially — an anchor-then-LCA continuity
// check reading a partial tombstone index could pass a reincarnation)
// and reports capped=true. The apply consumer must then NOT quietly
// replay legacy: capped evidence with recorded update provenance in
// window aborts the apply (see resolveAndBuildPkMoveFolds), while a
// proven link-free window keeps legacy replay — that is exactly the
// proven-absence verdict every tombstone would receive.
const pkMoveEvidenceMaxEntries = 1 << 22

func capturePkMoveStreamEvidence(
	ctx context.Context,
	dataHashmap, tombstoneHashmap databranchutils.BranchHashmap,
	parallelism int,
) (departed map[pkMoveDepartKey]types.TS, tombs map[string][]pkMoveTombRef, capped bool, err error) {
	return capturePkMoveStreamEvidenceBounded(
		ctx, dataHashmap, tombstoneHashmap, parallelism, pkMoveEvidenceMaxEntries)
}

// pkMoveKeyVersionIndex materializes one key's visible data versions ONCE
// per shard: every decodable version contributes its birth to the
// key-level multiset (tier 2) and to its row id's birth list (tier 1),
// each list sorted ascending. A high-churn key then resolves each of its
// K tombstones by binary search over V versions instead of re-decoding
// every version per tombstone, turning the capture's worst case from
// O(K·V) decodes into O(V·log V + K·log V). The price is retention of
// one birth multiset per touched key for the shard pass; that is the
// same order of data the hash map already holds, and the tombstone cap
// still fail-closes the whole capture.
type pkMoveKeyVersionIndex struct {
	births []types.TS
	byRID  map[types.Rowid][]types.TS
}

// buildPkMoveKeyVersionIndex decodes every version of one key exactly
// once into the two sorted lists. Undecodable rows are skipped from
// both lists, matching the per-row skip of the loop this replaces.
func buildPkMoveKeyVersionIndex(
	hm databranchutils.BranchHashmap,
	ret databranchutils.GetResult,
) *pkMoveKeyVersionIndex {
	idx := &pkMoveKeyVersionIndex{byRID: make(map[types.Rowid][]types.TS, len(ret.Rows))}
	for _, dataRow := range ret.Rows {
		rid, b, ok := decodePkMoveStreamRowIDAndBirth(hm, dataRow)
		if !ok {
			continue
		}
		idx.births = append(idx.births, b)
		idx.byRID[rid] = append(idx.byRID[rid], b)
	}
	sort.Slice(idx.births, func(i, j int) bool { return idx.births[i].LT(&idx.births[j]) })
	for _, list := range idx.byRID {
		sort.Slice(list, func(i, j int) bool { return list[i].LT(&list[j]) })
	}
	return idx
}

// deletedVersionBirth resolves the version one tombstone deleted,
// exactly as the two-tier scan it replaces did. Tier 1 is the newest
// exact row-id match not born after the deletion (a born-and-died
// version's birth equals the deletion's own commit instant); tier 2 —
// consulted only when tier 1 found nothing — is the newest birth
// strictly below the deletion across every row id of the key. Two rows
// sharing the greatest birth of either tier are corruption and record
// nothing, so hops over this deletion fail closed; a tier-1 tie in
// particular suppresses tier 2 exactly as the scan did.
func (idx *pkMoveKeyVersionIndex) deletedVersionBirth(
	tombRID types.Rowid, cts types.TS,
) (types.TS, bool) {
	if list := idx.byRID[tombRID]; len(list) > 0 {
		top := sort.Search(len(list), func(i int) bool { return list[i].GT(&cts) })
		if top > 0 {
			if top > 1 && list[top-2].EQ(&list[top-1]) {
				return types.TS{}, false
			}
			return list[top-1], true
		}
	}
	top := sort.Search(len(idx.births), func(i int) bool { return !idx.births[i].LT(&cts) })
	if top == 0 {
		return types.TS{}, false
	}
	if top > 1 && idx.births[top-2].EQ(&idx.births[top-1]) {
		return types.TS{}, false
	}
	return idx.births[top-1], true
}

// capturePkMoveStreamEvidenceBounded is capturePkMoveStreamEvidence with
// the entry cap spelled out so tests can exercise the fail-closed cap
// without materializing millions of rows.
func capturePkMoveStreamEvidenceBounded(
	ctx context.Context,
	dataHashmap, tombstoneHashmap databranchutils.BranchHashmap,
	parallelism int,
	maxEntries int,
) (departed map[pkMoveDepartKey]types.TS, tombs map[string][]pkMoveTombRef, capped bool, err error) {
	departed = make(map[pkMoveDepartKey]types.TS)
	tombs = make(map[string][]pkMoveTombRef)
	if dataHashmap == nil || tombstoneHashmap == nil {
		return departed, tombs, false, nil
	}
	var mu sync.Mutex
	total := 0
	err = tombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		localDepart := make(map[pkMoveDepartKey]types.TS)
		localTombs := make(map[string][]pkMoveTombRef)
		// One version index per key per shard: every tombstone of the
		// key pairs against the same materialization instead of
		// re-fetching and re-decoding the key's versions. A nil entry
		// marks a key the data map does not hold; pairing skips clean.
		indexes := make(map[string]*pkMoveKeyVersionIndex)
		err := cursor.ForEach(func(key, tombRow []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			tombRID, cts, hasID := decodePkMoveStreamRowIDAndBirth(tombstoneHashmap, tombRow)
			if !hasID {
				return nil
			}
			// Reserve one budget slot atomically with the cap check:
			// parallel shards must never oversubscribe the shared
			// budget, and the capped flag must be observed the moment
			// any shard exhausts it.
			mu.Lock()
			if capped || total >= maxEntries {
				capped = true
				mu.Unlock()
				return nil
			}
			total++
			mu.Unlock()
			// Cursor keys alias internal storage; the index outlives the
			// iteration, so copy.
			ks := string(append([]byte(nil), key...))
			localTombs[ks] = append(localTombs[ks], pkMoveTombRef{rowID: tombRID, cts: cts})
			// Pair the tombstone to the version it deleted through the
			// key's reusable version index: the data map is consulted
			// once per key, so a churned key's K tombstones no longer
			// re-fetch and re-decode its V versions K times over. The
			// pairing rule itself is unchanged: tier 1 is the newest
			// exact row-id match not born after the deletion, tier 2 —
			// only when tier 1 found nothing — the newest birth strictly
			// below it across every row id of the key, and two rows
			// sharing either tier's greatest birth are corruption that
			// records nothing, so hops over this deletion fail closed.
			idx, seen := indexes[ks]
			if !seen {
				dataRet, gErr := dataHashmap.GetByEncodedKey(key)
				if gErr != nil {
					return gErr
				}
				if dataRet.Exists {
					idx = buildPkMoveKeyVersionIndex(dataHashmap, dataRet)
				}
				indexes[ks] = idx
			}
			if idx == nil {
				return nil
			}
			if birth, ok := idx.deletedVersionBirth(tombRID, cts); ok {
				localDepart[pkMoveDepartKey{rowID: tombRID, cts: cts}] = birth
			}
			return nil
		})
		if err != nil {
			return err
		}
		mu.Lock()
		for k, b := range localDepart {
			departed[k] = b
		}
		for ks, refs := range localTombs {
			tombs[ks] = append(tombs[ks], refs...)
		}
		mu.Unlock()
		return nil
	}, parallelism)
	if err != nil {
		return nil, nil, false, err
	}
	if capped {
		// Fail the whole evidence space closed: partial indexes are
		// unsound for continuity checks. The caller decides between a
		// proven move-free legacy replay and a loud apply abort.
		logutil.Warn(
			"DataBranch-PkMoveEvidence-Capped",
			zap.Int("max-entries", maxEntries),
		)
		return make(map[pkMoveDepartKey]types.TS), make(map[string][]pkMoveTombRef), true, nil
	}
	// Deterministic per-key ordering: chain walks iterate these indexes
	// repeatedly, and a stable (cts, rowid) order keeps fold decisions
	// independent of shard merge order.
	for ks := range tombs {
		refs := tombs[ks]
		sort.Slice(refs, func(i, j int) bool {
			if refs[i].cts.EQ(&refs[j].cts) {
				return bytes.Compare(refs[i].rowID[:], refs[j].rowID[:]) < 0
			}
			return refs[i].cts.LT(&refs[j].cts)
		})
	}
	return departed, tombs, false, nil
}

// expandPickKeysWithPkMoveLinks widens a PICK key set along recorded pk
// moves so a moved row can be reassembled: starting from the literal picked
// keys, every in-window link whose old or new key is selected contributes
// its opposite endpoint, iterating to a fixpoint so chains close in both
// directions. Only links at or below each window's endPhys participate:
// moves younger than the selected endpoint are future history. There is no
// lower bound by design: write_ts is the recording transaction's CREATE
// timestamp, so a straddling transaction (created before the change window
// / BETWEEN boundary, committing inside it) would be filtered out and its
// departure would read as a same-key tail claiming an unrelated rebirth.
// Commit instants decide real change-window membership when the stream is
// replayed, so full history only adds provable keys.
//
// Keys the closure ADDS are dependency keys, not user selection: their
// encoded forms are returned through depSet so the PICK consumer can keep
// the original selection distinct from dependency keys (dependency rows
// not claimed by a proven move must not be applied, and a folded move may
// only be produced for a literally selected anchor).
//
// The closure is failure-honest but non-fatal: a whole-window scan whose
// LIMIT fired used to stop the expansion entirely — silently losing the
// replacement keys of every selected move once UNRELATED history crossed
// the reader budget (r25 review F4). An overflowed window now narrows to
// per-frontier-key departures probes (probePickWindowKeysIndividually):
// each selected key's own chain completes as long as its own history fits
// the budget, a key whose own history crosses it fails closed alone, and
// the query/row caps stop the expansion with whatever already proved
// sound. Fewer dependency keys can only reduce folding (legacy replay
// becomes the replay), never fabricate one.
func expandPickKeysWithPkMoveLinks(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	windows []pkMoveLinkTableWindow,
	tblStuff tableStuff,
	pickKeyHashmap databranchutils.BranchHashmap,
	depSet map[string]struct{},
) (added bool, err error) {
	if pickKeyHashmap == nil || tblStuff.def.pkKind == fakeKind ||
		len(tblStuff.def.pkColIdxes) == 0 {
		return false, nil
	}
	usable := windows[:0]
	for _, w := range windows {
		if w.tableID != 0 {
			usable = append(usable, w)
		}
	}
	if bh == nil || len(usable) == 0 {
		return false, nil
	}
	if pickKeyHashmap.ItemCount() == 0 {
		// No keys were materialized, and probing a keyless hashmap by encoded
		// key is rejected, so nothing can expand.
		return false, nil
	}
	exists, err := pkMoveLinkTableExists(ctx, ses, bh)
	if err != nil || !exists {
		return false, err
	}

	pkType := tblStuff.def.colTypes[tblStuff.def.pkColIdx]
	mp := ses.proc.Mp()
	addKey := func(encoded []byte) error {
		tuple, _, decErr := pickKeyHashmap.DecodeRow(encoded)
		if decErr != nil {
			return decErr
		}
		if len(tuple) != 1 {
			return moerr.NewInternalErrorNoCtxf(
				"unexpected pk-move link key width %d", len(tuple))
		}
		vec := vector.NewVec(pkType)
		defer vec.Free(mp)
		if appErr := appendTupleValueToVector(vec, tuple[0], mp); appErr != nil {
			return appErr
		}
		return pickKeyHashmap.PutByVectors([]*vector.Vector{vec}, []int{0})
	}

	// member reports whether encoded is already in the (growing) pick set.
	member := func(encoded []byte) (bool, error) {
		res, gerr := pickKeyHashmap.GetByEncodedKey(encoded)
		if gerr != nil {
			return false, gerr
		}
		return res.Exists, nil
	}

	// frontier accumulates encoded keys newly admitted in each round; each
	// round probes the catalog only for moves touching the frontier.
	frontier := make(map[string]struct{}, 64)
	// windowTruncated remembers which windows already overran the shared
	// row budget. The whole-window scan SQL carries no per-round state and
	// reads one snapshot, so a truncated verdict is stable for the rest of
	// this expansion: re-issuing it would return the same truncated page
	// every round. Later rounds of a truncated window go straight to the
	// per-frontier-key narrowing probes (r25 review F4).
	windowTruncated := make([]bool, len(usable))
	admitted := 0
	for round := 0; round < pkMoveLinkMaxChain; round++ {
		var probeKeys [][]byte
		if round == 0 {
			// Seed with the literal keys: enumerate them through the
			// dependency set complement is impossible, so read the frontier
			// from a one-shot scan of the pick hashmap below.
			probeKeys, err = enumeratePickKeys(pickKeyHashmap)
			if err != nil {
				return false, err
			}
		} else {
			probeKeys = probeKeys[:0]
			for k := range frontier {
				probeKeys = append(probeKeys, []byte(k))
			}
		}
		if len(probeKeys) == 0 {
			break
		}
		clear(frontier)

		var links []pkMoveLink
		for wi, window := range usable {
			var one []pkMoveLink
			var probeErr error
			if windowTruncated[wi] {
				one, probeErr = probePickWindowKeysIndividually(
					ctx, ses, bh, window, probeKeys)
			} else {
				var truncated bool
				one, truncated, probeErr = probePickWindowLinks(
					ctx, ses, bh, window, probeKeys)
				if probeErr == nil && truncated {
					// The window scan is no longer complete evidence once
					// UNRELATED history crosses the budget — narrow to
					// per-key prefix probes of exactly this round's
					// frontier instead of abandoning the selected moves'
					// closure (r25 review F4).
					windowTruncated[wi] = true
					one, probeErr = probePickWindowKeysIndividually(
						ctx, ses, bh, window, probeKeys)
				}
			}
			if probeErr != nil {
				return false, probeErr
			}
			links = append(links, one...)
		}
		for _, link := range links {
			if err = ctx.Err(); err != nil {
				return false, err
			}
			oldIn, err1 := member(link.oldPK)
			if err1 != nil {
				return false, err1
			}
			newIn, err2 := member(link.newPK)
			if err2 != nil {
				return false, err2
			}
			var admit []byte
			switch {
			case oldIn && !newIn:
				admit = link.newPK
			case !oldIn && newIn:
				admit = link.oldPK
			default:
				continue
			}
			if admitted >= pkMoveClosureMaxAdmittedKeys {
				// Guard against corrupt topology: stop expanding (fail
				// closed), keeping only keys admitted so far.
				logutil.Warn(
					"DataBranch-PkMoveLink-PickClosureKeyCap",
					zap.Int("admitted", admitted),
				)
				return added, nil
			}
			if err = addKey(admit); err != nil {
				return false, err
			}
			admitted++
			added = true
			frontier[string(admit)] = struct{}{}
			if depSet != nil {
				depSet[string(admit)] = struct{}{}
			}
		}
		if len(frontier) == 0 {
			break
		}
	}
	return added, nil
}

// enumeratePickKeys lists the encoded keys stored in a pick hashmap by
// scanning its shards directly. Shard callbacks run in parallel, so each
// cursor collects privately and the results merge afterwards.
func enumeratePickKeys(pickKeyHashmap databranchutils.BranchHashmap) ([][]byte, error) {
	var (
		mu   sync.Mutex
		keys [][]byte
	)
	err := pickKeyHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var local [][]byte
		if err := cursor.ForEach(func(key []byte, _ []byte) error {
			local = append(local, append([]byte(nil), key...))
			return nil
		}); err != nil {
			return err
		}
		mu.Lock()
		keys = append(keys, local...)
		mu.Unlock()
		return nil
	}, -1)
	return keys, err
}

// pkMovePickProbeSQL builds the 4-column closure probe over one link_id
// prefix range [lo, hi). The +1 on the LIMIT must be handled by the
// caller: it makes "history truncated at the budget" observable. Full
// history through endPhys with no write_ts lower bound: write_ts is the
// recording transaction's CREATE timestamp, so a straddling transaction
// (created before the change window / BETWEEN boundary, committing inside
// it) would be filtered out and its departure would read as a same-key
// tail claiming an unrelated rebirth; stream commit instants decide real
// window membership during the walk.
func pkMovePickProbeSQL(lo, hi []byte, endPhys uint64, limit int) string {
	return fmt.Sprintf(
		"select %s, %s, %s, %s from %s.%s where %s >= x'%x' and %s < x'%x' and %s <= %d order by %s limit %d",
		catalog.DataBranchUpdateLinkColOldRow,
		catalog.DataBranchUpdateLinkColOldPK,
		catalog.DataBranchUpdateLinkColNewPK,
		catalog.DataBranchUpdateLinkColWriteTS,
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK,
		catalog.DataBranchUpdateLinkColLinkID, lo,
		catalog.DataBranchUpdateLinkColLinkID, hi,
		catalog.DataBranchUpdateLinkColWriteTS, endPhys,
		catalog.DataBranchUpdateLinkColLinkID, limit,
	)
}

// readPickWindowLinkRows runs one closure probe and decodes its rows.
// cut reports that the LIMIT fired; a cut page must never be consumed as
// complete evidence.
func readPickWindowLinkRows(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	sql string,
	limit int,
) (out []pkMoveLink, cut bool, err error) {
	ret, sqlErr := runSql(ctx, ses, bh, sql, nil, nil)
	if sqlErr != nil {
		return nil, false, sqlErr
	}
	defer ret.Close()
	var loadErr error
	ret.ReadRows(func(rowCnt int, cols []*vector.Vector) bool {
		if len(cols) != 4 {
			loadErr = moerr.NewInternalErrorNoCtxf(
				"unexpected pk-move closure probe width %d", len(cols))
			return false
		}
		for i := 0; i < rowCnt; i++ {
			rowIDBytes := cols[0].GetBytesAt(i)
			if len(rowIDBytes) != types.RowidSize {
				loadErr = moerr.NewInternalErrorNoCtxf(
					"unexpected pk-move link rowid width %d", len(rowIDBytes))
				return false
			}
			var link pkMoveLink
			copy(link.oldRowID[:], rowIDBytes)
			link.oldPK = append([]byte(nil), cols[1].GetBytesAt(i)...)
			link.newPK = append([]byte(nil), cols[2].GetBytesAt(i)...)
			writeTS := vector.MustFixedColNoTypeCheck[uint64](cols[3])[i]
			link.writeTS = types.BuildTS(int64(writeTS), 0)
			out = append(out, link)
		}
		return true
	})
	if loadErr != nil {
		return nil, false, loadErr
	}
	return out, len(out) >= limit, nil
}

// probePickWindowLinks loads links of one table window at or below its
// endPhys (full history below the endpoint: write_ts is a creation-time
// value, so straddling transactions must not be filtered out — membership
// is decided by stream commit instants during replay) touching any of keys
// as old OR new key, from one capped scan of the table's whole
// link_id prefix filtered in memory by both key sha sets (secondary
// indexes are unusable here; see pkMoveLinkPrefixRange). An older shape
// probed the old-key direction with chunked (sha, sha+1) range ORs; the
// planning cost of that predicate grows superlinearly with the range count
// and drowned large picks, while the single scan reads the same rows at
// flat cost. The statement is LIMITed by the reader budget, so oversized
// history is never materialized; truncated == true reports that the LIMIT
// fired, in which case the caller must treat the read as incomplete
// rather than consume a silent prefix.
func probePickWindowLinks(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	window pkMoveLinkTableWindow,
	keys [][]byte,
) (links []pkMoveLink, truncated bool, err error) {
	// One LIMITed window scan of the table prefix serves both directions:
	// a fired LIMIT means the scan covered only a prefix, so closure
	// against either key direction would be based on incomplete evidence —
	// report truncated instead.
	if len(keys) == 0 {
		return links, false, nil
	}
	lo, hi, ok := pkMoveLinkPrefixRange(window.tableID, nil)
	if !ok {
		return links, false, nil
	}
	limit := pkMoveLinkReadBudget(window.tableID) + 1
	// Full history through endPhys here too: no write_ts lower bound
	// (straddlers commit inside the change window with a CREATE
	// timestamp outside it; see expandPickKeysWithPkMoveLinks).
	sql := pkMovePickProbeSQL(lo, hi, window.endPhys, limit)
	var scanned []pkMoveLink
	var cut bool
	if scanned, cut, err = readPickWindowLinkRows(ctx, ses, bh, sql, limit); err != nil {
		return nil, false, err
	}
	if cut {
		logutil.Warn(
			"DataBranch-PkMoveLink-PickWindowScanTruncated",
			zap.Uint64("table-id", window.tableID),
		)
		return links, true, nil
	}
	keySha := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		sha := sha256.Sum256(k)
		keySha[string(sha[:])] = struct{}{}
	}
	for _, link := range scanned {
		oldSha := sha256.Sum256(link.oldPK)
		newSha := sha256.Sum256(link.newPK)
		_, oldHit := keySha[string(oldSha[:])]
		_, newHit := keySha[string(newSha[:])]
		if !oldHit && !newHit {
			continue
		}
		links = append(links, link)
	}
	return links, false, nil
}

// probePickWindowKeysIndividually serves one OVERFLOWED window by probing
// exactly the frontier keys' own (sha, sha+1) departures ranges: the
// whole-window scan can no longer serve as complete evidence once
// UNRELATED history crosses the reader budget, and discarding the
// expansion outright would abort every selected pk move's reassembly even
// when its own chain history is tiny (r25 review F4). Each key rides one
// LIMITed prefix query; a fired LIMIT means THAT key's own departure
// history crosses the budget, so its chain fails closed (it contributes
// nothing and expansion stops from it) while every other frontier key
// keeps its clean answer. Two bounds keep the fallback cheap: at most
// pkMovePickClosureProbeQueryMax round-trips per call, and at most
// pkMoveLinkMaxCachedRows materialized rows in total; hitting either one
// stops the expansion with whatever keys already proved sound (all of
// them materially present in the result), never with a truncated prefix.
//
// Only the OLD-key direction is prefix-reachable (link_id carries the
// old-pk sha; see pkMoveLinkPrefixRange), so under overflow the closure
// completes the forward chain of every selected frontier key exactly,
// while arrivals AT a selected key stay unenumerable: a landed-key-only
// pick then replays legacy row apply for that key, the same sound
// semantics the pre-closure apply always had.
func probePickWindowKeysIndividually(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	window pkMoveLinkTableWindow,
	keys [][]byte,
) (links []pkMoveLink, err error) {
	remainRows := pkMoveLinkReadBudget(window.tableID)
	issued := 0
	for _, k := range keys {
		if len(k) == 0 {
			continue
		}
		if issued >= pkMovePickClosureProbeQueryMax || remainRows <= 0 {
			logutil.Warn(
				"DataBranch-PkMoveLink-PickClosureProbeBudget",
				zap.Uint64("table-id", window.tableID),
				zap.Int("issued", issued),
				zap.String("action", "stop expanding with the keys already proved sound"),
			)
			return links, nil
		}
		sha := sha256.Sum256(k)
		lo, hi, ok := pkMoveLinkPrefixRange(window.tableID, sha[:])
		if !ok {
			// The prefix overflows at MaxUint64/all-0xff: this key's range
			// is unrepresentable, so it fails closed alone.
			logutil.Warn(
				"DataBranch-PkMoveLink-PickClosureKeyRangeOverflow",
				zap.Uint64("table-id", window.tableID),
			)
			continue
		}
		issued++
		one, cut, qerr := readPickWindowLinkRows(
			ctx, ses, bh,
			pkMovePickProbeSQL(lo, hi, window.endPhys, remainRows+1),
			remainRows+1)
		if qerr != nil {
			return nil, qerr
		}
		if cut {
			// This key's OWN departure history crosses the remaining
			// budget: drop its page wholesale and keep every other key's
			// clean answer.
			logutil.Warn(
				"DataBranch-PkMoveLink-PickClosureKeyOverflow",
				zap.Uint64("table-id", window.tableID),
				zap.Int("budget", remainRows),
				zap.String("action", "key fails closed as incomplete"),
			)
			continue
		}
		links = append(links, one...)
		remainRows -= len(one)
	}
	return links, nil
}
