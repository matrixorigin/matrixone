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

package disttae

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"go.uber.org/zap"
)

// The data-branch pk-move provenance protocol (design doc
// docs/design/20261016-data-branch-pk-move-update-provenance.md) stores one
// row per pk-moving UPDATE in mo_catalog.mo_data_branch_update_link. The
// link's new_row_id column carries the workspace-stage identity of the
// replacement row, and only the workspace knows it: staged row ids are
// allocated (out of colexec.TxnWorkspaceSegment) when a staged insert is
// written, inside a txn-private copy the issuing operator never sees. The
// multi_update operator therefore writes link rows with a NULL new_row_id
// and this file back-fills that column when the link insert is staged,
// from the same transaction's already-staged inserts.
//
// The harvested identity is a pre-commit placeholder: the commit pipeline
// strips workspace rowid vectors from INSERT payloads (toPBEntry), so TN
// assigns the durable row id at precommit and the committed stream row
// never carries the workspace identity. The data-branch diff consumer
// therefore claims a landed generation by its commit-timestamp tie with
// the deleting tombstone, and a NULL new_row_id still folds single-shot.
// The recorded placeholder serves exactly one purpose: chaining
// same-transaction generations that were netted out of the committed
// stream (a replacement re-keyed again inside one transaction leaves no
// stream rows; the next link's old_row_id names the previous link's
// workspace new_row_id, and both carry the same write_ts create ts).
//
// Ordering guarantee: the planner appends the synthetic link ctx directly
// after the owning main-table ctx, and both execute inside one statement,
// so by the time a link batch reaches writeBatchWithAutoIncrEpochKnown the
// workspace holds the replacement rows of exactly this statement. Entries
// staged by EARLIER statements may also be present; matching is by
// (account, table, pk value) and later entries overwrite earlier ones, so
// a same-transaction re-key still resolves to the freshest staged row.
// Anything unresolvable (for example the owning insert went straight to S3
// and never staged a workspace batch) stays NULL: the single-shot claim
// still applies, and only same-transaction netted continuations become
// unprovable (fail closed, never wrong data).

// isPkMoveLinkTableInsert reports whether a staged insert targets the
// pk-move link catalog table. Tables are matched by name because the
// internal catalog assigns ids dynamically per cluster.
func isPkMoveLinkTableInsert(databaseName, tableName string) bool {
	return databaseName == catalog.MO_CATALOG &&
		tableName == catalog.MO_DATA_BRANCH_UPDATE_LINK
}

// pkMoveLinkCoordinatorBudget bounds the retained workspace bytes of one
// transaction's staged pk-move link rows. The producer spill
// (pkMoveLinkMemBudget in multi_update) bounds only that side: every
// ferry wave is read back and staged into the coordinator workspace, and
// link-table inserts are excluded from every pre-commit dump, so without
// an admission bound here the provenance of an arbitrarily large moving
// statement still accumulated in coordinator memory until the final
// commit. Crossing the budget fails the staging statement atomically
// (nothing of the wave enters the workspace); the operator reduces the
// number of primary keys moved per statement to fit. Test-injectable.
var pkMoveLinkCoordinatorBudget int64 = 64 << 20

// pkMoveBookkeepEntryOverhead amortizes the per-entry cost (map bucket
// slot, slice header, interior pointers) of one retained provenance
// bookkeeping entry across the per-statement ledger maps.
const pkMoveBookkeepEntryOverhead = 64

// pkMoveBookkeepBytesLocked measures the transaction's retained provenance
// bookkeeping: the by-key lineage stacks, the statement ledgers, the
// origin and heal maps, and the retirement evidence. Unlike the staged
// link batches these maps are never spilled, and before they rode the
// coordinator budget a marker-only wave (or a bulk non-key S3 update)
// could grow them across statement boundaries with zero charged
// footprint. The measure is computed from the live maps on demand, so
// statement rollback and marker consumption shrink it automatically with
// no release bookkeeping of its own.
func (txn *Transaction) pkMoveBookkeepBytesLocked() int64 {
	var total int64
	for _, set := range txn.pkMoveMarkerStmts {
		for pk, stmts := range set {
			total += int64(len(pk)) + pkMoveBookkeepEntryOverhead
			total += int64(len(stmts)) * 8
		}
	}
	for _, set := range txn.pkMoveDeadPks {
		for pk, stmts := range set {
			total += int64(len(pk)) + pkMoveBookkeepEntryOverhead
			total += int64(len(stmts)) * 8
		}
	}
	for _, set := range txn.pkMoveKeyLineage {
		for pk, stack := range set {
			total += int64(len(pk)) + pkMoveBookkeepEntryOverhead
			total += int64(len(stack)) * pkMoveBookkeepEntryOverhead
		}
	}
	for _, set := range txn.pkMoveRetireDeaths {
		for pk, list := range set {
			total += int64(len(pk)) + pkMoveBookkeepEntryOverhead
			total += int64(len(list)) * (types.RowidSize + 16)
		}
	}
	total += int64(len(txn.pkMoveStagedOldRids)) * (types.RowidSize + 16)
	total += int64(len(txn.pkMoveKilledLinks)) * pkMoveBookkeepEntryOverhead
	total += int64(len(txn.pkMoveRowidOrigins)) * (2*types.RowidSize + 16)
	total += int64(len(txn.pkMoveNullHeals)) * pkMoveBookkeepEntryOverhead
	total += int64(len(txn.pkMoveLinkBirth)) * pkMoveBookkeepEntryOverhead
	// r38 review F2 (D144): the carry journal rides the same umbrella —
	// measured on demand from the live journal, so statement rollback,
	// boundary adoption, and transaction close shrink the charge with no
	// release bookkeeping of their own.
	for i := range txn.pkMoveCarryJournal {
		total += pkMoveBookkeepEntryOverhead +
			int64(len(txn.pkMoveCarryJournal[i].oldIds))*(2*types.RowidSize+16)
	}
	return total
}

// checkPkMoveBookkeepBudgetLocked refuses, statement-atomically, to let
// the retained provenance state cross the coordinator budget. The fill
// calls it only once durable provenance is at stake (real link rows in
// the fill, or staged links already in the workspace): marker-only waves
// in a transaction that never stages a link are consumed continuity
// evidence that dies with the transaction, and charging them rejected
// ordinary bulk non-key UPDATEs (r22 review F3). A participating fill's
// bookkeeping growth is enforced at the fill that recorded it; the
// statement's rollback truncates the ledger entries and the measure
// drops back under the budget. The measure also carries the user-batch
// bytes the pin rule retains: repeated small updates of wide rows retain
// gigabytes while the bookkeeping stays small. The pin measure is
// DERIVED FROM THE LIVE WORKSPACE at every check — never cached across
// calls — so a statement rollback that truncates its writes or a dump
// that releases the retained batches drops the charge with them instead
// of leaving a stale charge behind that would spuriously fail the
// transaction's later small updates. A zero budget disables the check.
func (txn *Transaction) checkPkMoveBookkeepBudgetLocked() error {
	return txn.checkPkMoveBookkeepBudgetForPinnedLocked(
		txn.pkMoveStagedLinkPinnedRowIDsLocked())
}

// checkPkMoveBookkeepBudgetForPinnedLocked is the budget check with the
// pinned-row set supplied by the caller: the dump gate has just computed
// that set to decide the retention itself, so it checks against its own
// instead of scanning the staged link batches a second time.
func (txn *Transaction) checkPkMoveBookkeepBudgetForPinnedLocked(
	pinned map[types.Rowid]struct{},
) error {
	staged := txn.pkMoveLinkStagedBytes
	bookkeep := txn.pkMoveBookkeepBytesLocked()
	pinBytes := txn.pkMovePinnedRetainedBytesLocked(pinned)
	if pkMoveLinkCoordinatorBudget > 0 &&
		staged+bookkeep+pinBytes > pkMoveLinkCoordinatorBudget {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"data branch update provenance exceeds the coordinator retention budget: retained link bytes %d plus bookkeeping bytes %d plus pin-retained workspace bytes %d over budget %d bytes; reduce the number of primary keys moved per statement",
			staged, bookkeep, pinBytes, pkMoveLinkCoordinatorBudget))
	}
	return nil
}

// admitPkMoveLinkBatchLocked charges one staged link batch against the
// coordinator retention budget BEFORE it enters the workspace: the
// workspace accounting hooks add the batch's bytes on append, so
// pkMoveLinkStagedBytes reads exactly the retained total. The retained
// side includes the transaction's provenance bookkeeping, which no spill
// boundary ever releases, and the pin-retained workspace bytes, which
// are derived live so a rollback that released the pinned batches never
// leaves their charge behind. A failure leaves the workspace untouched;
// the statement's own rollback then unwinds its earlier writes as for
// any staging failure. A zero budget disables admission.
func (txn *Transaction) admitPkMoveLinkBatchLocked(bat *batch.Batch) error {
	sz := int64(bat.Size())
	retained := txn.pkMoveLinkStagedBytes + txn.pkMoveBookkeepBytesLocked() +
		txn.pkMovePinnedRetainedBytesLocked(txn.pkMoveStagedLinkPinnedRowIDsLocked())
	if pkMoveLinkCoordinatorBudget > 0 &&
		retained+sz > pkMoveLinkCoordinatorBudget {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"data branch update provenance exceeds the coordinator retention budget: retained %d bytes, staging %d bytes, budget %d bytes; reduce the number of primary keys moved per statement",
			retained, sz, pkMoveLinkCoordinatorBudget))
	}
	return nil
}

// Allocation-failure injection seams: the provenance bookkeeping touches
// the mpool only through these two vector operations, and their failures
// must surface as statement errors (a swallowed failure used to let an
// UPDATE or a rollback continue with incomplete or unrestored identity
// state). Tests substitute faulting implementations to exercise exactly
// those failure paths.
var (
	pkMoveVecAppendBytes = vector.AppendBytes
	pkMoveVecSetBytesAt  = vector.SetBytesAt
	// pkMoveVecOverwriteRowidAt is the retained-cell replacement seam of
	// r40 review F2 (D148); tests substitute faulting implementations to
	// exercise its statement-failure paths like any other mpool-touching
	// seam above.
	pkMoveVecOverwriteRowidAt = pkMoveVecOverwriteRowidAtBase
)

// pkMoveVecOverwriteRowidAtBase replaces one RETAINED, already-accounted
// 24-byte row-id varlena cell IN PLACE (r40 review F2, D148). Every
// payload is types.RowidSize — never inlinable (VarlenaInlineSize = 23) —
// so the displaced bytes are area-backed, and copying the replacement
// over the same window leaves len(v.area), the descriptors, and the
// batch's measured size ALL unchanged, forward and inverse alike.
// vector.SetBytesAt cannot promise that: it appends a fresh area range
// and leaves the displaced one in the measured area until batch recycle,
// so repeated failed carry attempts consumed the coordinator budget even
// though the logical row ids restored. There is NO appending fallback on
// this path: a precondition violation would mint uncharged retained
// growth, so it fails the enclosing statement loudly instead.
// MaterializeOwned performs the copy-on-write transactionally — an
// allocation failure leaves the vector unchanged and readable. Initial
// writes into vectors under construction keep pkMoveVecSetBytesAt: their
// growth IS the batch's measured construction.
func pkMoveVecOverwriteRowidAtBase(vec *vector.Vector, row int, data []byte, mp *mpool.MPool) error {
	if len(data) != types.RowidSize {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"pk-move rowid in-place overwrite: replacement payload is %d bytes, want %d",
			len(data), types.RowidSize))
	}
	if vec.IsConst() {
		return moerr.NewInternalErrorNoCtx(
			"pk-move rowid in-place overwrite: const vector")
	}
	if row < 0 || row >= vec.Length() {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"pk-move rowid in-place overwrite: row %d outside vector length %d",
			row, vec.Length()))
	}
	if vec.IsNull(uint64(row)) {
		return moerr.NewInternalErrorNoCtx(
			"pk-move rowid in-place overwrite: null cell")
	}
	if displaced := vec.GetBytesAt(row); len(displaced) != types.RowidSize {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"pk-move rowid in-place overwrite: displaced payload is %d bytes, want %d",
			len(displaced), types.RowidSize))
	}
	if err := vec.MaterializeOwned(mp); err != nil {
		return err
	}
	window := vec.GetBytesAt(row)
	if len(window) != types.RowidSize {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"pk-move rowid in-place overwrite: materialized window is %d bytes, want %d",
			len(window), types.RowidSize))
	}
	copy(window, data)
	return nil
}

// fillPkMoveLinkNewRowIDsLocked back-fills the new_row_id column of a staged
// link-table insert batch. Must be called with txn.Lock held, after the
// rowid vector has been prepended to bat, and before the workspace entry is
// formed so the final memory accounting sees the filled column.
//
// Same-key continuity markers (old pk bytes == new pk bytes, emitted by pk-
// unchanged updates of workspace-minted rows) are consumed here, never
// committed: a marker means "the row previously known as old_row_id was
// replaced by a non-key edit; link rows naming it must follow the lineage".
// For each resolved marker this hook (a) RETARGETS every previously staged
// link row whose new_row_id names the marker's pre-image onto the fresh
// replacement, keeping their liveness observable to the commit prune, and
// (b) records marker.replacement -> lineage-origin in
// txn.pkMoveRowidOrigins so a LATER statement's move of this row stages
// with the origin identity the committed stream will actually show
// departing. Rows of the new batch itself get their old_row_id rewritten
// through that same origin map first. Marker rows are shrunk out of the
// staged batch last.
//
// The returned error reports allocation and retarget failures: the fill
// caller fails the statement so incomplete provenance can never commit,
// and the ensuing statement rollback restores whatever the failed pass
// already changed.
func (txn *Transaction) fillPkMoveLinkNewRowIDsLocked(accountId uint32, bat *batch.Batch) error {
	tableIdx, newPKIdx, newRowIdx, oldPKIdx, oldRowIdx, linkIDIdx := -1, -1, -1, -1, -1, -1
	for i, attr := range bat.Attrs {
		switch attr {
		case catalog.DataBranchUpdateLinkColTableID:
			tableIdx = i
		case catalog.DataBranchUpdateLinkColNewPK:
			newPKIdx = i
		case catalog.DataBranchUpdateLinkColNewRow:
			newRowIdx = i
		case catalog.DataBranchUpdateLinkColOldPK:
			oldPKIdx = i
		case catalog.DataBranchUpdateLinkColOldRow:
			oldRowIdx = i
		case catalog.DataBranchUpdateLinkColLinkID:
			linkIDIdx = i
		}
	}
	if tableIdx < 0 || newPKIdx < 0 || newRowIdx < 0 || oldPKIdx < 0 || oldRowIdx < 0 {
		// Schema drift (mixed-version catalog): leave the column NULL; the
		// consumer folds by commit-timestamp tie and only same-transaction
		// netted continuations become unprovable.
		return nil
	}
	tableVec := bat.Vecs[tableIdx]
	newPKVec := bat.Vecs[newPKIdx]
	oldPKVec := bat.Vecs[oldPKIdx]
	oldRowVec := bat.Vecs[oldRowIdx]
	if tableVec.GetType().Oid != types.T_uint64 {
		return nil
	}
	rowCount := bat.RowCount()
	if rowCount == 0 {
		return nil
	}
	tableIDs := vector.MustFixedColNoTypeCheck[uint64](tableVec)
	if len(tableIDs) < rowCount {
		return nil
	}

	isMarker := make([]bool, rowCount)
	var keepSels []int64
	for i := 0; i < rowCount; i++ {
		isMarker[i] = !oldPKVec.IsNull(uint64(i)) && !newPKVec.IsNull(uint64(i)) &&
			bytes.Equal(oldPKVec.GetBytesAt(i), newPKVec.GetBytesAt(i))
		if !isMarker[i] {
			keepSels = append(keepSels, int64(i))
		}
	}

	// Remember every staged old identity this fill observes: the
	// retirement evaluation explains a diverted death exactly when some
	// staged link or marker named the murdered generation as its own old
	// side (the death is that departure going through provenance).
	// Populate-missing wins: later fills must not restamp an earlier
	// fill's rows, and heals below rewrite the cells, never this set.
	for i := 0; i < rowCount; i++ {
		if oldRowVec.IsNull(uint64(i)) {
			continue
		}
		raw := oldRowVec.GetBytesAt(i)
		if len(raw) != types.RowidSize {
			continue
		}
		rid := types.DecodeFixed[types.Rowid](raw)
		if txn.pkMoveStagedOldRids == nil {
			txn.pkMoveStagedOldRids = make(map[types.Rowid]int)
		}
		if _, ok := txn.pkMoveStagedOldRids[rid]; !ok {
			txn.pkMoveStagedOldRids[rid] = txn.statementID
		}
	}

	// Pass 1: old_row_id origin rewrite. A previous statement's same-key
	// edit replaced the pre-image this batch's rows name; the committed
	// stream will show only the ORIGIN generation departing, so link rows
	// must name the origin to anchor the fold. Marker rows are exempt:
	// they never commit, and their old_row_id is Pass 2's retarget
	// pre-image, which must stay the IMMEDIATE predecessor generation
	// the edit replaced (rewriting it to the durable origin would aim the
	// retarget at a generation no staged link cell or heal binding names).
	if len(txn.pkMoveRowidOrigins) > 0 {
		for i := 0; i < rowCount; i++ {
			if isMarker[i] || oldRowVec.IsNull(uint64(i)) {
				continue
			}
			raw := oldRowVec.GetBytesAt(i)
			if len(raw) != types.RowidSize {
				continue
			}
			if origin, ok := txn.pkMoveRowidOrigins[types.DecodeFixed[types.Rowid](raw)]; ok {
				if err := pkMoveVecSetBytesAt(oldRowVec, i, origin[:], txn.proc.Mp()); err != nil {
					return err
				}
			}
		}
	}

	// Pass 1b: by-key lineage heals for the S3 route. A same-key edit
	// whose replacement never staged (an S3-buffered write) could not
	// enter the exact map above, so a later statement moving the row
	// still names the S3-buffered identity as old_row_id while the
	// committed stream shows the marker's durable pre-image departing.
	// Heal that recorded identity back to the durable origin a valid
	// binding names, or the consumer's tombstone screen misses the
	// lineage's own departure and the fold can adopt a foreign
	// same-key departure (the two-lineage misadoption).
	if len(txn.pkMoveKeyLineage) > 0 {
		for i := 0; i < rowCount; i++ {
			if isMarker[i] || oldRowVec.IsNull(uint64(i)) ||
				tableVec.IsNull(uint64(i)) || oldPKVec.IsNull(uint64(i)) {
				continue
			}
			raw := oldRowVec.GetBytesAt(i)
			if len(raw) != types.RowidSize {
				continue
			}
			rid := types.DecodeFixed[types.Rowid](raw)
			if _, ok := txn.pkMoveRowidOrigins[rid]; ok {
				// The exact map governs; it already rewrote above.
				continue
			}
			retired, herr := txn.healPkMoveLinkOldRowLocked(
				pkMoveAcctTbl{account: accountId, table: tableIDs[i]},
				string(oldPKVec.GetBytesAt(i)), oldRowVec, i)
			if herr != nil {
				return herr
			}
			if retired {
				// The bound lineage died before the generation this row
				// moved: kill the link so the prune drops it and the
				// apply replays the delete semantics the committed
				// history actually carries. Rolling this statement back
				// revives the binding and drops the kill record with it.
				if 0 <= linkIDIdx && linkIDIdx < len(bat.Vecs) {
					linkIDVec := bat.Vecs[linkIDIdx]
					if !linkIDVec.IsNull(uint64(i)) {
						if txn.pkMoveKilledLinks == nil {
							txn.pkMoveKilledLinks = make(map[string]int)
						}
						txn.pkMoveKilledLinks[string(linkIDVec.GetBytesAt(i))] = txn.statementID
					}
				}
			}
		}
	}

	// Harvest through the transaction's incremental cache: re-encoding the
	// whole workspace for every staged link batch is quadratic in the
	// moving transaction's size.
	resolve := func(i int) (types.Rowid, bool) {
		var rid types.Rowid
		if newPKVec.IsNull(uint64(i)) || tableVec.IsNull(uint64(i)) {
			return rid, false
		}
		harvest := txn.pkMoveHarvestNewRowIDsCachedLocked(accountId, tableIDs[i])
		rid, hit := harvest[string(newPKVec.GetBytesAt(i))]
		return rid, hit
	}

	// Pass 2: retarget and origin composition for markers, in row order.
	// The continuity index builds LAZILY on the first resolved marker: a
	// fill with nothing but straight moves (the bulk pk-change workload)
	// never pays for it, while a fill of N markers over L staged links
	// pays O(L + N) instead of N full staged-set scans. The diverted-death
	// reverse index (r26 review F5) follows the same discipline for the
	// superseded-generation retirement below.
	var contIdx *pkMoveContinuityIndex
	var deathsByRid map[types.Rowid]struct{}
	for i := 0; i < rowCount; i++ {
		if !isMarker[i] {
			continue
		}
		// Continuity bookkeeping survives drop: the marker itself never
		// commits, but the S3-death evaluation needs to know this
		// statement CONTINUED the key, or a diverted death of the edit's
		// own predecessor generation would look like the replacement's
		// murder.
		if !oldPKVec.IsNull(uint64(i)) && !tableVec.IsNull(uint64(i)) {
			txn.pkMoveMarkerStmts = notePkMoveStatement(
				txn.pkMoveMarkerStmts,
				pkMoveAcctTbl{account: accountId, table: tableIDs[i]},
				string(oldPKVec.GetBytesAt(i)), txn.statementID)
		}
		fresh, resolved := resolve(i)
		if !resolved {
			// Unresolved markers (the edit's replacement never staged,
			// e.g. the S3 buffering route) carry no REPLACEMENT identity —
			// but the marker still proves which durable lineage the key's
			// current row descends from. Record that origin so a later
			// statement's move of the row can heal its old side (Pass 1b);
			// discarding the marker here was what let an S3 same-key edit
			// redirect a move onto another lineage.
			//
			// A marker whose pre-image is a WORKSPACE-stage identity the
			// new side of staged links still names describes a route
			// switch (workspace move, then an S3-route same-key edit):
			// the edit consumes that staged generation, and the
			// replacement now lives only as the S3 route's committed
			// image. Continue the provenance explicitly by retargeting
			// those links' new_row_id to the S3 NULL identity — the
			// consumer's single-shot claim then governs the landed
			// generation exactly as for any S3-route move — or the commit
			// prune reads the consumed workspace identity as murdered
			// and drops the whole lineage's provenance.
			if !oldRowVec.IsNull(uint64(i)) {
				if raw := oldRowVec.GetBytesAt(i); len(raw) == types.RowidSize {
					pre := types.DecodeFixed[types.Rowid](raw)
					if pkMoveIsWorkspaceStageRowid(&pre) {
						// Same lazy build as the resolved side: a fill of
						// nothing but straight moves never pays for the
						// index, while one carrying M route switches over
						// L staged links pays O(L + M) instead of M full
						// staged-set scans under txn.Lock.
						if contIdx == nil && len(txn.pkMoveLinkBirth) > 0 {
							contIdx = txn.buildPkMoveContinuityIndexLocked()
						}
						if err := txn.retargetPkMoveLinkNewRowIDsToNullLocked(pre, contIdx); err != nil {
							return err
						}
					}
				}
			}
			txn.bindPkMoveKeyLineageLocked(
				pkMoveAcctTbl{account: accountId, table: tableIDs[i]},
				string(oldPKVec.GetBytesAt(i)), oldRowVec, i)
			continue
		}
		if !oldRowVec.IsNull(uint64(i)) {
			if raw := oldRowVec.GetBytesAt(i); len(raw) == types.RowidSize {
				if contIdx == nil && len(txn.pkMoveLinkBirth) > 0 {
					contIdx = txn.buildPkMoveContinuityIndexLocked()
				}
				preImage := types.DecodeFixed[types.Rowid](raw)
				if err := txn.retargetPkMoveLinkNewRowIDsLocked(preImage, fresh, contIdx); err != nil {
					return err
				}
				// NULL-identity (S3-route) links naming this key bind to
				// the workspace generation the marker established, so the
				// keep evaluation tracks their lineage's real replacement.
				txn.continuePkMoveNullLinksLocked(
					pkMoveAcctTbl{account: accountId, table: tableIDs[i]},
					oldPKVec.GetBytesAt(i), fresh, contIdx)
				// r26 review F5 (D112): the superseded pre-image cannot be
				// named by any FUTURE diverted death — its batch row
				// ceases to be visible with this composition — so its
				// staged-old coverage entry serves only deaths already
				// recorded. Retire it when none exists; a recorded
				// diverted death keeps its explanation. Rollback-safe: a
				// rolled-back supersession revives the batch row, and a
				// death of it is then correctly unexplained — the
				// departure went away with the statement.
				if deathsByRid == nil && len(txn.pkMoveRetireDeaths) > 0 {
					deathsByRid = make(map[types.Rowid]struct{})
					for _, perKey := range txn.pkMoveRetireDeaths {
						for _, list := range perKey {
							for _, d := range list {
								deathsByRid[d.rid] = struct{}{}
							}
						}
					}
				}
				if _, open := deathsByRid[preImage]; !open {
					delete(txn.pkMoveStagedOldRids, preImage)
				}
				// Record the lineage only when it reaches a DURABLE
				// generation: a later move of 'fresh' is that lineage's
				// chain head, and its link must name the durable row id the
				// committed stream shows departing (fill Pass 1 rewrites the
				// old side through this map). Workspace-lineage generations
				// (insert- or move-minted) anchor literally to the netted
				// stream insert, so they keep no origin entry; a stitch
				// link's old side must also stay literal, or it loses the
				// predecessor's retargeted new_row_id and the prune drops
				// the whole head as a dead chain.
				origin, ok := txn.pkMoveRowidOrigins[preImage]
				if !ok {
					if pkMoveIsWorkspaceStageRowid(&preImage) {
						continue
					}
					origin = preImage
				}
				if txn.pkMoveRowidOrigins == nil {
					txn.pkMoveRowidOrigins = make(map[types.Rowid]types.Rowid)
				}
				txn.pkMoveNoteOriginWriteLocked(fresh)
				txn.pkMoveRowidOrigins[fresh] = origin
				if ok {
					// r26 review F5: the superseded generation is dead the
					// moment the retarget above re-pointed every staged
					// link cell and null heal to 'fresh' — retire its
					// origin entry so repeated same-key updates retain
					// O(live rows) bookkeeping instead of one entry per
					// UPDATE forever. Journaled like every origin write,
					// so a rolled-back statement revives exactly the
					// generation it superseded.
					txn.pkMoveNoteOriginWriteLocked(preImage)
					delete(txn.pkMoveRowidOrigins, preImage)
				}
			}
		}
	}

	old := bat.Vecs[newRowIdx]
	filled := vector.NewVec(*old.GetType())
	for i := 0; i < rowCount; i++ {
		var newRowID types.Rowid
		resolved := false
		if !isMarker[i] {
			newRowID, resolved = resolve(i)
		}
		if resolved {
			if err := pkMoveVecAppendBytes(filled, newRowID[:], false, txn.proc.Mp()); err != nil {
				filled.Free(txn.proc.Mp())
				return err
			}
		} else if err := pkMoveVecAppendBytes(filled, nil, true, txn.proc.Mp()); err != nil {
			filled.Free(txn.proc.Mp())
			return err
		}
	}
	bat.Vecs[newRowIdx] = filled
	old.Free(txn.proc.Mp())

	// Pass 3: markers are consumed; drop them so they never commit. The
	// empty-batch case is safe: workspace entries with zero rows are
	// skipped by dump, prune and request generation alike.
	if len(keepSels) != rowCount {
		bat.Shrink(keepSels, false)
		bat.SetRowCount(bat.Vecs[0].Length())
	}

	// Pass 4: statement bookkeeping for the surviving rows. link ids
	// remember their staging statement (the S3-death evaluation compares
	// statement ids), and the owning tables unlock the delete path's
	// death recording. Rolled-back statements have their tags dropped by
	// rollbackPkMoveStmtStateLocked.
	if txn.pkMoveLinkTables == nil {
		txn.pkMoveLinkTables = make(map[pkMoveAcctTbl]struct{})
	}
	var idVec *vector.Vector
	if 0 <= linkIDIdx && linkIDIdx < len(bat.Vecs) {
		idVec = bat.Vecs[linkIDIdx]
	}
	keptRows := bat.RowCount()
	tableIDs = vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[tableIdx])
	for r := 0; r < keptRows && r < len(tableIDs); r++ {
		txn.pkMoveLinkTables[pkMoveAcctTbl{account: accountId, table: tableIDs[r]}] = struct{}{}
		if idVec == nil || idVec.IsNull(uint64(r)) {
			continue
		}
		if txn.pkMoveLinkBirth == nil {
			txn.pkMoveLinkBirth = make(map[string]int)
		}
		txn.pkMoveLinkBirth[string(idVec.GetBytesAt(r))] = txn.statementID
	}

	// Pass 5: binding retirement. A staged DEPARTURE (old pk bytes !=
	// new pk bytes) moves its lineage off the key: consume the binding so
	// a later generation re-arriving at the key (from a vacate/insert)
	// can never be healed onto the departed lineage.
	if len(txn.pkMoveKeyLineage) > 0 {
		for r := 0; r < keptRows && r < len(tableIDs); r++ {
			if oldPKVec.IsNull(uint64(r)) || newPKVec.IsNull(uint64(r)) {
				continue
			}
			if bytes.Equal(oldPKVec.GetBytesAt(r), newPKVec.GetBytesAt(r)) {
				continue
			}
			txn.consumePkMoveKeyLineageLocked(
				pkMoveAcctTbl{account: accountId, table: tableIDs[r]},
				string(oldPKVec.GetBytesAt(r)))
		}
	}
	// Refuse to grow retained provenance past the coordinator budget,
	// statement-atomically, before the caller stages the batch — but only
	// once durable provenance is at stake. A fill whose rows were all
	// consumed continuity markers, inside a transaction that has staged
	// no link rows so far, leaves only self-cancelling evidence behind:
	// the marker family exists solely so a LATER statement's move of
	// those rows can name their generation, and if no move ever comes the
	// bookkeeping dies with the transaction and commits nothing. Charging
	// it rejects ordinary bulk non-key UPDATEs outright — they emit one
	// marker family per touched row (over 112 measured bytes per distinct
	// row), so a few hundred thousand rows cross the default budget with
	// no branch, no move, and no durable provenance anywhere (r22 review
	// F3). Durable growth stays enforced: a fill that produced real link
	// rows checks here, a transaction already holding staged links checks
	// on every later fill, and the first real link batch's admission
	// re-checks with the full measure — an update-then-move transaction
	// still stops at the move, never committing unbounded provenance.
	if keptRows == 0 && !txn.pkMoveHasLiveStagedLinksLocked() {
		return nil
	}
	return txn.checkPkMoveBookkeepBudgetLocked()
}

// pkMoveHasLiveStagedLinksLocked reports whether the workspace holds any
// staged link-table rows. It is THE participation predicate shared by
// every provenance budget boundary (fill-end, diverted-delete wave,
// swallowed-delete wave, pre-dump): bookkeeping durable provenance
// already depends on is charged, while consumed continuity evidence that
// dies with the transaction — marker families, lineage bindings, origin
// and heal maps recorded before any link exists — is not. The r23
// review's F3: a transaction holding no staged link rows commits no
// provenance, so its delete waves and spill retention must never be
// refused; any later statement that stages a real link re-measures the
// full retained state at its own admission/fill/dump boundary, keeping
// genuine growth bounded from the first real link on.
func (txn *Transaction) pkMoveHasLiveStagedLinksLocked() bool {
	for i := range txn.writes {
		we := &txn.writes[i]
		if we.typ == INSERT && we.bat != nil && we.bat.RowCount() > 0 &&
			isPkMoveLinkTableInsert(we.databaseName, we.tableName) {
			return true
		}
	}
	return false
}

// pkMoveNoteRetargetLocked records a link row's pre-retarget new_row_id
// once per statement (first mutation wins), so RollbackLastStatement can
// restore the value even though the mutated batch belongs to an earlier
// statement the writes-slice truncation does not touch.
func (txn *Transaction) pkMoveNoteRetargetLocked(bat *batch.Batch, idIdx, row int, prev types.Rowid) {
	if idIdx < 0 || idIdx >= len(bat.Vecs) {
		return
	}
	idVec := bat.Vecs[idIdx]
	if idVec.IsNull(uint64(row)) {
		return
	}
	linkID := idVec.GetBytesAt(row)
	if txn.pkMoveRetargetUndoSeen == nil {
		txn.pkMoveRetargetUndoSeen = make(map[string]struct{})
	}
	key := string(append([]byte(nil), linkID...))
	if _, dup := txn.pkMoveRetargetUndoSeen[key]; dup {
		return
	}
	txn.pkMoveRetargetUndoSeen[key] = struct{}{}
	txn.pkMoveRetargetUndo = append(txn.pkMoveRetargetUndo, pkMoveRetargetUndoRec{
		linkID: key,
		prev:   prev,
	})
}

// pkMoveNoteHealsLocked records the original pkMoveNullHeals entry about
// to be created (created=true; previous content ignored) or overwritten
// (prev and prevStmt carry the old binding), once per statement per link.
func (txn *Transaction) pkMoveNoteHealsLocked(linkID string, prev types.Rowid, prevStmt int, created bool) {
	if txn.pkMoveRetargetUndoSeen == nil {
		txn.pkMoveRetargetUndoSeen = make(map[string]struct{})
	}
	if _, dup := txn.pkMoveRetargetUndoSeen[linkID]; dup {
		return
	}
	txn.pkMoveRetargetUndoSeen[linkID] = struct{}{}
	txn.pkMoveRetargetUndo = append(txn.pkMoveRetargetUndo, pkMoveRetargetUndoRec{
		linkID:   linkID,
		prev:     prev,
		prevStmt: prevStmt,
		heal:     true,
		created:  created,
	})
}

// pkMoveLinkCellRef points at one staged link row inside the writes
// slice; the fill's continuity index records these once so per-marker
// work never rescans the whole staged set.
type pkMoveLinkCellRef struct {
	entry int
	row   int
}

// pkMoveNullLinkEndpoint keys the NULL-identity side of the continuity
// index: the (account, table, new pk) a wounded link could continue
// through.
type pkMoveNullLinkEndpoint struct {
	acct  pkMoveAcctTbl
	newPK string
}

// pkMoveLinkEntryLayout caches a staged link entry's column positions so
// indexed lookups pay no per-row attribute scan.
type pkMoveLinkEntryLayout struct {
	tableIdx  int
	newRowIdx int
	newPKIdx  int
	idIdx     int
}

// pkMoveContinuityIndex maps the staged link references one fill pass can
// touch: non-NULL new_row cells by the identity they name (retarget
// targets), NULL-identity rows by their (account, table, new pk) endpoint
// (continuation targets), and heal bindings by lineage identity. It is
// built lazily on the first RESOLVED marker of a fill and discarded with
// it — pure per-statement scope, so rollback never has to undo it. The
// per-marker full scans it replaces made an "update N primary keys, then
// edit those N payloads" transaction quadratic under txn.Lock.
type pkMoveContinuityIndex struct {
	byNewRow   map[types.Rowid][]pkMoveLinkCellRef
	byNullPK   map[pkMoveNullLinkEndpoint][]pkMoveLinkCellRef
	healsByRID map[types.Rowid][]string
	layouts    []*pkMoveLinkEntryLayout // parallel to txn.writes; nil for non-link entries
}

// pkMoveLinkEntryLayoutLocked returns the cached column layout of a
// staged link entry, or nil when the entry is not a well-formed link
// insert (wrong table, type, batch shape, or missing/odd columns).
func (txn *Transaction) pkMoveLinkEntryLayoutLocked(we *Entry) *pkMoveLinkEntryLayout {
	if !isPkMoveLinkTableInsert(we.databaseName, we.tableName) ||
		we.typ != INSERT || we.bat == nil || we.bat.RowCount() == 0 {
		return nil
	}
	l := &pkMoveLinkEntryLayout{tableIdx: -1, newRowIdx: -1, newPKIdx: -1, idIdx: -1}
	for j, attr := range we.bat.Attrs {
		switch attr {
		case catalog.DataBranchUpdateLinkColTableID:
			l.tableIdx = j
		case catalog.DataBranchUpdateLinkColNewRow:
			l.newRowIdx = j
		case catalog.DataBranchUpdateLinkColNewPK:
			l.newPKIdx = j
		case catalog.DataBranchUpdateLinkColLinkID:
			l.idIdx = j
		}
	}
	if l.tableIdx < 0 || l.newRowIdx < 0 || l.newPKIdx < 0 || l.idIdx < 0 ||
		l.tableIdx >= len(we.bat.Vecs) || l.newRowIdx >= len(we.bat.Vecs) ||
		l.newPKIdx >= len(we.bat.Vecs) || l.idIdx >= len(we.bat.Vecs) ||
		we.bat.Vecs[l.tableIdx].GetType().Oid != types.T_uint64 {
		return nil
	}
	rowCount := we.bat.RowCount()
	if len(vector.MustFixedColNoTypeCheck[uint64](we.bat.Vecs[l.tableIdx])) < rowCount {
		return nil
	}
	return l
}

// buildPkMoveContinuityIndexLocked scans the staged link rows exactly
// once and indexes the references the fill's Pass 2 can claim.
func (txn *Transaction) buildPkMoveContinuityIndexLocked() *pkMoveContinuityIndex {
	idx := &pkMoveContinuityIndex{
		byNewRow:   make(map[types.Rowid][]pkMoveLinkCellRef),
		byNullPK:   make(map[pkMoveNullLinkEndpoint][]pkMoveLinkCellRef),
		healsByRID: make(map[types.Rowid][]string),
		layouts:    make([]*pkMoveLinkEntryLayout, len(txn.writes)),
	}
	for i := range txn.writes {
		we := &txn.writes[i]
		l := txn.pkMoveLinkEntryLayoutLocked(we)
		if l == nil {
			continue
		}
		idx.layouts[i] = l
		tableIDs := vector.MustFixedColNoTypeCheck[uint64](we.bat.Vecs[l.tableIdx])
		newRowVec := we.bat.Vecs[l.newRowIdx]
		newPKVec := we.bat.Vecs[l.newPKIdx]
		idVec := we.bat.Vecs[l.idIdx]
		for r := 0; r < we.bat.RowCount(); r++ {
			if !newRowVec.IsNull(uint64(r)) {
				raw := newRowVec.GetBytesAt(r)
				if len(raw) == types.RowidSize {
					idx.byNewRow[types.DecodeFixed[types.Rowid](raw)] = append(
						idx.byNewRow[types.DecodeFixed[types.Rowid](raw)],
						pkMoveLinkCellRef{entry: i, row: r})
				}
				continue
			}
			if newPKVec.IsNull(uint64(r)) || idVec.IsNull(uint64(r)) {
				continue
			}
			endpoint := pkMoveNullLinkEndpoint{
				acct:  pkMoveAcctTbl{account: we.accountId, table: tableIDs[r]},
				newPK: string(newPKVec.GetBytesAt(r)),
			}
			idx.byNullPK[endpoint] = append(idx.byNullPK[endpoint], pkMoveLinkCellRef{entry: i, row: r})
		}
	}
	for id, h := range txn.pkMoveNullHeals {
		idx.healsByRID[h.rid] = append(idx.healsByRID[h.rid], id)
	}
	return idx
}

// continuePkMoveNullLinksLocked binds staged NULL-identity link rows whose
// new key a just-resolved continuity marker consumed onward: after an
// S3-route move, a workspace-route same-key edit stages a real replacement
// the link can track. Binding is refused when the death ledger shows the
// key died AFTER the link's birth in a statement other than this marker's
// — that death proves the marker continues a post-mortem reincarnation,
// not the move's lineage. The binding is bookkeeping only; the committed
// link image keeps its NULL cell. The endpoint index lookup replaces the
// pre-F5 full writes-slice scan; the per-row acceptance conditions are
// unchanged.
func (txn *Transaction) continuePkMoveNullLinksLocked(
	acct pkMoveAcctTbl, pk []byte, fresh types.Rowid, idx *pkMoveContinuityIndex,
) {
	// No staged surviving link rows means no bind targets: the
	// pkMoveLinkBirth population is the exact proxy (pass 4 records one
	// birth per surviving staged link row). Skipping even the index
	// lookup is what keeps bulk pk-unchanged updates linear per marker.
	if len(txn.pkMoveLinkBirth) == 0 || idx == nil {
		return
	}
	endpoint := pkMoveNullLinkEndpoint{acct: acct, newPK: string(pk)}
	refs := idx.byNullPK[endpoint]
	if len(refs) == 0 {
		return
	}
	death, hasDeath := 0, false
	if set := txn.pkMoveDeadPks[acct]; set != nil {
		death, hasDeath = pkMoveLatestStmt(set[string(pk)])
	}
	for _, ref := range refs {
		we := &txn.writes[ref.entry]
		l := idx.layouts[ref.entry]
		if l == nil || ref.row >= we.bat.RowCount() {
			continue
		}
		idVec := we.bat.Vecs[l.idIdx]
		linkID := string(idVec.GetBytesAt(ref.row))
		if _, ok := txn.pkMoveNullHeals[linkID]; ok {
			continue
		}
		birth := txn.statementID
		if b, ok := txn.pkMoveLinkBirth[linkID]; ok {
			birth = b
		}
		if hasDeath && death > birth && death != txn.statementID {
			continue
		}
		txn.pkMoveNoteHealsLocked(linkID, types.Rowid{}, 0, true)
		if txn.pkMoveNullHeals == nil {
			txn.pkMoveNullHeals = make(map[string]pkMoveNullHeal)
		}
		txn.pkMoveNullHeals[linkID] = pkMoveNullHeal{rid: fresh, stmt: txn.statementID}
	}
	// Every staged NULL link at this endpoint was considered once: the
	// endpoint cannot gain new members within this fill, so drop it.
	delete(idx.byNullPK, endpoint)
}

// pkMoveNoteOriginWriteLocked records the current pkMoveRowidOrigins[key]
// state once per statement, so a rolled-back statement's origin writes can
// be undone without disturbing earlier statements' mappings.
func (txn *Transaction) pkMoveNoteOriginWriteLocked(key types.Rowid) {
	if txn.pkMoveOriginUndoSeen == nil {
		txn.pkMoveOriginUndoSeen = make(map[types.Rowid]struct{})
	}
	if _, dup := txn.pkMoveOriginUndoSeen[key]; dup {
		return
	}
	prev, existed := txn.pkMoveRowidOrigins[key]
	txn.pkMoveOriginUndoSeen[key] = struct{}{}
	txn.pkMoveOriginUndo = append(txn.pkMoveOriginUndo, pkMoveOriginUndoRec{
		key:     key,
		prev:    prev,
		existed: existed,
	})
}

// retargetPkMoveLinkNewRowIDsLocked rewrites the new_row_id column of every
// ALREADY-STAGED link batch (previous statements, and earlier batches of
// the current statement): link rows whose recorded replacement is fromID
// now name toID. Called when a same-key edit replaced the recorded
// replacement row. The references come from the fill's continuity index
// (built once per staging statement), so N resolved markers cost
// O(links + markers) under txn.Lock instead of N full staged-set scans;
// with NONE staged at all (the pkMoveLinkBirth population is empty
// exactly then) the lookup provably finds nothing, which is what keeps
// the per-marker call of a bulk non-key update cheap.
//
// A rewrite that faults midway returns the failure so the staging
// statement aborts: the undo records taken before each mutation let the
// statement rollback restore the cells already retargeted, and committing
// a half-retargeted link set is never an option. Workspace memory
// accounting is re-balanced around the mutated entry on every path.
func (txn *Transaction) retargetPkMoveLinkNewRowIDsLocked(fromID, toID types.Rowid, idx *pkMoveContinuityIndex) error {
	if len(txn.pkMoveLinkBirth) == 0 || idx == nil {
		return nil
	}
	refs := idx.byNewRow[fromID]
	// The refs are staged ascending by entry: process each entry's slice
	// as one accounted mutation, exactly like the pre-index full scan
	// wrapped one entry's row loop.
	for start := 0; start < len(refs); {
		e := refs[start].entry
		end := start
		for end < len(refs) && refs[end].entry == e {
			end++
		}
		we := &txn.writes[e]
		l := idx.layouts[e]
		if l == nil {
			start = end
			continue
		}
		vec := we.bat.Vecs[l.newRowIdx]
		// The row-id columns are binary(24) varlenas replaced in place
		// (r40 review F2, D148): the overwrite leaves len(v.area), every
		// descriptor, and the measured size untouched, so a round trip
		// returns the batch's footprint to its pre-round bytes exactly.
		// The accounting wrap stays so the mutation never runs outside
		// the workspace accounting helpers — delTransaction trips the
		// "changed outside accounting helpers" BUG guard otherwise — and
		// observes zero net delta across the loop.
		accounted := we.accountedSize != 0
		if accounted {
			txn.unaccountWorkspaceEntryLocked(we)
		}
		var writeErr error
		for _, ref := range refs[start:end] {
			if ref.row >= we.bat.RowCount() {
				continue
			}
			// Undo before mutation: the rollback path restores this
			// statement's retargets by link id. Only the FIRST mutation
			// of a row within one statement records (the original value).
			txn.pkMoveNoteRetargetLocked(we.bat, l.idIdx, ref.row, fromID)
			if err := pkMoveVecOverwriteRowidAt(vec, ref.row, toID[:], txn.proc.Mp()); err != nil {
				writeErr = err
				break
			}
		}
		if accounted {
			txn.accountWorkspaceEntryLocked(we)
		}
		if writeErr != nil {
			return writeErr
		}
		start = end
	}
	// The rewritten cells now name toID: re-key the references so a LATER
	// marker of this same fill whose pre-image is toID (a same-txn
	// continuity chain) finds them, exactly as the pre-index rescan of the
	// live cell values did.
	delete(idx.byNewRow, fromID)
	idx.byNewRow[toID] = append(idx.byNewRow[toID], refs...)
	// Heal bindings carry lineage identities the batch columns cannot (the
	// link cell stays NULL); a retarget must move them too, or a healed
	// lineage's second same-key edit strands the binding on the replaced
	// generation.
	movedHeals := idx.healsByRID[fromID]
	for _, id := range movedHeals {
		h, ok := txn.pkMoveNullHeals[id]
		if !ok {
			continue
		}
		txn.pkMoveNoteHealsLocked(id, h.rid, h.stmt, false)
		h.rid = toID
		txn.pkMoveNullHeals[id] = h
	}
	if len(movedHeals) > 0 {
		delete(idx.healsByRID, fromID)
		idx.healsByRID[toID] = append(idx.healsByRID[toID], movedHeals...)
	}
	return nil
}

// retargetPkMoveLinkNewRowIDsToNullLocked continues staged link rows
// whose new_row_id names preID across a workspace-to-S3 route switch: an
// unresolved continuity marker just proved the generation preID moved
// onto the S3 buffering route, whose committed replacement carries no
// staged workspace identity. Every staged link still naming preID
// switches to the S3 NULL identity — the committed image is the
// single-shot claim's territory exactly as for any S3-route move — and
// heal bindings anchored on preID are removed (the NULL-identity death
// gate knows this shape). Without the switch the commit prune reads
// preID's consumption as a murder and drops the lineage's provenance,
// reverting the apply to DELETE+INSERT. The targets ride the fill's
// continuity index (built once per staging statement), so M route
// switches over L staged links cost O(L + M) under txn.Lock instead of
// M full staged-set scans; the freshly nulled rows re-key into the
// index's NULL-identity side so later resolved markers of the same fill
// can bind them. Every mutation rides the statement undo log exactly
// like the rowid retarget: a rollback of the editing statement restores
// both the cells and the heal bindings.
func (txn *Transaction) retargetPkMoveLinkNewRowIDsToNullLocked(
	preID types.Rowid, idx *pkMoveContinuityIndex,
) error {
	if len(txn.pkMoveLinkBirth) == 0 || idx == nil {
		return nil
	}
	refs := idx.byNewRow[preID]
	continued := make([]pkMoveLinkCellRef, 0, len(refs))
	// The refs are staged ascending by entry: process each entry's slice
	// as one accounted mutation, exactly like the rowid retarget wraps
	// one entry's row loop.
	for start := 0; start < len(refs); {
		e := refs[start].entry
		end := start
		for end < len(refs) && refs[end].entry == e {
			end++
		}
		we := &txn.writes[e]
		l := idx.layouts[e]
		if l == nil {
			start = end
			continue
		}
		newRowVec := we.bat.Vecs[l.newRowIdx]
		// Same accounting discipline as the rowid retarget: mutating a
		// staged batch outside the accounting helpers trips the
		// delTransaction guard.
		accounted := we.accountedSize != 0
		if accounted {
			txn.unaccountWorkspaceEntryLocked(we)
		}
		for _, ref := range refs[start:end] {
			if ref.row >= we.bat.RowCount() || newRowVec.IsNull(uint64(ref.row)) {
				continue
			}
			// Undo before mutation: the rollback path restores this
			// statement's retargets by link id. Only the FIRST mutation
			// of a row within one statement records (the original value).
			txn.pkMoveNoteRetargetLocked(we.bat, l.idIdx, ref.row, preID)
			nulls.Add(newRowVec.GetNulls(), uint64(ref.row))
			continued = append(continued, ref)
		}
		if accounted {
			txn.accountWorkspaceEntryLocked(we)
		}
		start = end
	}
	// The nulled rows are NULL-identity rows now: move their references
	// across to the endpoint side of the index so a LATER resolved
	// marker of this same fill binds them through
	// continuePkMoveNullLinksLocked, exactly as the build-time scan
	// indexed rows that arrived NULL.
	delete(idx.byNewRow, preID)
	for _, ref := range continued {
		we := &txn.writes[ref.entry]
		l := idx.layouts[ref.entry]
		idVec := we.bat.Vecs[l.idIdx]
		newPKVec := we.bat.Vecs[l.newPKIdx]
		if idVec.IsNull(uint64(ref.row)) || newPKVec.IsNull(uint64(ref.row)) {
			continue
		}
		tableIDs := vector.MustFixedColNoTypeCheck[uint64](we.bat.Vecs[l.tableIdx])
		if ref.row >= len(tableIDs) {
			continue
		}
		endpoint := pkMoveNullLinkEndpoint{
			acct:  pkMoveAcctTbl{account: we.accountId, table: tableIDs[ref.row]},
			newPK: string(newPKVec.GetBytesAt(ref.row)),
		}
		idx.byNullPK[endpoint] = append(idx.byNullPK[endpoint], ref)
	}
	// Heal bindings anchored on preID lose their survival evidence with
	// the generation: the link row they decorate is already NULL-identity,
	// so removing the binding returns it to the plain diverted-death
	// gate. The undo record recreates the binding on rollback.
	movedHeals := idx.healsByRID[preID]
	for _, id := range movedHeals {
		h, ok := txn.pkMoveNullHeals[id]
		if !ok {
			continue
		}
		txn.pkMoveNoteHealsLocked(id, h.rid, h.stmt, false)
		delete(txn.pkMoveNullHeals, id)
	}
	if len(movedHeals) > 0 {
		delete(idx.healsByRID, preID)
	}
	return nil
}

// pkMoveLineageBind is one statement-tagged lineage binding for a key.
// origin is the durable generation the row currently at the key descends
// from; wm is the txn.writes length at bind time (binding-validity scans
// for intervening deletes start there); stmt is the binding statement;
// consumed marks that a recorded departure already moved the lineage off
// the key.
type pkMoveLineageBind struct {
	origin   types.Rowid
	wm       int
	stmt     int
	consumed bool
}

// pkMoveRetireDeath is one diverted delete that can retire a by-key
// lineage binding: stmt tags it for statement rollback, and rid names
// the murdered generation so the retirement evaluation can tell it apart
// from a RECORDED departure (a staged link or continuity marker that
// names the same rowid as its old side consumed exactly that generation;
// the lineage then left the key through provenance, not through an
// unrecorded delete).
type pkMoveRetireDeath struct {
	rid  types.Rowid
	stmt int
}

// notePkMoveRetireDeath appends one diverted death to the ledger. Deaths
// of one key arrive in statement order, so the list keeps the
// append-ascending discipline RollbackLastStatement's tail cut relies on.
func notePkMoveRetireDeath(
	m map[pkMoveAcctTbl]map[string][]pkMoveRetireDeath,
	key pkMoveAcctTbl, pk string, rid types.Rowid, stmt int,
) map[pkMoveAcctTbl]map[string][]pkMoveRetireDeath {
	if m == nil {
		m = make(map[pkMoveAcctTbl]map[string][]pkMoveRetireDeath)
	}
	set := m[key]
	if set == nil {
		set = make(map[string][]pkMoveRetireDeath)
		m[key] = set
	}
	set[pk] = append(set[pk], pkMoveRetireDeath{rid: rid, stmt: stmt})
	return m
}

// pkMoveBindingRetiredLocked reports whether an unconsumed diverted death
// retired the lineage binding bound at bindStmt for (acctTbl, pk): a
// delete of the key's live generation in a statement at or after the bind
// that NO staged link or continuity marker explains. Explained deaths —
// the diverted rowid is some staged row's recorded old side — are the
// edit's own predecessor or the move's own pre-image going away through
// provenance, and the lineage legitimately continues there. An
// unexplained death murdered the lineage's terminal generation without
// recorded provenance, so nothing that re-arrives at the key afterwards
// may inherit the dead lineage.
func (txn *Transaction) pkMoveBindingRetiredLocked(acctTbl pkMoveAcctTbl, pk string, bindStmt int) bool {
	for _, d := range txn.pkMoveRetireDeaths[acctTbl][pk] {
		if d.stmt < bindStmt {
			continue
		}
		if _, explained := txn.pkMoveStagedOldRids[d.rid]; explained {
			continue
		}
		return true
	}
	return false
}

// pkMoveDeleteScanEntry is the incremental DELETE-side fold of
// txn.writes for one (account, table): encoded pk -> the LATEST absolute
// write index whose delete named that key. gen pins the fold to the
// workspace compaction epoch; scanned is the absolute watermark up to
// which entries were folded.
type pkMoveDeleteScanEntry struct {
	gen     uint64
	scanned int
	dels    map[string]int
}

// bindPkMoveKeyLineageLocked records that the row currently at key
// (acctTbl, pk) descends from the durable lineage of the marker
// pre-image at (oldRowVec, row). Only S3-route (unresolved) markers call
// this: their replacement never stages, so the exact origin map cannot
// carry the lineage and the by-key binding is the only evidence a later
// statement's move can heal its old side through. A workspace-stage
// pre-image composes the lineage's already-recorded evidence — a valid
// binding, else the exact origin map entry the earlier edit left — and
// stays unbound when there is none (an independently inserted row), the
// same distinctness rule Pass 2 applies to resolved markers.
func (txn *Transaction) bindPkMoveKeyLineageLocked(
	acctTbl pkMoveAcctTbl, pk string, oldRowVec *vector.Vector, row int,
) {
	if oldRowVec == nil || oldRowVec.IsNull(uint64(row)) {
		return
	}
	raw := oldRowVec.GetBytesAt(row)
	if len(raw) != types.RowidSize {
		return
	}
	pre := types.DecodeFixed[types.Rowid](raw)
	if pkMoveIsWorkspaceStageRowid(&pre) {
		// An edit-first lineage switched to the S3 route: this workspace
		// pre-image was ITSELF the staged replacement of an earlier
		// same-key edit, so the lineage's durable origin lives in the
		// existing binding evidence or the exact origin map, never in
		// the uncommitted identity. Compose that evidence BEFORE
		// excluding the generation: without the composition a later
		// statement's move of the key re-anchors on the uncommitted
		// identity, the committed stream shows no such departure, and
		// the apply replays DELETE+INSERT where a native UPDATE would
		// have cascaded. A workspace generation with no valid binding
		// and no recorded origin is an independently inserted row: it
		// anchors literally to its own netted stream insert and must
		// stay unbound.
		origin, ok := txn.lookupPkMoveKeyLineageLocked(acctTbl, pk)
		if !ok {
			origin, ok = txn.pkMoveRowidOrigins[pre]
		}
		if !ok {
			return
		}
		txn.appendPkMoveKeyLineageLocked(acctTbl, pk, origin)
		return
	}
	// A live valid binding for this key already names the lineage's
	// durable origin; the same-key edit continues that lineage, so the
	// recorded origin stays. Without one, compose through the exact map
	// (an earlier healed lineage), else take the durable pre-image
	// literally.
	origin := pre
	if current, ok := txn.lookupPkMoveKeyLineageLocked(acctTbl, pk); ok {
		origin = current
	} else if o, ok := txn.pkMoveRowidOrigins[pre]; ok {
		origin = o
	}
	txn.appendPkMoveKeyLineageLocked(acctTbl, pk, origin)
}

// appendPkMoveKeyLineageLocked pushes one binding for (acctTbl, pk),
// replacing the top of stack when the same statement re-binds the key
// (a statement contributes at most one binding per key).
func (txn *Transaction) appendPkMoveKeyLineageLocked(
	acctTbl pkMoveAcctTbl, pk string, origin types.Rowid,
) {
	set := txn.pkMoveKeyLineage[acctTbl]
	if set == nil {
		set = make(map[string][]pkMoveLineageBind)
		if txn.pkMoveKeyLineage == nil {
			txn.pkMoveKeyLineage = make(map[pkMoveAcctTbl]map[string][]pkMoveLineageBind)
		}
		txn.pkMoveKeyLineage[acctTbl] = set
	}
	stack := set[pk]
	bind := pkMoveLineageBind{origin: origin, wm: len(txn.writes), stmt: txn.statementID}
	if n := len(stack); n > 0 && stack[n-1].stmt == txn.statementID {
		stack[n-1] = bind
	} else {
		stack = append(stack, bind)
	}
	set[pk] = stack
}

// lookupPkMoveKeyLineageLocked returns the durable origin a VALID,
// unconsumed lineage binding names for (acctTbl, pk), if any. A binding
// is valid while no staged DELETE of the same key outdates its bind
// watermark: the bound generation left the key without a recorded move
// when one does, and later generations at the key must not inherit the
// dead lineage. Delete-scan failures poison the lookup (no heal), never
// the statement: the unhealed link then keeps literal identities, which
// the consumer's evidence rules handle fail-closed.
func (txn *Transaction) lookupPkMoveKeyLineageLocked(
	acctTbl pkMoveAcctTbl, pk string,
) (types.Rowid, bool) {
	set := txn.pkMoveKeyLineage[acctTbl]
	stack := set[pk]
	if len(stack) == 0 {
		return types.Rowid{}, false
	}
	top := stack[len(stack)-1]
	// A consumed binding governs only its own statement's departures.
	if top.consumed && top.stmt != txn.statementID {
		return types.Rowid{}, false
	}
	dels, ok := txn.pkMoveDeleteScanLocked(acctTbl)
	if !ok {
		return types.Rowid{}, false
	}
	if idx, dead := dels[pk]; dead && idx >= top.wm {
		return types.Rowid{}, false
	}
	// A diverted delete no recorded departure explains murdered the
	// bound generation without ever reaching the staged-delete scan
	// above: the lineage died, and this binding must not carry it onto
	// whatever row re-arrived at the key afterwards.
	if txn.pkMoveBindingRetiredLocked(acctTbl, pk, top.stmt) {
		return types.Rowid{}, false
	}
	return top.origin, true
}

// healPkMoveLinkOldRowLocked rewrites the recorded old identity of one
// move row to its lineage's durable origin when a valid binding governs
// (acctTbl, pk). The committed stream shows the ORIGIN generation
// departing: healing here is what makes the consumer's tombstone screen
// see the lineage's own departure again, so a same-key foreign departure
// is no longer adoptable as the fold's continuation. Identities already
// literal to the origin need no cell write (they still count as a use).
//
// The first return reports a RETIRED healing target: a visible lineage
// binding governs the key, but the diverted-delete boundary proved the
// bound generation dead with no recorded departure (some later statement
// deleted it and no staged link or marker explains that death). The row
// this link moved is then provably NOT a continuation of the bound
// lineage, so the fill kills the link instead of letting the apply fold
// the unrelated row onto the dead lineage's family.
func (txn *Transaction) healPkMoveLinkOldRowLocked(
	acctTbl pkMoveAcctTbl, pk string, oldRowVec *vector.Vector, row int,
) (bool, error) {
	raw := oldRowVec.GetBytesAt(row)
	rid := types.DecodeFixed[types.Rowid](raw)
	if pkMoveIsWorkspaceStageRowid(&rid) {
		// Workspace-stage generations anchor literally; their lineage
		// rides the exact origin map, never a by-key binding.
		return false, nil
	}
	origin, ok := txn.lookupPkMoveKeyLineageLocked(acctTbl, pk)
	if !ok {
		if stack := txn.pkMoveKeyLineage[acctTbl][pk]; len(stack) > 0 {
			top := stack[len(stack)-1]
			visible := !(top.consumed && top.stmt != txn.statementID)
			if visible && txn.pkMoveBindingRetiredLocked(acctTbl, pk, top.stmt) {
				return true, nil
			}
		}
		return false, nil
	}
	if origin == rid {
		return false, nil
	}
	return false, pkMoveVecSetBytesAt(oldRowVec, row, origin[:], txn.proc.Mp())
}

// consumePkMoveKeyLineageLocked marks the binding governing (acctTbl,
// pk) as consumed by a recorded departure of this statement: the move
// healed through it (or named its origin literally) and the lineage left
// the key, so later generations re-arriving at the key must not inherit
// the departed lineage. The consumption rides the same statement-tagged
// stack, rolling back with its statement.
func (txn *Transaction) consumePkMoveKeyLineageLocked(acctTbl pkMoveAcctTbl, pk string) {
	set := txn.pkMoveKeyLineage[acctTbl]
	stack := set[pk]
	if len(stack) == 0 {
		return
	}
	top := stack[len(stack)-1]
	if top.consumed {
		return
	}
	if top.stmt == txn.statementID {
		top.consumed = true
		stack[len(stack)-1] = top
		set[pk] = stack
		return
	}
	set[pk] = append(stack, pkMoveLineageBind{
		origin:   top.origin,
		wm:       len(txn.writes),
		stmt:     txn.statementID,
		consumed: true,
	})
}

// pkMoveDeleteScanLocked lazily folds staged DELETE rows of
// (account, table) into encoded-pk -> latest absolute write index, with
// the harvest cache's invalidation discipline. The second return is
// false when a delete row's pk could not be encoded: binding lookups
// then refuse every heal (fail closed) for the statement.
func (txn *Transaction) pkMoveDeleteScanLocked(acctTbl pkMoveAcctTbl) (map[string]int, bool) {
	ce := txn.pkMoveDeleteScan[acctTbl]
	if ce != nil && (ce.gen != txn.pkMoveHarvestGen || ce.scanned > len(txn.writes)) {
		delete(txn.pkMoveDeleteScan, acctTbl)
		ce = nil
	}
	if ce == nil {
		ce = &pkMoveDeleteScanEntry{gen: txn.pkMoveHarvestGen}
		if txn.pkMoveDeleteScan == nil {
			txn.pkMoveDeleteScan = make(map[pkMoveAcctTbl]*pkMoveDeleteScanEntry)
		}
		txn.pkMoveDeleteScan[acctTbl] = ce
	}
	dels, ok := pkMoveFoldDeleteEntries(txn.writes[ce.scanned:], acctTbl.account, acctTbl.table, ce.scanned, ce.dels)
	if !ok {
		delete(txn.pkMoveDeleteScan, acctTbl)
		return nil, false
	}
	ce.dels = dels
	ce.scanned = len(txn.writes)
	return ce.dels, true
}

// pkMoveFoldDeleteEntries folds the DELETE entries of a writes suffix
// into out (allocated on first hit), mapping encoded pk -> the latest
// absolute write index whose delete carried that pk. base is the
// absolute index of writes[0]. The encoded pk comes from the delete
// batch's pk column (its pkCheckPos when known, else the "[rowid, pk]"
// delete layout). Returns ok=false when a delete row cannot be encoded;
// callers fail closed, mirroring the harvest's poison posture.
func pkMoveFoldDeleteEntries(
	writes []Entry, accountId uint32, tableId uint64, base int, out map[string]int,
) (map[string]int, bool) {
	packer := types.NewPacker()
	defer packer.Close()
	for i := range writes {
		we := &writes[i]
		if we.typ != DELETE || we.accountId != accountId || we.tableId != tableId {
			continue
		}
		if we.bat == nil || len(we.bat.Vecs) == 0 {
			continue
		}
		pos := -1
		if we.pkCheckPos > 0 && we.pkCheckPos < len(we.bat.Vecs) {
			pos = we.pkCheckPos
		} else if we.pkMovePkPos > 0 && we.pkMovePkPos < len(we.bat.Vecs) {
			// composite keys resolve here (r26 review F2)
			pos = we.pkMovePkPos
		} else if len(we.bat.Attrs) > 1 && we.bat.Attrs[1] == "pk" && len(we.bat.Vecs) > 1 {
			pos = 1
		}
		if pos < 0 {
			continue
		}
		pkVec := we.bat.Vecs[pos]
		n := min(we.bat.RowCount(), pkVec.Length())
		for r := 0; r < n; r++ {
			if pkVec.IsNull(uint64(r)) {
				continue
			}
			packer.Reset()
			if err := vector.EncodeRowToPacker(packer, pkVec, r); err != nil {
				return nil, false
			}
			if out == nil {
				out = make(map[string]int)
			}
			key := string(packer.Bytes())
			if idx, ok := out[key]; !ok || base+i > idx {
				out[key] = base + i
			}
		}
	}
	return out, true
}

// pkMoveHarvestNewRowIDs maps encoded pk bytes to the row id a staged insert
// assigned, over all workspace entries for (accountId, tableId). Only INSERT
// entries whose row id vector is already prepended (Vecs[0] is T_Rowid) and
// whose pk column position is known carry identities; everything else is
// skipped. Iteration order is staging order, so when several live entries
// hold the same pk value (same-key re-insert in a later statement) the
// freshest one wins. Returns nil when no usable entry exists.
//
// This is a pure function over the writes slice so unit tests can drive it
// without bootstrapping a transaction.
func pkMoveHarvestNewRowIDs(writes []Entry, accountId uint32, tableId uint64) map[string]types.Rowid {
	out, _ := pkMoveFoldHarvestEntries(writes, accountId, tableId, nil, nil)
	return out
}

// pkMoveFoldHarvestEntries folds one writes slice into out (allocated on
// first write when nil), staging order winning for repeated pks. The second
// return is false when a pk vector failed to encode: provenance for the
// table is then unusable and callers fail closed.
//
// excludedOf reports the batchSelectList rows of one staged batch: those
// rows are logically deleted (an update's pre-image the commit will
// shrink out) and must never resolve a replacement — harvesting a
// stale generation re-anchors provenance on a row the committed stream
// does not carry, and the commit prune then drops the genuine lineage.
func pkMoveFoldHarvestEntries(
	writes []Entry, accountId uint32, tableId uint64, out map[string]types.Rowid,
	excludedOf func(*batch.Batch) []int64,
) (map[string]types.Rowid, bool) {
	packer := types.NewPacker()
	defer packer.Close()
	for i := range writes {
		we := &writes[i]
		if we.typ != INSERT || we.accountId != accountId || we.tableId != tableId {
			continue
		}
		bat := we.bat
		if bat == nil || len(bat.Vecs) == 0 ||
			bat.Vecs[0].GetType().Oid != types.T_Rowid {
			continue
		}
		// WriteBatch prepends the rowid after resolving the pk position, so a
		// usable pk position is always > 0 here; -1 means "unknown".
		// Composite-key batches resolve through pkMovePkPos (r26 review F2).
		pos := we.pkCheckPos
		if pos <= 0 || pos >= len(bat.Vecs) {
			pos = we.pkMovePkPos
		}
		if pos <= 0 || pos >= len(bat.Vecs) {
			continue
		}
		pkVec := bat.Vecs[pos]
		rowIDVec := bat.Vecs[0]
		n := min(bat.RowCount(), pkVec.Length())
		if rowIDVec.Length() < n {
			continue
		}
		var excluded []int64
		if excludedOf != nil {
			excluded = excludedOf(bat)
		}
		nextExcluded := 0
		for r := 0; r < n; r++ {
			for nextExcluded < len(excluded) && excluded[nextExcluded] < int64(r) {
				nextExcluded++
			}
			if nextExcluded < len(excluded) && excluded[nextExcluded] == int64(r) {
				continue
			}
			if pkVec.IsNull(uint64(r)) {
				continue
			}
			packer.Reset()
			if err := vector.EncodeRowToPacker(packer, pkVec, r); err != nil {
				// Encoding must never fail for a pk column vector; if it
				// does, provenance for this table is unusable (fail closed).
				return nil, false
			}
			if out == nil {
				out = make(map[string]types.Rowid, n)
			}
			out[string(append([]byte(nil), packer.Bytes()...))] =
				vector.GetFixedAtNoTypeCheck[types.Rowid](rowIDVec, r)
		}
	}
	return out, true
}

// pkMoveHarvestCacheEntry is the incremental per-(account, table) harvest:
// entries [0, scanned) are already folded into byTable; only the suffix is
// re-encoded per fill. gen pins the cache to the workspace compaction epoch
// — mergeTxnWorkspaceLocked re-keys merged rows, and every bump forces one
// full rescan. A scan watermark ahead of len(writes) (statement rollback
// truncated the slice) also rebuilds.
type pkMoveHarvestCacheEntry struct {
	gen     uint64
	selGen  uint64
	scanned int
	byTable map[string]types.Rowid
}

// pkMoveHarvestNewRowIDsCachedLocked resolves staged pk->rowid without
// re-encoding the whole workspace per staged link batch. Pure increment
// over pkMoveHarvestNewRowIDs: within one compaction epoch the writes slice
// is append-only between fills, and every structural rewrite bumps
// txn.pkMoveHarvestGen. Callers treat a nil result exactly like a harvest
// miss (fail closed). The fold also rides the batchSelectList generation:
// a same-key edit that selects an already-folded staged row out of its
// batch logically deletes that row in place, so the cached fold must
// rebuild instead of resolving a replacement to the stale generation.
func (txn *Transaction) pkMoveHarvestNewRowIDsCachedLocked(accountId uint32, tableId uint64) map[string]types.Rowid {
	key := pkMoveAcctTbl{account: accountId, table: tableId}
	ce := txn.pkMoveHarvestCache[key]
	if ce != nil && (ce.gen != txn.pkMoveHarvestGen || ce.selGen != txn.batchSelectListGen ||
		ce.scanned > len(txn.writes)) {
		delete(txn.pkMoveHarvestCache, key)
		ce = nil
	}
	if ce == nil {
		ce = &pkMoveHarvestCacheEntry{gen: txn.pkMoveHarvestGen, selGen: txn.batchSelectListGen}
		if txn.pkMoveHarvestCache == nil {
			txn.pkMoveHarvestCache = make(map[pkMoveAcctTbl]*pkMoveHarvestCacheEntry)
		}
		txn.pkMoveHarvestCache[key] = ce
	}
	folded, ok := pkMoveFoldHarvestEntries(txn.writes[ce.scanned:], accountId, tableId, ce.byTable,
		func(bat *batch.Batch) []int64 { return txn.batchSelectList[bat] })
	if !ok {
		// Poisoned table: drop the cache entry so every fill fails closed
		// instead of trusting a half-folded map.
		delete(txn.pkMoveHarvestCache, key)
		return nil
	}
	ce.byTable = folded
	ce.scanned = len(txn.writes)
	ce.selGen = txn.batchSelectListGen
	return ce.byTable
}

// pkMoveIsWorkspaceStageRowid reports whether rid belongs to the workspace
// identity segment: only such identities are pre-commit placeholders owed to
// staged inserts of this transaction (committed row ids and TN-assigned
// identities never name a workspace batch).
func pkMoveIsWorkspaceStageRowid(rid *types.Rowid) bool {
	return rid.BorrowSegmentID().EQ(&colexec.TxnWorkspaceSegment)
}

// pkMoveAcctTbl keys workspace-rowid bookkeeping by the OWNING user table:
// workspace row ids are only unique per transaction, and collisions across
// accounts or tables must never cross-match.
type pkMoveAcctTbl struct {
	account uint32
	table   uint64
}

// pkMoveRetargetUndoRec records one link row's previous new_row_id before a
// same-key marker retargeted it. RollbackLastStatement restores these in
// reverse order: the retarget mutates a link row an EARLIER statement
// staged, which the writes-slice truncation cannot undo. heal=true marks a
// pkMoveNullHeals change instead of a batch cell mutation; created=true
// means the binding itself was born this statement (rollback deletes it).
type pkMoveRetargetUndoRec struct {
	linkID string
	prev   types.Rowid
	// prevStmt is the pre-mutation birth statement of a heal binding; the
	// rollback recreate path needs it when the rolled-back statement
	// removed the binding outright (the workspace-to-S3 NULL switch).
	prevStmt int
	heal     bool
	created  bool
}

// pkMoveOriginUndoRec records one pkMoveRowidOrigins write so statement
// rollback can restore the previous mapping (or drop a freshly added one).
type pkMoveOriginUndoRec struct {
	key     types.Rowid
	prev    types.Rowid
	existed bool
}

// pkMoveDeathLedger carries the per-transaction liveness evidence for
// S3-route (NULL-identity) replacements into the keep computation. It is a
// plain value type so unit tests drive the decision without a transaction:
//   - dead: encoded pk -> the statement ids whose delete path diverted a
//     row of that key staged in an uncommitted CN object. The committed
//     stream never shows that death (the object is rewritten minus the
//     row), so without this record the prune cannot tell a live staged
//     replacement from one an uncommitted-object rewrite erased. A key
//     can divert-die in several statements; every death must be covered.
//   - markers: encoded pk -> the statement ids that processed a same-key
//     continuity marker for that key. A marker explains only the deaths
//     recorded in the SAME statement.
//   - birth: link_id -> staging statement id.
type pkMoveDeathLedger struct {
	dead    map[pkMoveAcctTbl]map[string]map[int]struct{}
	markers map[pkMoveAcctTbl]map[string]map[int]struct{}
	birth   map[string]int
	heals   map[string]types.Rowid
	// killed names link rows whose fill proved their lineage dead before
	// the generation they moved (the diverted-delete retirement); the
	// keep set drops them unconditionally and they never act as rescue
	// seeds or death-ledger continuers.
	killed map[string]struct{}
}

// pkMoveNullHeal binds a NULL-identity (S3-route) link row to the
// workspace identity a later resolved continuity marker established for
// the same key. The link's committed image keeps its NULL column (the
// single-shot claim then correctly governs it); the binding exists so the
// in-transaction keep evaluation tracks the lineage's REAL replacement
// identity — without it, a marker-rescued lineage whose staged replacement
// is later deleted from the workspace looks alive only because its
// stale S3-generation death was explained away.
type pkMoveNullHeal struct {
	rid  types.Rowid
	stmt int
}

// notePkMoveDivertedDeleteLocked records the encoded pk of a delete that
// will never reach the committed stream: the row lived in an uncommitted
// CN object (the delete diverts to the per-block list and the object is
// rewritten minus the row at commit) or in the raw workspace (deleteBatch
// swallows it out of the outgoing batch). Both boundaries share this
// kernel: the proof shape for the lineage ledgers is identical. Only tables with staged link rows
// in this transaction are tracked, and only for the [rowid, pk] delete
// layout the update and deletion operators produce. Synchronization
// contract (one rule for every provenance-map accessor): the caller MUST
// hold txn.Lock. deleteBatch batches the diverted rows of one delete
// batch behind a single lock hold, covering the parallel UpdateWriteS3
// producers compile can fan out on one transaction — a producer can
// still be staging link rows (pass-4 writes pkMoveLinkTables under the
// same lock) while another's output reaches source.Delete. The
// beginCatalogMutation fence is only an atomic revision counter pair,
// not mutual exclusion, so it cannot guard these maps.
// notePkMoveDivertedDeletes is the production divert/delete boundary:
// deleteBatch batches the diverted rows of one delete batch behind this
// single lock hold, because deleteBatch itself runs without txn.Lock
// (parallel UpdateWriteS3 producers on one transaction can each reach
// source.Delete) while the update pipeline's link staging mutates the
// same provenance maps under txn.Lock. Tests for the boundary must drive
// THIS function, not the inner Locked one, so removing the critical
// section is observable under -race.
//
// The diverted rows are provenance growth: one diverted DELETE wave
// after the transaction's final link fill would otherwise accumulate
// retirement/death records with no budget check in sight (the fill-end
// and admission checks only observe fill-driven growth). One check per
// critical section, after the whole batch is noted, keeps the wave
// statement-atomic: the error fails the deleting statement and its
// rollback truncates exactly these statement-tagged entries.
func (txn *Transaction) notePkMoveDivertedDeletes(tableId uint64, bat *batch.Batch, offsets []int64) error {
	if len(offsets) == 0 {
		return nil
	}
	txn.Lock()
	defer txn.Unlock()
	for _, off := range offsets {
		txn.notePkMoveDivertedDeleteLocked(tableId, bat, int(off))
	}
	// Charge the wave only once durable provenance is at stake: a
	// transaction holding no staged link rows consumes this evidence for
	// nothing — it dies with the transaction — and refusing it rejected
	// ordinary bulk non-key UPDATEs the moment a later DELETE crossed
	// the marker bookkeeping they left behind (r23 review F3). The
	// recording above still runs: in-transaction chain correctness
	// (murdered-lineage retirement) needs it regardless of the charge.
	if !txn.pkMoveHasLiveStagedLinksLocked() {
		return nil
	}
	return txn.checkPkMoveBookkeepBudgetLocked()
}

// notePkMoveSwallowedDeletes is the production raw-workspace delete
// boundary: a delete whose row lives in an uncommitted INSERT batch is
// excluded there (batchSelectList) and vanishes from the outgoing delete
// batch, so neither the staged-delete scan nor the diverted ledger ever
// sees the death. A by-key lineage binding bound across that death then
// stays alive: a later statement continuing the key composes or heals
// the replacement onto the murdered lineage, and the committed stream
// cannot show the gap. Record the swallowed deaths through the same
// ledgers, gates and budget discipline as the diverted boundary —
// same-statement staged-old coverage keeps legitimate edit chains
// explained, while a plain delete retires the binding it murdered, and
// statement rollback revives exactly its own records. Synchronization
// contract matches notePkMoveDivertedDeletes: deleteBatch runs without
// txn.Lock, so the wave goes behind this single lock hold and the
// budget check keeps the wave statement-atomic.
func (txn *Transaction) notePkMoveSwallowedDeletes(tableId uint64, bat *batch.Batch, sels []int64) error {
	if len(sels) == 0 {
		return nil
	}
	txn.Lock()
	defer txn.Unlock()
	for _, s := range sels {
		txn.notePkMoveDivertedDeleteLocked(tableId, bat, int(s))
	}
	// Same participation gate as the diverted boundary: the wave's
	// ledger entries serve only the commit evaluation of staged links,
	// so with no staged link the wave is recorded but never charged
	// (r23 review F3).
	if !txn.pkMoveHasLiveStagedLinksLocked() {
		return nil
	}
	return txn.checkPkMoveBookkeepBudgetLocked()
}

func (txn *Transaction) notePkMoveDivertedDeleteLocked(tableId uint64, bat *batch.Batch, row int) {
	if len(bat.Vecs) < 2 || len(bat.Attrs) < 2 || bat.Attrs[1] != "pk" ||
		(len(txn.pkMoveLinkTables) == 0 && len(txn.pkMoveKeyLineage) == 0) {
		return
	}
	pkVec := bat.Vecs[1]
	if pkVec.IsNull(uint64(row)) {
		return
	}
	packer := types.NewPacker()
	pkOK := true
	if err := vector.EncodeRowToPacker(packer, pkVec, row); err != nil {
		pkOK = false
	}
	// The packer buffer dies with Close below: snapshot the encoded pk
	// once for both ledgers.
	pk := ""
	if pkOK {
		pk = string(append([]byte(nil), packer.Bytes()...))
	}
	// Retirement evidence runs BEFORE the staged-link gate: a diverted
	// delete lands outside the staged-delete scan no matter when the
	// first move link arrives, and a by-key lineage binding must know
	// its bound generation was murdered even if the binding is this
	// transaction's only provenance so far. Without the record, a
	// spilled reinsert plus a later move heals the unrelated row onto
	// the dead lineage and the apply cascades its children across.
	if pkOK && len(txn.pkMoveKeyLineage) > 0 &&
		bat.Vecs[0].GetType().Oid == types.T_Rowid {
		rid := vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[0], row)
		for key := range txn.pkMoveKeyLineage {
			if key.table != tableId {
				continue
			}
			txn.pkMoveRetireDeaths = notePkMoveRetireDeath(
				txn.pkMoveRetireDeaths, key, pk, rid, txn.statementID)
		}
	}
	packer.Close()
	if len(txn.pkMoveLinkTables) == 0 {
		return
	}
	matched := false
	for key := range txn.pkMoveLinkTables {
		if key.table == tableId {
			matched = true
			break
		}
	}
	if !matched || !pkOK {
		return
	}
	for key := range txn.pkMoveLinkTables {
		if key.table != tableId {
			continue
		}
		txn.pkMoveDeadPks = notePkMoveStatement(
			txn.pkMoveDeadPks, key, pk, txn.statementID)
	}
}

// notePkMoveStatement records stmt in m[key][pk]'s statement set. A key
// can die diverted or be continued in several statements of one
// transaction, and every recorded statement must stay available to the
// keep evaluation: collapsing to the latest statement would let a later
// edit erase an earlier edit's death-or-continuity evidence (or let an
// unrelated later marker cover a death it never consumed).
func notePkMoveStatement(
	m map[pkMoveAcctTbl]map[string]map[int]struct{}, key pkMoveAcctTbl, pk string, stmt int,
) map[pkMoveAcctTbl]map[string]map[int]struct{} {
	if m == nil {
		m = make(map[pkMoveAcctTbl]map[string]map[int]struct{})
	}
	set := m[key]
	if set == nil {
		set = make(map[string]map[int]struct{})
		m[key] = set
	}
	stmts := set[pk]
	if stmts == nil {
		stmts = make(map[int]struct{})
		set[pk] = stmts
	}
	stmts[stmt] = struct{}{}
	return m
}

// retirePkMoveFinishedMarkerStmtsLocked drops the FINISHED statement's
// continuity-marker ledger entries (r26 review F5 / D112): a marker on
// key K in statement d only ever explains a death of K recorded in the
// SAME statement d, and no future statement can record a death under d,
// so the entry is load-bearing only while d is open. Retention must stay
// O(live rows) for ordinary repeated updates, not one ledger entry per
// UPDATE forever. When the finished statement diverted a death of K as
// well, marker and death STAY as a pair: the commit prune matches old
// deaths against same-statement markers, and dropping either half would
// turn an explained death into an unexplained killer. Such pairs are
// bounded by the transaction's diverted-delete traffic — the evidence
// itself — while the marker-only entries this removes are the bulk
// non-key UPDATE overhead the r26 review priced. Statement rollback is
// unaffected: rollback tail-cuts only entries above the surviving
// statement, and the finished statement's records sit beyond rollback
// reach at this hook.
func (txn *Transaction) retirePkMoveFinishedMarkerStmtsLocked(finishedStmt int) {
	for acctTbl, perKey := range txn.pkMoveMarkerStmts {
		for k, stmts := range perKey {
			if _, ok := stmts[finishedStmt]; !ok {
				continue
			}
			if deadSet := txn.pkMoveDeadPks[acctTbl][k]; deadSet != nil {
				if _, paired := deadSet[finishedStmt]; paired {
					continue
				}
			}
			delete(stmts, finishedStmt)
			if len(stmts) == 0 {
				delete(perKey, k)
			}
		}
		if len(perKey) == 0 {
			delete(txn.pkMoveMarkerStmts, acctTbl)
		}
	}
}

// sweepPkMoveFinishedContinuityLocked compacts the statement-tagged
// continuity history once the statement boundary proves its entries sit
// beyond rollback reach (r27 review F6 / D119). The unresolved-marker
// route (e.g. the S3 buffering path of a plain same-key UPDATE) appends
// one lineage binding and one staged-old coverage entry per key per
// statement, and the resolved-generation retirement never sees either,
// so repeated edits kept O(rows x statements) of identical history —
// each re-bind composes the SAME durable origin out of the live top.
// Reclaiming rules:
//
//   - Binding stacks: lookups read only a stack's top, consumption
//     pushes a new top, and RollbackLastStatement tail-cuts entries
//     above the surviving statement, which finished entries can never
//     be: everything below the top is unreachable. The surviving top
//     keeps its origin, watermark, stmt tag, and consumption flag
//     verbatim, so later lookups, heals, and kills observe identical
//     answers. A top already consumed by the finished statement is
//     permanently invisible (its consumption can no longer roll back) —
//     the whole key retires, and a fresh generation arriving later
//     starts a new lineage epoch, exactly what the dead lineage
//     requires.
//   - Staged-old coverage (the pkMoveStagedOldRids explanations): an
//     entry's rid was consumed by the very edit that observed it, so no
//     FUTURE statement can divert-delete that identity — the S3 write
//     that replaced it already removed the row from the readable state,
//     the same "batch row ceases to be visible" argument D112 applies
//     to resolved compositions. The entry then serves only deaths
//     ALREADY recorded, so it retires when the retained death ledger
//     names no such rid; a recorded death keeps its explanation until
//     that ledger entry itself retires or rolls back.
//
// Bindings carry no undo records and the coverage rebuild at rollback
// re-derives old sides from surviving link rows, so the dropped history
// is never needed again. Retention returns to O(live keys), matching
// the flat curve D112 proved for the exact origin map.
func (txn *Transaction) sweepPkMoveFinishedContinuityLocked(finishedStmt int) {
	for acctTbl, set := range txn.pkMoveKeyLineage {
		for pk, stack := range set {
			if len(stack) == 0 {
				delete(set, pk)
				continue
			}
			// Bindings are appended with ascending stmt ids and none can
			// outlive the current statement, so at this hook every element
			// satisfies stmt <= finishedStmt.
			top := stack[len(stack)-1]
			if top.consumed && top.stmt <= finishedStmt {
				delete(set, pk)
				continue
			}
			if len(stack) > 1 {
				// Fresh slice: the old backing array must not pin the
				// dropped prefix.
				set[pk] = []pkMoveLineageBind{top}
			}
		}
		if len(set) == 0 {
			delete(txn.pkMoveKeyLineage, acctTbl)
		}
	}
	if len(txn.pkMoveStagedOldRids) != 0 {
		var deathsByRid map[types.Rowid]struct{}
		for _, perKey := range txn.pkMoveRetireDeaths {
			for _, list := range perKey {
				for _, d := range list {
					if deathsByRid == nil {
						deathsByRid = make(map[types.Rowid]struct{})
					}
					deathsByRid[d.rid] = struct{}{}
				}
			}
		}
		for rid := range txn.pkMoveStagedOldRids {
			if _, keep := deathsByRid[rid]; !keep {
				delete(txn.pkMoveStagedOldRids, rid)
			}
		}
		if len(txn.pkMoveStagedOldRids) == 0 {
			txn.pkMoveStagedOldRids = nil
		}
	}
}

// pkMoveLatestStmt returns the greatest statement in a recorded set, or
// false when the set is empty.
func pkMoveLatestStmt(stmts map[int]struct{}) (int, bool) {
	latest, ok := 0, false
	for s := range stmts {
		if !ok || s > latest {
			latest, ok = s, true
		}
	}
	return latest, ok
}

// rollbackPkMoveStmtStateLocked undoes the provenance side effects of the
// statement RollbackLastStatement just truncated. Must run AFTER the
// writes slice was truncated and txn.statementID already stepped back past
// the rolled-back statement: mutations the truncation removed are skipped
// by the restore scan naturally, and every statement-tagged record beyond
// the surviving statements is dropped.
func (txn *Transaction) rollbackPkMoveStmtStateLocked() error {
	// Restore retargeted new_row_id cells and heal bindings (reverse
	// order; the undo list holds each entry's value from BEFORE this
	// statement). Restore stays best-effort-complete: one faulting cell
	// must not skip the rest of the undo list, and the joined failure
	// propagates to the rollback's caller instead of vanishing — a
	// silently unrestored identity would keep a dead replacement named.
	var retErr error
	// The staged-link cell index is built lazily on the first non-heal
	// undo record and scanned ONCE per rollback: restoring N retargets by
	// re-scanning every staged link batch per record costs N(N+1)/2
	// row-id comparisons under txn.Lock, so bulk payload edits after many
	// recorded moves turn a statement rollback into a seconds-long
	// quadratic stall. One pass indexes link id -> cell; each restore then
	// touches exactly its own row.
	var cellIndex map[string]pkMoveStagedLinkCell
	indexBuilt := false
	for i := len(txn.pkMoveRetargetUndo) - 1; i >= 0; i-- {
		rec := txn.pkMoveRetargetUndo[i]
		if rec.heal {
			if rec.created {
				delete(txn.pkMoveNullHeals, rec.linkID)
			} else if h, ok := txn.pkMoveNullHeals[rec.linkID]; ok {
				h.rid = rec.prev
				txn.pkMoveNullHeals[rec.linkID] = h
			} else {
				// The rolled-back statement removed this binding outright
				// (the workspace-to-S3 NULL switch): recreate it with its
				// pre-mutation birth statement.
				txn.pkMoveNullHeals[rec.linkID] = pkMoveNullHeal{
					rid: rec.prev, stmt: rec.prevStmt,
				}
			}
			continue
		}
		if !indexBuilt {
			cellIndex = txn.pkMoveStagedLinkCellIndexLocked()
			indexBuilt = true
		}
		cell, ok := cellIndex[rec.linkID]
		if !ok {
			// The row left the workspace with the rolled-back
			// statement's truncation (or was pruned): either way there
			// is nothing the restore could damage.
			continue
		}
		retErr = errors.Join(retErr, txn.restorePkMoveLinkCellLocked(cell, rec.prev))
	}
	txn.pkMoveRetargetUndo = nil
	txn.pkMoveRetargetUndoSeen = nil
	for i := len(txn.pkMoveOriginUndo) - 1; i >= 0; i-- {
		rec := txn.pkMoveOriginUndo[i]
		if rec.existed {
			txn.pkMoveRowidOrigins[rec.key] = rec.prev
		} else {
			delete(txn.pkMoveRowidOrigins, rec.key)
		}
	}
	txn.pkMoveOriginUndo = nil
	txn.pkMoveOriginUndoSeen = nil
	// The harvest cache folded entries of the rolled-back statement. Its
	// length-watermark guard only notices a truncated slice until a retry
	// refills the writes slice to the same length — drop the cache instead.
	txn.pkMoveHarvestCache = nil
	// Statement-tagged records beyond the surviving statements die.
	for id, stmt := range txn.pkMoveLinkBirth {
		if stmt > txn.statementID {
			delete(txn.pkMoveLinkBirth, id)
		}
	}
	for acctTbl, set := range txn.pkMoveDeadPks {
		for pk, stmts := range set {
			for stmt := range stmts {
				if stmt > txn.statementID {
					delete(stmts, stmt)
				}
			}
			if len(stmts) == 0 {
				delete(set, pk)
			}
		}
		if len(set) == 0 {
			delete(txn.pkMoveDeadPks, acctTbl)
		}
	}
	for acctTbl, set := range txn.pkMoveMarkerStmts {
		for pk, stmts := range set {
			for stmt := range stmts {
				if stmt > txn.statementID {
					delete(stmts, stmt)
				}
			}
			if len(stmts) == 0 {
				delete(set, pk)
			}
		}
		if len(set) == 0 {
			delete(txn.pkMoveMarkerStmts, acctTbl)
		}
	}
	for id, h := range txn.pkMoveNullHeals {
		if h.stmt > txn.statementID {
			delete(txn.pkMoveNullHeals, id)
		}
	}
	// Lineage bindings: append-only statement-tagged stacks — cut each
	// key's tail above the surviving statements (bindings are append or
	// same-statement replace with ascending stmt ids, so all entries to
	// drop sit in one tail). The delete-scan cache folded rolled-back
	// write entries; drop it wholesale like the harvest cache.
	for acctTbl, set := range txn.pkMoveKeyLineage {
		for pk, stack := range set {
			cut := len(stack)
			for j := len(stack) - 1; j >= 0; j-- {
				if stack[j].stmt > txn.statementID {
					cut = j
				} else {
					break
				}
			}
			if cut == 0 {
				delete(set, pk)
			} else if cut < len(stack) {
				set[pk] = stack[:cut]
			}
		}
		if len(set) == 0 {
			delete(txn.pkMoveKeyLineage, acctTbl)
		}
	}
	// The diverted-death retirement ledger keeps the same statement-tagged
	// tail discipline as the binding stacks: a rolled-back DELETE revives
	// the lineage bindings its diverted rows retired. Kill records drop
	// with the statement whose fill made them.
	for acctTbl, set := range txn.pkMoveRetireDeaths {
		for pk, list := range set {
			cut := len(list)
			for j := len(list) - 1; j >= 0; j-- {
				if list[j].stmt > txn.statementID {
					cut = j
				} else {
					break
				}
			}
			if cut == 0 {
				delete(set, pk)
			} else if cut < len(list) {
				set[pk] = list[:cut]
			}
		}
		if len(set) == 0 {
			delete(txn.pkMoveRetireDeaths, acctTbl)
		}
	}
	for id, stmt := range txn.pkMoveKilledLinks {
		if stmt > txn.statementID {
			delete(txn.pkMoveKilledLinks, id)
		}
	}
	// Rebuild the staged-old coverage set with the fill population's own
	// populate-missing discipline: observations a rolled-back statement
	// made die with it, a SURVIVING statement's observation stands (its
	// fill saw that identity as an old side, whatever later heals wrote
	// into the batch cells), and the surviving link rows' current cells
	// cover anything not already observed (stamp 0 — staged by surviving
	// statements, so no later rollback may retire them through this
	// path). A wholesale rescan used to erase surviving fills' coverage,
	// turning a legitimate explained death into a retirement after any
	// unrelated later rollback.
	for rid, stmt := range txn.pkMoveStagedOldRids {
		if stmt > txn.statementID {
			delete(txn.pkMoveStagedOldRids, rid)
		}
	}
	for rid := range txn.scanPkMoveStagedOldRidsLocked() {
		if txn.pkMoveStagedOldRids == nil {
			txn.pkMoveStagedOldRids = make(map[types.Rowid]int)
		}
		if _, ok := txn.pkMoveStagedOldRids[rid]; !ok {
			txn.pkMoveStagedOldRids[rid] = 0
		}
	}
	if len(txn.pkMoveStagedOldRids) == 0 {
		txn.pkMoveStagedOldRids = nil
	}
	txn.pkMoveDeleteScan = nil
	// Retention genesis (D161(B)): the rolled statement's registration
	// transitions unwind here — the map entries return to ABSENT so a retried
	// statement re-probes and re-resolves OWNED/EXTERNAL under its own
	// snapshot; the staged rows die with the positional workspace cut.
	txn.unwindPkMoveGenesisJournalLocked()
	return retErr
}

// scanPkMoveStagedOldRidsLocked walks every staged link row and collects
// the current old_row_id cells into a fresh coverage set stamped 0. Used
// by statement rollback (fill-time population stamps the observing
// statement and is gone with it).
func (txn *Transaction) scanPkMoveStagedOldRidsLocked() map[types.Rowid]int {
	var out map[types.Rowid]int
	for i := range txn.writes {
		we := &txn.writes[i]
		if !isPkMoveLinkTableInsert(we.databaseName, we.tableName) ||
			we.typ != INSERT || we.bat == nil || we.bat.RowCount() == 0 {
			continue
		}
		oldRowIdx := -1
		for j, attr := range we.bat.Attrs {
			if attr == catalog.DataBranchUpdateLinkColOldRow {
				oldRowIdx = j
				break
			}
		}
		if oldRowIdx < 0 || oldRowIdx >= len(we.bat.Vecs) {
			continue
		}
		oldRowVec := we.bat.Vecs[oldRowIdx]
		for r := 0; r < we.bat.RowCount(); r++ {
			if oldRowVec.IsNull(uint64(r)) {
				continue
			}
			raw := oldRowVec.GetBytesAt(r)
			if len(raw) != types.RowidSize {
				continue
			}
			if out == nil {
				out = make(map[types.Rowid]int)
			}
			out[types.DecodeFixed[types.Rowid](raw)] = 0
		}
	}
	return out
}

// pkMoveStagedLinkCell locates one staged link row inside the workspace
// for a direct restore.
type pkMoveStagedLinkCell struct {
	writesIdx int
	row       int
}

// Test seam: fired once per staged link row the rollback index build
// visits, so a test can assert a whole rollback performs exactly one
// linear pass over the staged link rows regardless of how many retargets
// it restores.
var pkMoveRestoreScanVisitHook func()

// pkMoveStagedLinkCellIndexLocked builds one link-id -> staged-cell index
// over the whole workspace in a single scan. First occurrence wins in
// (writes, row) order, exactly what the old per-record rescan matched;
// link ids are unique across the staged rows by construction. Must be
// called with txn.Lock held.
func (txn *Transaction) pkMoveStagedLinkCellIndexLocked() map[string]pkMoveStagedLinkCell {
	var out map[string]pkMoveStagedLinkCell
	for i := range txn.writes {
		we := &txn.writes[i]
		if !isPkMoveLinkTableInsert(we.databaseName, we.tableName) ||
			we.typ != INSERT || we.bat == nil || we.bat.RowCount() == 0 {
			continue
		}
		idIdx := -1
		for j, attr := range we.bat.Attrs {
			if attr == catalog.DataBranchUpdateLinkColLinkID {
				idIdx = j
				break
			}
		}
		if idIdx < 0 || idIdx >= len(we.bat.Vecs) {
			continue
		}
		idVec := we.bat.Vecs[idIdx]
		for r := 0; r < we.bat.RowCount(); r++ {
			if pkMoveRestoreScanVisitHook != nil {
				pkMoveRestoreScanVisitHook()
			}
			if idVec.IsNull(uint64(r)) {
				continue
			}
			key := string(idVec.GetBytesAt(r))
			if _, exists := out[key]; exists {
				continue
			}
			if out == nil {
				out = make(map[string]pkMoveStagedLinkCell)
			}
			out[key] = pkMoveStagedLinkCell{writesIdx: i, row: r}
		}
	}
	return out
}

// restorePkMoveLinkCellLocked writes prev back into the new_row_id cell
// at the indexed staged link row. A write failure is returned: the caller
// aggregates and surfaces it, and the workspace memory accounting of the
// mutated entry stays balanced on every path.
func (txn *Transaction) restorePkMoveLinkCellLocked(cell pkMoveStagedLinkCell, prev types.Rowid) error {
	we := &txn.writes[cell.writesIdx]
	newRowIdx := -1
	for j, attr := range we.bat.Attrs {
		if attr == catalog.DataBranchUpdateLinkColNewRow {
			newRowIdx = j
			break
		}
	}
	if newRowIdx < 0 || newRowIdx >= len(we.bat.Vecs) || cell.row >= we.bat.RowCount() {
		// The index was built from this same workspace inside the
		// rollback and nothing mutates the batches between build and
		// restore; the guard only keeps a malformed cell harmless.
		return nil
	}
	newRowVec := we.bat.Vecs[newRowIdx]
	accounted := we.accountedSize != 0
	if accounted {
		txn.unaccountWorkspaceEntryLocked(we)
	}
	var err error
	if newRowVec.IsNull(uint64(cell.row)) {
		// The rolled-back statement nullified this cell (the
		// workspace-to-S3 route switch): clear the null bit and write
		// the recorded workspace identity back. Re-establishing a NULL
		// cell is an initial write, not the replacement of a measured
		// payload, so it keeps the appending API whose growth the
		// accounting wrap measures (r40 review F2, D148).
		nulls.Del(newRowVec.GetNulls(), uint64(cell.row))
		err = pkMoveVecSetBytesAt(newRowVec, cell.row, prev[:], txn.proc.Mp())
	} else {
		// Same-binary-length in-place overwrite of a retained, measured
		// cell, exactly like the retarget this restores (r40 review F2,
		// D148); the null bit never moves because retargets only ever
		// wrote non-null cells.
		err = pkMoveVecOverwriteRowidAt(newRowVec, cell.row, prev[:], txn.proc.Mp())
	}
	if accounted {
		txn.accountWorkspaceEntryLocked(we)
	}
	return err
}

// pkMoveStagedLinkPinnedRowIDsLocked returns every transaction-local
// workspace row id that a statement-boundary dump must leave in memory:
// the old and new inhabitants named by staged pk-move link rows and the
// workspace bindings of pending heals. A statement-boundary dump that
// converts one of those rows into an object row mints a durable identity
// the recorded provenance cannot name, and every later statement's chain
// through the row (the successor pin matches departure and arrival by
// exact identity) severs. The pin is per ROW ID, not per table: the
// unrelated rows sharing the owner's TABLE keep spilling — retention is
// bounded by the number of provenance-relevant rows, not by whatever
// else the transaction writes into that table.
//
// The durable-origin ledger (pkMoveRowidOrigins) deliberately pins
// NOTHING through its keys: an origin entry precedes its lineage's first
// link (a payload edit, a workspace spill, then the move), so key-
// pinning retained every edited row's complete payload batch at every
// statement-boundary dump — the whole updated payload of a moving
// transaction with no link yet (r24 review F3). The dump instead
// SHADOWS the spilling origin-keyed rows and translates the ledger keys
// onto the durable object identities the write mints (see
// recordPkMoveDumpShadowLocked / translatePkMoveOriginDumpShadowLocked),
// keeping every later move's old-side rewrite exact while ordinary
// payload updates spill like any other workspace wave. A workspace-
// staged origin VALUE (defensive: the fill only records durable values)
// still pins, matching the compaction remap's defensive posture. The
// scan reads the staged link batches' row-id columns once per statement-
// boundary dump; the coordinator admission budget bounds the staged link
// bytes, so the scan stays cheap. Pruned links retire their pins
// automatically (their zero-row batches contribute nothing), while heal
// entries live until commit — exactly the window in which a later
// statement can still name those identities. Must be called with
// txn.Lock held.
// pkMoveStagedLinkRowIDsLocked collects every row-id value staged into
// the link table's old_row/new_row columns by this transaction. The scan
// reads the staged link batches' row-id columns once per caller; the
// coordinator admission budget bounds the staged link bytes, so the
// scan stays cheap. Pruned links retire their rows automatically (their
// zero-row batches contribute nothing). Must be called with txn.Lock
// held.
func (txn *Transaction) pkMoveStagedLinkRowIDsLocked() map[types.Rowid]struct{} {
	var pinned map[types.Rowid]struct{}
	add := func(rid types.Rowid) {
		if pinned == nil {
			pinned = make(map[types.Rowid]struct{})
		}
		pinned[rid] = struct{}{}
	}
	addVarlenaCol := func(we *Entry, col string) {
		idx := -1
		for j, attr := range we.bat.Attrs {
			if attr == col {
				idx = j
				break
			}
		}
		if idx < 0 || idx >= len(we.bat.Vecs) {
			return
		}
		vec := we.bat.Vecs[idx]
		if vec.GetType().Oid != types.T_varbinary && vec.GetType().Oid != types.T_varchar &&
			vec.GetType().Oid != types.T_binary {
			return
		}
		for r := 0; r < we.bat.RowCount(); r++ {
			if vec.IsNull(uint64(r)) {
				continue
			}
			raw := vec.GetBytesAt(r)
			if len(raw) != types.RowidSize {
				continue
			}
			add(types.DecodeFixed[types.Rowid](raw))
		}
	}
	for i := range txn.writes {
		we := &txn.writes[i]
		if we.typ != INSERT || we.bat == nil || we.bat.RowCount() == 0 ||
			!isPkMoveLinkTableInsert(we.databaseName, we.tableName) {
			continue
		}
		addVarlenaCol(we, catalog.DataBranchUpdateLinkColOldRow)
		addVarlenaCol(we, catalog.DataBranchUpdateLinkColNewRow)
	}
	return pinned
}

func (txn *Transaction) pkMoveStagedLinkPinnedRowIDsLocked() map[types.Rowid]struct{} {
	pinned := txn.pkMoveStagedLinkRowIDsLocked()
	add := func(rid types.Rowid) {
		if pinned == nil {
			pinned = make(map[types.Rowid]struct{})
		}
		pinned[rid] = struct{}{}
	}
	for _, v := range txn.pkMoveRowidOrigins {
		// Origin KEYS no longer pin: the statement-boundary dump shadows
		// their rows and translates the keys onto the derived object
		// identities (r24 review F3). Keep only the defensive workspace-
		// staged VALUE pin; the fill records durable values today, but a
		// workspace value would dangle identically across a spill.
		if pkMoveIsWorkspaceStageRowid(&v) {
			add(v)
		}
	}
	for _, h := range txn.pkMoveNullHeals {
		if pkMoveIsWorkspaceStageRowid(&h.rid) {
			add(h.rid)
		}
	}
	return pinned
}

// pkMoveRewriteMigrationRelevantLocked collects every row-id identity
// the pk-move bookkeeping can currently name: staged link row-id cells,
// durable-origin keys AND values, and null-heal targets. The deletion-
// driven CN object rewrite (compactDeletionOnObjsLocked) re-mints the
// row id of every row SURVIVING a dirty object's rewrite; provenance
// naming a pre-rewrite identity would dangle exactly like a dump-shadow
// loss, so the workers shadow the named survivors and the parent
// re-keys the bookkeeping through the derived identities (r25 review
// F2). Returns nil when nothing is recorded, so ordinary transactions
// keep the zero-read fast path. Must be called with txn.Lock held.
func (txn *Transaction) pkMoveRewriteMigrationRelevantLocked() map[types.Rowid]struct{} {
	relevant := txn.pkMoveStagedLinkRowIDsLocked()
	add := func(rid types.Rowid) {
		if relevant == nil {
			relevant = make(map[types.Rowid]struct{})
		}
		relevant[rid] = struct{}{}
	}
	for k, v := range txn.pkMoveRowidOrigins {
		add(k)
		add(v)
	}
	for _, h := range txn.pkMoveNullHeals {
		add(h.rid)
	}
	return relevant
}

// pkMoveBatchHasPinnedRowID reports whether a workspace INSERT batch
// holds at least one row whose identity a staged link, origin entry or
// heal binding still names. The row-id vector sits at index 0 exactly as
// writeBatch prepends it; zero-row batches keep nothing.
func pkMoveBatchHasPinnedRowID(bat *batch.Batch, pinned map[types.Rowid]struct{}) bool {
	if bat == nil || bat.RowCount() == 0 || len(bat.Vecs) == 0 ||
		bat.Vecs[0].GetType().Oid != types.T_Rowid {
		return false
	}
	ids := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[0])
	n := min(bat.RowCount(), len(ids))
	for r := 0; r < n; r++ {
		if _, ok := pinned[ids[r]]; ok {
			return true
		}
	}
	return false
}

// No per-spill row-id tracking supplements the prune's staged set: the
// fill names only rows the workspace currently stages, and every dump
// recomputes pkMoveStagedLinkPinnedRowIDsLocked from the staged link
// cells, so a batch carrying a named identity is pinned in memory until
// the final prune — a spill therefore never owns an identity any staged
// link can name, and recording spilled ids only accumulated provenance
// the prune could never consult.

// pkMovePinnedRetainedBytesLocked measures the workspace bytes the pin
// rule retains for this dump: every in-memory INSERT batch holding at
// least one pinned row stays, and the pin is row-based while retention
// is batch-wide, so one pinned wide row holds its whole batch in memory.
// The measure exactly matches the keep predicate (INSERT, fileName == "",
// has pinned row) so the coordinator budget charges what the dump
// actually retains. Must be called with txn.Lock held; the caller
// recomputes it per dump from the live workspace, so batches a later
// dump releases drop out of the measure naturally.
func (txn *Transaction) pkMovePinnedRetainedBytesLocked(pinned map[types.Rowid]struct{}) int64 {
	if len(pinned) == 0 {
		return 0
	}
	var total int64
	for i := range txn.writes {
		we := &txn.writes[i]
		if we.typ != INSERT || we.fileName != "" || we.bat == nil ||
			we.bat.RowCount() == 0 {
			continue
		}
		if pkMoveBatchHasPinnedRowID(we.bat, pinned) {
			total += int64(we.bat.Size())
		}
	}
	return total
}

// pkMoveDumpShadowRow records one spilling workspace row whose identity
// pkMoveRowidOrigins still keys on: the dump mints every spilling row a
// durable object identity, and without a shadow the ledger key would
// dangle — a later move of this row would look up its old side under
// the new object identity and MISS, naming the intermediate generation
// instead of the durable origin the committed stream shows departing.
type pkMoveDumpShadowRow struct {
	pk string
	ws types.Rowid
}

// recordPkMoveDumpShadowLocked captures the (encoded pk, workspace row
// id) of every row of we.bat that pkMoveRowidOrigins keys on, grouped by
// the workspace table key the dump writes one object for. Returning the
// possibly-allocated map keeps the no-provenance case allocation-free.
// An origins hit on a batch without a usable pk position FAILS the dump:
// spilling it unshadowed silently severs in-transaction lineage, so the
// only safe outcomes are an exact shadow or a refused statement. Must be
// called with txn.Lock held.
func (txn *Transaction) recordPkMoveDumpShadowLocked(
	shadows map[workspaceTableKey][]pkMoveDumpShadowRow,
	key workspaceTableKey,
	we *Entry,
) (map[workspaceTableKey][]pkMoveDumpShadowRow, error) {
	bat := we.bat
	if bat == nil || bat.RowCount() == 0 || len(bat.Vecs) == 0 ||
		bat.Vecs[0].GetType().Oid != types.T_Rowid {
		return shadows, nil
	}
	rowIDVec := bat.Vecs[0]
	n := min(bat.RowCount(), rowIDVec.Length())
	hits := make([]int, 0, 4)
	for r := 0; r < n; r++ {
		if _, ok := txn.pkMoveRowidOrigins[vector.GetFixedAtNoTypeCheck[types.Rowid](rowIDVec, r)]; ok {
			hits = append(hits, r)
		}
	}
	if len(hits) == 0 {
		return shadows, nil
	}
	pos := we.pkCheckPos
	if pos <= 0 || pos >= len(bat.Vecs) {
		// composite-key batches resolve through pkMovePkPos (r26 review F2)
		pos = we.pkMovePkPos
	}
	if pos <= 0 || pos >= len(bat.Vecs) {
		return shadows, moerr.NewInternalErrorNoCtxf(
			"pk-move dump shadow: %d origin-keyed rows of %s.%s lack a usable pk column position %d",
			len(hits), we.databaseName, we.tableName, pos)
	}
	pkVec := bat.Vecs[pos]
	packer := types.NewPacker()
	defer packer.Close()
	if shadows == nil {
		shadows = make(map[workspaceTableKey][]pkMoveDumpShadowRow)
	}
	rows := shadows[key]
	for _, r := range hits {
		if r >= pkVec.Length() {
			return shadows, moerr.NewInternalErrorNoCtxf(
				"pk-move dump shadow: %s.%s pk column shorter than the row-id column",
				we.databaseName, we.tableName)
		}
		if pkVec.IsNull(uint64(r)) {
			return shadows, moerr.NewInternalErrorNoCtxf(
				"pk-move dump shadow: %s.%s origin-keyed row %d has a NULL pk",
				we.databaseName, we.tableName, r)
		}
		packer.Reset()
		if err := vector.EncodeRowToPacker(packer, pkVec, r); err != nil {
			return shadows, err
		}
		rows = append(rows, pkMoveDumpShadowRow{
			pk: string(packer.Bytes()),
			ws: vector.GetFixedAtNoTypeCheck[types.Rowid](rowIDVec, r),
		})
	}
	shadows[key] = rows
	return shadows, nil
}

// resolvePkMoveDumpShadowLocked joins the shadow rows of one dumped
// table against the object(s) the CNS3Writer just wrote: the writer
// sorts the wave by pk when it flushes, so row order in the object
// cannot be assumed; reading the pk column back and joining on encoded
// pk values is the only comparator-free way to recover row position.
// Every shadow row must match exactly one object row and duplicate pks
// are impossible inside one live workspace wave — any violation means
// the object no longer reflects the workspace the shadow recorded, so
// the dump must fail loudly instead of publishing parity-losing
// provenance. Returns workspace id -> derived object id. Must be called
// with txn.Lock held.
func (txn *Transaction) resolvePkMoveDumpShadowLocked(
	ctx context.Context,
	fs fileservice.FileService,
	stats []objectio.ObjectStats,
	tableDef *pbplan.TableDef,
	rows []pkMoveDumpShadowRow,
) (map[types.Rowid]types.Rowid, error) {
	// Delegates to the lock-free form: the deletion-rewrite migration's
	// workers resolve shadows against objects THEY just wrote without
	// touching txn state (r25 review F2).
	return resolvePkMoveDumpShadow(ctx, txn.proc.GetMPool(), fs, stats, tableDef, rows)
}

// pkMoveShadowPkColumn locates the primary-key column's sequence number
// and type for shadow encoding and read-back parity joins. hasKey
// reports whether the table definition declares any primary key; found
// reports whether the key column exists in the definition.
func pkMoveShadowPkColumn(
	tableDef *pbplan.TableDef,
) (seq uint16, pkType types.Type, hasKey bool, found bool) {
	if tableDef == nil || tableDef.Pkey == nil {
		return 0, types.Type{}, false, false
	}
	for _, colDef := range tableDef.Cols {
		if colDef.Name == tableDef.Pkey.PkeyColName {
			return uint16(colDef.Seqnum), types.MustTypeFromPlan(colDef.Typ), true, true
		}
	}
	return 0, types.Type{}, true, false
}

// resolvePkMoveDumpShadow joins the shadow rows against the object(s) a
// CNS3 writer just wrote: the writer sorts the wave by pk when it
// flushes, so row order in the object cannot be assumed; reading the pk
// column back and joining on encoded pk values is the only
// comparator-free way to recover row position. Every shadow row must
// match exactly one object row and duplicate pks are impossible inside
// one live wave — any violation means the object no longer reflects the
// wave the shadow recorded, so the caller must fail loudly instead of
// publishing parity-losing provenance. Returns recorded id -> derived
// object id. Lock-free: the mpool is explicit.
func resolvePkMoveDumpShadow(
	ctx context.Context,
	mp *mpool.MPool,
	fs fileservice.FileService,
	stats []objectio.ObjectStats,
	tableDef *pbplan.TableDef,
	rows []pkMoveDumpShadowRow,
) (map[types.Rowid]types.Rowid, error) {
	seq16, pkType, hasKey, found := pkMoveShadowPkColumn(tableDef)
	if !hasKey {
		return nil, moerr.NewInternalErrorNoCtxf(
			"pk-move dump shadow: %d shadow rows on a table without a primary key", len(rows))
	}
	if !found {
		return nil, moerr.NewInternalErrorNoCtxf(
			"pk-move dump shadow: pk column %q missing from the table definition",
			tableDef.Pkey.PkeyColName)
	}
	seq := int(seq16)
	want := make(map[string]types.Rowid, len(rows))
	for _, row := range rows {
		if _, dup := want[row.pk]; dup {
			return nil, moerr.NewInternalErrorNoCtxf(
				"pk-move dump shadow: duplicate pk inside one dump shadow")
		}
		want[row.pk] = row.ws
	}
	packer := types.NewPacker()
	defer packer.Close()
	matched := make(map[string]types.Rowid, len(rows))
	out := make(map[types.Rowid]types.Rowid, len(rows))
	for i := range stats {
		loc := stats[i].ObjectLocation()
		meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
		if err != nil {
			return nil, err
		}
		dataMeta := meta.MustGetMeta(objectio.SchemaData)
		objID := stats[i].ObjectName().ObjectId()
		for blkIdx := uint16(0); blkIdx < uint16(dataMeta.BlockCount()); blkIdx++ {
			ioVec, err := objectio.ReadOneBlock(
				ctx, &dataMeta, loc.Name().String(), blkIdx,
				[]uint16{uint16(seq)}, []types.Type{pkType}, mp, fs, fileservice.SkipAllCache)
			if err != nil {
				return nil, err
			}
			if len(ioVec.Entries) != 1 {
				ioVec.Release()
				return nil, moerr.NewInternalErrorNoCtx(
					"pk-move dump shadow: pk column read-back returned no entry")
			}
			obj, derr := objectio.DecodeCached(ioVec.Entries[0].CachedData)
			if derr != nil {
				ioVec.Release()
				return nil, derr
			}
			pkVec, ok := obj.(*vector.Vector)
			if !ok {
				ioVec.Release()
				return nil, moerr.NewInternalErrorNoCtx(
					"pk-move dump shadow: pk column read-back decoded to a non-vector")
			}
			blkID := objectio.NewBlockidWithObjectID(objID, blkIdx)
			for r := 0; r < pkVec.Length(); r++ {
				if pkVec.IsNull(uint64(r)) {
					continue
				}
				packer.Reset()
				if encErr := vector.EncodeRowToPacker(packer, pkVec, r); encErr != nil {
					ioVec.Release()
					return nil, encErr
				}
				key := string(packer.Bytes())
				ws, hit := want[key]
				if !hit {
					continue
				}
				if _, dup := matched[key]; dup {
					ioVec.Release()
					return nil, moerr.NewInternalErrorNoCtx(
						"pk-move dump shadow: object pk duplicates inside one dump wave")
				}
				matched[key] = types.NewRowid(&blkID, uint32(r))
				out[ws] = matched[key]
			}
			ioVec.Release()
		}
	}
	if len(matched) != len(want) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"pk-move dump shadow: %d of %d shadow rows missing from the written objects",
			len(want)-len(matched), len(want))
	}
	return out, nil
}

// translatePkMoveOriginDumpShadowLocked re-keys pkMoveRowidOrigins (and
// defensively its values and all pending undo bookkeeping) through the
// derived object identities of one just-dumped wave. The translation
// preserves the ledger exactly: one mapped entry per moved row, the same
// durable origin value, so a later statement's move still rewrites its
// old side to the identical durable generation — byte-identical to the
// lineage the same rows would have produced without the spill. The map
// is rebuilt from the untouched source because derived ids can collide
// with existing keys only through corruption; building a fresh map keeps
// the rewrite order-independent and loud. An admission refusal (the
// in-window journal append below) is returned so the dump fails loudly
// before any map mutates. Must be called with txn.Lock held.
func (txn *Transaction) translatePkMoveOriginDumpShadowLocked(m map[types.Rowid]types.Rowid) error {
	if len(m) == 0 || len(txn.pkMoveRowidOrigins) == 0 {
		return nil
	}
	// r35 review F1 (D139), re-homed by r38 review F1/F2 (D143/D144): a
	// boundary dump runs inside the maintenance window, where a later
	// RC-leg failure rolls the boundary back and the tail cut deletes the
	// spill identities this swap publishes while the raw batches keep
	// their pre-translation identities. Journal this swap's pairs in the
	// transaction-level carry journal BEFORE the remap — effective-only,
	// one immutable entry per round (D150, r43 review F2), and
	// budget-admitted under the shared participation rule (D151, r43
	// review F1) so an overflow refuses loudly before any map mutates:
	// RollbackLastStatement's walker applies the inverse in strict
	// reverse chronological order to the CURRENT maps, unwinding this
	// swap exactly while every later in-window contribution (the
	// transfer's completed carry above all) survives untouched.
	// Mid-statement WriteBatch spills open no window and keep their
	// statement-undo restoration (the pending undo records below ride
	// the same translation), so they journal nothing.
	if txn.maintWindow != nil {
		oldIds := make([]types.Rowid, 0, len(m))
		newIds := make([]types.Rowid, 0, len(m))
		for oldID, newID := range m {
			oldIds = append(oldIds, oldID)
			newIds = append(newIds, newID)
		}
		effOld, effNew, admErr := txn.pkMoveCarryJournalAdmitLocked(oldIds, newIds)
		if admErr != nil {
			return admErr
		}
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryDump, effOld, effNew)
	}
	remapped := make(map[types.Rowid]types.Rowid, len(txn.pkMoveRowidOrigins))
	for k, v := range txn.pkMoveRowidOrigins {
		nk, kok := m[k]
		nv, vok := m[v]
		switch {
		case kok && vok:
			remapped[nk] = nv
		case kok:
			remapped[nk] = v
		case vok:
			remapped[k] = nv
		default:
			remapped[k] = v
		}
	}
	txn.pkMoveRowidOrigins = remapped
	// Pending undo records and their first-record-wins set are keyed by
	// the same identities: a mid-statement dump (the WriteBatch-triggered
	// spill) can precede RollbackLastStatement, so the rollback records
	// must fire on the translated keys to restore the same logical state.
	for i := range txn.pkMoveOriginUndo {
		if nv, ok := m[txn.pkMoveOriginUndo[i].key]; ok {
			txn.pkMoveOriginUndo[i].key = nv
		}
		if nv, ok := m[txn.pkMoveOriginUndo[i].prev]; ok {
			txn.pkMoveOriginUndo[i].prev = nv
		}
	}
	if len(txn.pkMoveOriginUndoSeen) != 0 {
		seen := make(map[types.Rowid]struct{}, len(txn.pkMoveOriginUndoSeen))
		for k := range txn.pkMoveOriginUndoSeen {
			if nv, ok := m[k]; ok {
				seen[nv] = struct{}{}
			} else {
				seen[k] = struct{}{}
			}
		}
		txn.pkMoveOriginUndoSeen = seen
	}
	return nil
}

// recordPkMoveRewriteShadow parallels recordPkMoveDumpShadowLocked for
// the deletion-driven CN object rewrite in compactDeletionOnObjsLocked:
// rewriteObjectByDeletion re-mints the row id of every row surviving a
// dirty object's rewrite, so provenance naming a pre-rewrite identity
// would dangle exactly like an unshadowed dump (r25 review F2). For
// every block of the original object this reads the pk column, skips
// the offsets the pending deletions remove, and shadows the survivors
// the relevance set names; resolvePkMoveDumpShadow then joins them
// against the rewritten object by pk value. Rows the bookkeeping never
// named cost one pk-column read per dirty object and nothing else.
// Lock-free: the caller's workers run without the txn lock, so the
// mpool and the read filesystem are explicit.
func recordPkMoveRewriteShadow(
	ctx context.Context,
	mp *mpool.MPool,
	fs fileservice.FileService,
	origStats objectio.ObjectStats,
	tableDef *pbplan.TableDef,
	deletions map[objectio.Blockid][]int64,
	relevant map[types.Rowid]struct{},
) ([]pkMoveDumpShadowRow, error) {
	seq, pkType, hasKey, found := pkMoveShadowPkColumn(tableDef)
	if !hasKey {
		return nil, moerr.NewInternalErrorNoCtx(
			"pk-move rewrite shadow: dirty object of a table without a primary key")
	}
	if !found {
		return nil, moerr.NewInternalErrorNoCtxf(
			"pk-move rewrite shadow: pk column %q missing from the table definition",
			tableDef.Pkey.PkeyColName)
	}
	loc := origStats.ObjectLocation()
	meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
	if err != nil {
		return nil, err
	}
	dataMeta := meta.MustGetMeta(objectio.SchemaData)
	objID := origStats.ObjectName().ObjectId()
	packer := types.NewPacker()
	defer packer.Close()
	var rows []pkMoveDumpShadowRow
	for blkIdx := uint16(0); blkIdx < uint16(dataMeta.BlockCount()); blkIdx++ {
		blkID := objectio.NewBlockidWithObjectID(objID, blkIdx)
		var deleted map[int64]struct{}
		for _, off := range deletions[blkID] {
			if deleted == nil {
				deleted = make(map[int64]struct{}, 4)
			}
			deleted[off] = struct{}{}
		}
		ioVec, err := objectio.ReadOneBlock(
			ctx, &dataMeta, loc.Name().String(), blkIdx,
			[]uint16{seq}, []types.Type{pkType}, mp, fs, fileservice.SkipAllCache)
		if err != nil {
			return nil, err
		}
		if len(ioVec.Entries) != 1 {
			ioVec.Release()
			return nil, moerr.NewInternalErrorNoCtx(
				"pk-move rewrite shadow: pk column read returned no entry")
		}
		obj, derr := objectio.DecodeCached(ioVec.Entries[0].CachedData)
		if derr != nil {
			ioVec.Release()
			return nil, derr
		}
		pkVec, ok := obj.(*vector.Vector)
		if !ok {
			ioVec.Release()
			return nil, moerr.NewInternalErrorNoCtx(
				"pk-move rewrite shadow: pk column read decoded to a non-vector")
		}
		for r := 0; r < pkVec.Length(); r++ {
			if pkVec.IsNull(uint64(r)) {
				continue
			}
			if _, gone := deleted[int64(r)]; gone {
				continue
			}
			oldID := types.NewRowid(&blkID, uint32(r))
			if _, named := relevant[oldID]; !named {
				continue
			}
			packer.Reset()
			if encErr := vector.EncodeRowToPacker(packer, pkVec, r); encErr != nil {
				ioVec.Release()
				return nil, encErr
			}
			rows = append(rows, pkMoveDumpShadowRow{
				pk: string(packer.Bytes()),
				ws: oldID,
			})
		}
		ioVec.Release()
	}
	return rows, nil
}

// pkMoveRewriteMigration accumulates the (old -> new) row-id pairs the
// rewrite of one or more dirty objects produced for rows the pk-move
// bookkeeping names. The workers of compactDeletionOnObjsLocked append
// under the local locker; the parent applies ONE bookkeeping remap
// after they finish (r25 review F2).
type pkMoveRewriteMigration struct {
	oldIds []types.Rowid
	newIds []types.Rowid
}

func (m *pkMoveRewriteMigration) add(pairs map[types.Rowid]types.Rowid) {
	for oldID, newID := range pairs {
		m.oldIds = append(m.oldIds, oldID)
		m.newIds = append(m.newIds, newID)
	}
}

// pkMoveCarryKind classifies one carry-journal entry's provenance re-key
// class (r38 review F1, D143): the boundary dump's raw -> spill
// translations (r35 review F1, D139), the dirty-object compaction wave's
// migration pairs (r36 review F1, D141), and the object-route tombstone
// transfer's re-mint rounds (r37 review F1, D142). Every kind unwinds
// through the same forward engine with swapped pair lists; the kind only
// labels the entry — dump and wave entries exist inside boundary-
// maintenance windows, object entries on the window and AdvanceSnapshot
// routes alike. The in-memory transfer leg and the merge/shrink in-place
// re-keys have no kind: their re-minted identities live in retained
// workspace batches that outlast any rollback (sheltered, r26/D110), so
// they journal nothing.
type pkMoveCarryKind int

const (
	pkMoveCarryDump pkMoveCarryKind = iota + 1
	pkMoveCarryWave
	pkMoveCarryObject
)

// pkMoveCarryJournalEntry records one admitted provenance carry round for
// statement rollback: seq stamps txn.statementID of the carrying
// statement — the same clock retirement records stamp (r26/D113) — so one
// statement-seq discipline owns publications, retirements, and provenance
// carries alike; oldIds[i] re-keyed to newIds[i] under txn.Lock at carry
// time. Retention is effective-only (r38 review F2, D144): a pair whose
// old identity references no provenance artifact is a definitional no-op
// in remapPkMoveLinkRowIDsLocked and is never journaled — the mutation
// still receives the full pair list (a no-op pair is one map probe).
type pkMoveCarryJournalEntry struct {
	seq    int
	kind   pkMoveCarryKind
	oldIds []types.Rowid
	newIds []types.Rowid
}

// pkMoveProvenanceReferenceRowIDsLocked computes the transaction's
// complete provenance reference set for journal admission (r38 review F2,
// D144): every identity remapPkMoveLinkRowIDsLocked can rewrite — the
// keys and values of the durable-origin ledger, the null-heal targets,
// the lineage-binding origins, and the row ids staged into the link
// table's old_row and new_row cells. A carried pair whose OLD identity is
// absent moves nothing, so journaling it would only grow the journal with
// zero undo value. The same helper backs admission and the charge
// estimate, so the filter cannot drift from the engine's actual effect.
// Must be called with txn.Lock held.
func (txn *Transaction) pkMoveProvenanceReferenceRowIDsLocked() map[types.Rowid]struct{} {
	ref := txn.pkMoveRewriteMigrationRelevantLocked()
	for _, set := range txn.pkMoveKeyLineage {
		for _, stack := range set {
			for i := range stack {
				if ref == nil {
					ref = make(map[types.Rowid]struct{})
				}
				ref[stack[i].origin] = struct{}{}
			}
		}
	}
	return ref
}

// pkMoveCarryJournalAdmitLocked computes the round's EFFECTIVE carry
// pairs and refuses — loudly and statement-atomically, before any map,
// staged cell, or lineage stack mutates — when the journal's growth
// would cross the coordinator budget (r38 review F2, D144; the complete
// retained growth is exactly the journal charge under r40 review F2's
// D148 byte-neutral retained-cell overwrite, so journal admission IS the
// complete admission, r39 review F2's G = J + A + R with A = R = 0).
// Pair-list integrity is asserted before anything else: unequal lengths
// or a non-injective old side mis-unwinds later, so it fails the
// enclosing statement here. Under D150 (r43 review F2) there is NO
// coalescing — every effective pair rides its own per-round entry — so
// the charge equals the real append growth exactly. The budget refusal
// applies the shared participation rule (D151, r43 review F1): it fires
// only while the transaction holds live staged link rows; a marker-only
// transaction still runs the integrity checks, the effective-pair
// computation, and the journal append, but is never refused — its
// bookkeeping dies with the transaction and commits no provenance.
// A zero budget disables the check (existing convention), and the
// inverse never re-admits (the overwrite measures zero, so the reserve
// degenerates to the never-fail discipline).
func (txn *Transaction) pkMoveCarryJournalAdmitLocked(
	oldIds, newIds []types.Rowid,
) (effOld, effNew []types.Rowid, err error) {
	if len(oldIds) != len(newIds) {
		return nil, nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"pk-move carry journal admission: pair-list length mismatch (%d old vs %d new)",
			len(oldIds), len(newIds)))
	}
	if len(oldIds) == 0 {
		return nil, nil, nil
	}
	seen := make(map[types.Rowid]struct{}, len(oldIds))
	for i := range oldIds {
		if _, dup := seen[oldIds[i]]; dup {
			return nil, nil, moerr.NewInternalErrorNoCtx(
				"pk-move carry journal admission: old side is not injective")
		}
		seen[oldIds[i]] = struct{}{}
	}
	ref := txn.pkMoveProvenanceReferenceRowIDsLocked()
	effOld = make([]types.Rowid, 0, len(oldIds))
	effNew = make([]types.Rowid, 0, len(newIds))
	for i := range oldIds {
		if _, ok := ref[oldIds[i]]; ok {
			effOld = append(effOld, oldIds[i])
			effNew = append(effNew, newIds[i])
		}
	}
	if len(effOld) == 0 {
		return effOld, effNew, nil
	}
	// D150: charge what the append below will actually grow — one
	// per-round entry (immutable once written) plus every effective
	// pair's two row-id cells. charge == real growth.
	delta := int64(len(effOld))*(2*types.RowidSize+16) + pkMoveBookkeepEntryOverhead
	if pkMoveLinkCoordinatorBudget > 0 && txn.pkMoveHasLiveStagedLinksLocked() {
		staged := txn.pkMoveLinkStagedBytes
		bookkeep := txn.pkMoveBookkeepBytesLocked() + delta
		pinBytes := txn.pkMovePinnedRetainedBytesLocked(
			txn.pkMoveStagedLinkPinnedRowIDsLocked())
		if staged+bookkeep+pinBytes > pkMoveLinkCoordinatorBudget {
			return nil, nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"data branch update provenance exceeds the coordinator retention budget: retained link bytes %d plus bookkeeping bytes %d (carry journal growth %d) plus pin-retained workspace bytes %d over budget %d bytes; reduce the number of primary keys moved per statement",
				staged, bookkeep, delta, pinBytes, pkMoveLinkCoordinatorBudget))
		}
	}
	return effOld, effNew, nil
}

// pkMoveCarryJournalAppendLocked journals the admitted effective pairs
// under the CURRENT statement's seq after pkMoveCarryJournalAdmitLocked
// approved them, BEFORE the carry mutates any map or staged cell. D150
// (r43 review F2): every round appends its OWN chronological entry —
// stamped with the current statement's seq and immutable once written —
// with NO composition ever. A chained pair (its old identity minted by
// an earlier round of this transaction) names the intermediate identity
// exactly, so the stepwise unwind passes a byte-exact intermediate
// checkpoint and restores every intermediate state rather than hoping
// one composed pair can invert a partially mutated chain. The admitted
// slices are defensively copied so no later caller mutation can reach an
// already journaled (immutable) entry. Must be called with txn.Lock
// held, in the same critical section as the admission so the projected
// charge equals the real growth.
func (txn *Transaction) pkMoveCarryJournalAppendLocked(
	kind pkMoveCarryKind, effOld, effNew []types.Rowid,
) {
	if len(effOld) == 0 {
		return
	}
	txn.pkMoveCarryJournal = append(txn.pkMoveCarryJournal,
		pkMoveCarryJournalEntry{
			seq:    txn.statementID,
			kind:   kind,
			oldIds: append([]types.Rowid(nil), effOld...),
			newIds: append([]types.Rowid(nil), effNew...),
		})
}

// unwindPkMoveCarryJournalLocked rolls every carry the rolled statement
// journaled back out of the provenance maps and staged link cells
// (r38 review F1, D143): entries stamped at or after rolledStmt unwind in
// strict reverse chronological order through the forward engine with
// swapped pair lists (the injective dead=nil re-key is exactly invertible,
// r37 review F1, D142), and each consumed entry leaves the journal. The
// inverse never re-admits and never fails the budget (D146/D148): the
// retained-cell overwrite measures zero, so the pre-authorization
// degenerates to this never-fail walk. An entry with a selected stamp
// outside the selected chronological tail is an ordering violation that
// surfaces loudly instead of silently skipping a carry. Must be called
// with txn.Lock held, after the positional tail cut and before the
// statement-undo replay.
func (txn *Transaction) unwindPkMoveCarryJournalLocked(rolledStmt int) (err error) {
	n := len(txn.pkMoveCarryJournal)
	if n == 0 {
		return nil
	}
	first := n
	for first > 0 && txn.pkMoveCarryJournal[first-1].seq >= rolledStmt {
		first--
	}
	for i := n - 1; i >= first; i-- {
		e := &txn.pkMoveCarryJournal[i]
		if werr := txn.remapPkMoveLinkRowIDsLocked(e.newIds, e.oldIds, nil); werr != nil {
			err = errors.Join(err, werr)
		}
		txn.pkMoveCarryJournal[i] = pkMoveCarryJournalEntry{}
	}
	for i := 0; i < first; i++ {
		if txn.pkMoveCarryJournal[i].seq >= rolledStmt {
			err = errors.Join(err, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"pk-move carry journal ordering violation: entry %d stamped %d precedes the rolled statement %d",
				i, txn.pkMoveCarryJournal[i].seq, rolledStmt)))
		}
	}
	txn.pkMoveCarryJournal = txn.pkMoveCarryJournal[:first]
	return err
}

// settlePkMoveAdoptedLedgersLocked runs the ONE adoption walk settling
// both roll-forward ledgers against the statement seq the widened cut
// just made irrevocable (r39 review F1, D145; r40 review F1, D147):
// retirement records stamped at or below it become the settled prefix of
// txn.retiredTransfers — deposit order keeps them consecutive, and a
// record beyond the new boundary stamped at or below it is an ordering
// inversion reported loudly — and the carry journal drops the same seqs'
// entries (the widened cut guarantees no later rollback's cut end can
// reach their identities; eligibility is ledger MEMBERSHIP, never numeric
// seq alone, because the ensuing statement re-issues the same numeric seq
// for its fresh work). Settlement changes no stamp, no >= test, and no
// cut arithmetic, and the close drain still enumerates the whole
// retirement collection. Runs on every statement boundary — window or
// not — because an AdvanceSnapshot transfer carries and retires with no
// window at all. Must be called with txn.Lock held.
func (txn *Transaction) settlePkMoveAdoptedLedgersLocked() error {
	adopted := txn.statementID
	i := txn.retiredTransfersSettled
	for i < len(txn.retiredTransfers) && txn.retiredTransfers[i].statementSeq <= adopted {
		i++
	}
	for j := i; j < len(txn.retiredTransfers); j++ {
		if txn.retiredTransfers[j].statementSeq <= adopted {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"pk-move retirement settlement ordering violation: record %d stamped %d sits beyond the adopted boundary %d",
				j, txn.retiredTransfers[j].statementSeq, adopted))
		}
	}
	txn.retiredTransfersSettled = i
	if len(txn.pkMoveCarryJournal) != 0 {
		var kept []pkMoveCarryJournalEntry
		for k := range txn.pkMoveCarryJournal {
			if txn.pkMoveCarryJournal[k].seq > adopted {
				kept = append(kept, txn.pkMoveCarryJournal[k])
			}
		}
		txn.pkMoveCarryJournal = kept
	}
	return nil
}

// carryPkMoveProvenanceThroughRekeyLocked carries every pk-move
// bookkeeping structure through one structural row-id re-key that
// already rewrote the user-side cells: staged link row-id cells,
// durable-origin keys and values, and null-heal targets re-key onto the
// replacement identities. Two re-key engines ride it: the deletion-
// driven CN object rewrite, which re-mints the row ids of a dirty
// object's survivors (r25 review F2), and the RC tombstone transfer,
// which re-mints the txn's pending delete tombstones onto the storage
// generation the commit will actually publish — the staged link's
// old_row_id must name the transferred identity byte-for-byte or the
// consumer's exact-match claim misses and replays DELETE/INSERT (r25
// review F3). dead is nil for both engines: the rewrite shadows only
// survivors (a removed row's bygone identity is unreferenceable later),
// and a transfer target is guaranteed live on the new generation. Every
// caller reaches this with the per-statement pk-move undo logs already
// reset (IncrStatementID nils them before its merge/transfer work, the
// commit path likewise), so unlike the dump translation there are no
// rollback records to re-key. One epoch bump covers the whole wave.
// Must be called with txn.Lock held.
// pkMoveTransferRekeyPairs joins a persisted-tombstone transfer's staged
// (old) batch against its read-back result batch by pk VALUE (r26 review
// F3 / D110): doTransferRowids emits result rows in object READ order, not
// staged order, so positional pairing is invalid. Vecs[0] are the row ids
// and Vecs[1] the pks on both sides. Only pairs whose identity actually
// changed are returned, and ambiguity fails closed: a count mismatch, a
// staged pk missing from the result, or a pk duplicated on the read-back
// side means the object and the workspace disagree about the transferred
// rows — publishing re-minted tombstones with severed provenance is worse
// than aborting the transfer (and with it the enclosing statement/commit).
func pkMoveTransferRekeyPairs(
	staged, result *batch.Batch,
) ([]types.Rowid, []types.Rowid, error) {
	stagedRows := staged.RowCount()
	if stagedRows == 0 {
		return nil, nil, nil
	}
	if len(staged.Vecs) < 2 || len(result.Vecs) < 2 {
		return nil, nil, moerr.NewInternalErrorNoCtx(
			"pk-move transfer rekey: transfer batches lack rowid or pk vectors")
	}
	if result.RowCount() != stagedRows {
		return nil, nil, moerr.NewInternalErrorNoCtxf(
			"pk-move transfer rekey: %d staged rows read back as %d",
			stagedRows, result.RowCount())
	}
	packer := types.NewPacker()
	defer packer.Close()

	// Index the read-back side by pk; duplicates are ambiguous by
	// construction of tombstone transfer (one row, one departure, and the
	// injective remap in remapPkMoveLinkRowIDsLocked relies on it).
	type slot struct {
		rowid types.Rowid
		dup   bool
	}
	byPK := make(map[string]slot, stagedRows)
	resIDs := result.Vecs[0]
	resPKs := result.Vecs[1]
	for r := 0; r < result.RowCount() && r < resIDs.Length() && r < resPKs.Length(); r++ {
		packer.Reset()
		if err := vector.EncodeRowToPacker(packer, resPKs, r); err != nil {
			return nil, nil, err
		}
		k := string(packer.Bytes())
		if s, ok := byPK[k]; ok {
			s.dup = true
			byPK[k] = s
			continue
		}
		byPK[k] = slot{rowid: vector.GetFixedAtNoTypeCheck[types.Rowid](resIDs, r)}
	}

	oldIDs := staged.Vecs[0]
	oldPKs := staged.Vecs[1]
	var oldList, newList []types.Rowid
	for r := 0; r < stagedRows && r < oldIDs.Length() && r < oldPKs.Length(); r++ {
		packer.Reset()
		if err := vector.EncodeRowToPacker(packer, oldPKs, r); err != nil {
			return nil, nil, err
		}
		s, ok := byPK[string(packer.Bytes())]
		if !ok {
			return nil, nil, moerr.NewInternalErrorNoCtxf(
				"pk-move transfer rekey: staged row %d's pk is missing from the read-back object", r)
		}
		if s.dup {
			return nil, nil, moerr.NewInternalErrorNoCtxf(
				"pk-move transfer rekey: staged row %d's pk is ambiguous in the read-back object", r)
		}
		old := vector.GetFixedAtNoTypeCheck[types.Rowid](oldIDs, r)
		if old == s.rowid {
			continue
		}
		oldList = append(oldList, old)
		newList = append(newList, s.rowid)
	}
	return oldList, newList, nil
}

func (txn *Transaction) carryPkMoveProvenanceThroughRekeyLocked(oldIds, newIds []types.Rowid) error {
	if len(oldIds) == 0 {
		return nil
	}
	if err := txn.remapPkMoveLinkRowIDsLocked(oldIds, newIds, nil); err != nil {
		return err
	}
	txn.pkMoveHarvestGen++
	return nil
}

// holdsPkMoveProvenanceLocked reports whether this transaction carries any
// pk-move provenance state an object-route tombstone transfer must
// preserve. Single source for the D110 carry gate and the D113
// pass-through/retirement gate; transfer paths reach it with txn.Lock held
// (inside forEachTableHasDeletesLocked callbacks).
func (txn *Transaction) holdsPkMoveProvenanceLocked() bool {
	return len(txn.pkMoveRowidOrigins) != 0 ||
		len(txn.pkMoveNullHeals) != 0 ||
		len(txn.pkMoveKeyLineage) != 0 ||
		len(txn.pkMoveLinkTables) != 0
}

// retireTransferredTombstoneStatsLocked retires the persisted tombstone
// objects a pass-through object-route transfer just replaced (r26 review F3,
// D113). The replacement objects it published carry every input row — rows
// whose target object survived travel verbatim, the rest re-keyed — so the
// superseded source entries naming them must never publish: at a pk-move
// anchor key the twin tombstone breaks the chain evidence's uniqueness and
// the data-branch apply fails closed unresolved, rejecting an update the
// source committed.
//
// Matching is by exact object name against this table's file-backed DELETE
// entries in txn.writes. A fully covered entry releases its slot in place
// (bat = nil: the established nil-slot convention keeps slice indices, and
// therefore the statement offsets, stable); a partially covered one shrinks
// to its un-retired stats. Retired stats go into a retiredTransferRecord so
// RollbackLastStatement can resurrect the entries whose replacement
// publications the rolled-back region discarded, and delTransaction can
// gc-collect files the commit never publishes. ObjectStats is a fixed-size
// array type: the slice-to-array conversion above already deep-copied the
// bytes out of the batch memory the release/shrink invalidates. Must be
// called with txn.Lock held.
func (txn *Transaction) retireTransferredTombstoneStatsLocked(
	tbl *txnTable, inputs []objectio.ObjectStats,
) (err error) {
	if len(inputs) == 0 {
		return nil
	}
	// r32 review F2 (D133): book cleanup ownership BEFORE this walk
	// retires any tombstone file into unpublished garbage. The input
	// count upper-bounds the unique retired names (each input names one
	// file); the defer below releases the slack for inputs no delete
	// entry actually retires, and the close drain's registration
	// converts the rest name-for-name. Exhaustion fails the boundary
	// before any orphan is produced.
	if err := reserveDurableOrphanOwnership(txn.engine.fs, len(inputs)); err != nil {
		return err
	}
	retiredNames := make(map[objectio.ObjectNameShort]struct{}, len(inputs))
	for i := range inputs {
		retiredNames[*inputs[i].ObjectShortName()] = struct{}{}
	}
	record := &retiredTransferRecord{statementSeq: txn.statementID}
	defer func() {
		// Deposit even on a mid-walk error: the slots already released or
		// shrunk are unpublished garbage either way, and the record is the
		// only place their files can be gc-collected from at close.
		if len(record.entries) != 0 {
			txn.retiredTransfers = append(txn.retiredTransfers, *record)
		}
		// D133 slack release: inputs this walk retired nowhere need no
		// outstanding reservation (only unique retired file names remain
		// booked, exactly the inventory the close drain hands to the
		// registry's name-for-name conversion).
		retired := make(map[string]struct{}, len(inputs))
		for i := range record.entries {
			for j := range record.entries[i].stats {
				retired[record.entries[i].stats[j].ObjectName().String()] = struct{}{}
			}
		}
		releaseDurableOrphanReservation(txn.engine.fs, len(inputs)-len(retired))
	}()
	for idx := range txn.writes {
		e := &txn.writes[idx]
		if e.typ != DELETE || e.bat == nil || e.bat.IsEmpty() ||
			e.fileName == "" ||
			e.tableId != tbl.tableId || e.databaseId != tbl.db.databaseId {
			continue
		}
		col, area := vector.MustVarlenaRawData(e.bat.Vecs[0])
		kept := make([]objectio.ObjectStats, 0, len(col))
		retired := make([]objectio.ObjectStats, 0, len(col))
		for _, c := range col {
			ss := objectio.ObjectStats(c.GetByteSlice(area))
			if _, hit := retiredNames[*ss.ObjectShortName()]; hit {
				retired = append(retired, ss)
			} else {
				kept = append(kept, ss)
			}
		}
		if len(retired) == 0 {
			continue
		}
		// Prepare the retained batch BEFORE any slot, stash, or journal
		// mutation (r27 review F3 / D116): a mid-shrink allocation failure
		// must leave the entry exactly as it was — retired stats still only
		// in the original slot, never also in the rollback ledger. The
		// ledger records a detachment only once it has actually completed;
		// otherwise statement rollback would resurrect the stats beside
		// the still-intact original (two publications of one identity).
		var shrunk *batch.Batch
		if len(kept) != 0 {
			var err2 error
			shrunk, err2 = buildPkMoveRetireShrunkBatch(kept, txn.proc.Mp())
			if err2 != nil {
				return err2
			}
		}
		for i := range retired {
			txn.cn_flushed_s3_tombstone_object_stats_list.Delete(retired[i])
		}
		record.entries = append(record.entries, retiredTransferEntry{
			accountId:          e.accountId,
			databaseId:         e.databaseId,
			tableId:            e.tableId,
			databaseName:       e.databaseName,
			tableName:          e.tableName,
			fileName:           e.fileName,
			note:               e.note,
			tnStore:            e.tnStore,
			pkChkByTN:          e.pkChkByTN,
			pkCheckPos:         e.pkCheckPos,
			pkCheckReady:       e.pkCheckReady,
			pkMovePkPos:        e.pkMovePkPos,
			skipTransfer:       e.skipTransfer,
			autoIncrEpoch:      e.autoIncrEpoch,
			autoIncrEpochKnown: e.autoIncrEpochKnown,
			stats:              retired,
		})
		if len(kept) == 0 {
			txn.releaseWorkspaceEntryBatchLocked(idx)
			continue
		}
		txn.unaccountWorkspaceEntryLocked(e)
		delete(txn.batchSelectList, e.bat)
		e.bat.Clean(txn.proc.Mp())
		e.bat = shrunk
		txn.accountWorkspaceEntryLocked(e)
	}
	txn.assertWorkspaceAccountingLocked()
	return nil
}

// buildPkMoveRetireShrunkBatch materializes the retained-stat batch for a
// partially covered retirement. Rides the package's vector-op seams so the
// allocation-failure contracts can be pinned deterministically in tests.
func buildPkMoveRetireShrunkBatch(
	kept []objectio.ObjectStats, mp *mpool.MPool,
) (*batch.Batch, error) {
	shrunk := colexec.AllocCNS3ResultBat(true)
	if err := shrunk.PreExtend(mp, len(kept)); err != nil {
		shrunk.Clean(mp)
		return nil, err
	}
	for i := range kept {
		if err := pkMoveVecAppendBytes(
			shrunk.Vecs[0], kept[i].Marshal(), false, mp); err != nil {
			shrunk.Clean(mp)
			return nil, err
		}
	}
	shrunk.SetRowCount(shrunk.Vecs[0].Length())
	return shrunk, nil
}

// resurrectRetiredTransferEntriesLocked re-appends the tombstone entries
// whose transfer-round retirements ran under statements the tail cut just
// discarded (D113). rolledStmt is the rolled-back statement id: retirement
// records with statementSeq >= rolledStmt lost their replacement
// publications to the cut, so their superseded originals must publish
// again — byte-for-byte the entries the retirement removed. Resurrecting
// under the current tail region matches the workspace discipline transfer
// rounds already use for their replacement entries.
//
// r33 review F2 (D136): every fully resurrected entry returns its files
// to publishing workspace ownership, so the retirement's D133 reservation
// for each unique file name is released here exactly once — the files are
// no longer orphanable. Entries still retired after a mid-walk failure
// keep their reservations in the surviving records for the close drain.
// Must be called with txn.Lock held.
func (txn *Transaction) resurrectRetiredTransferEntriesLocked(rolledStmt int) (err error) {
	// r39 review F1 (D145, refined by r40 review F1, D147): resurrection
	// walks ONLY the active tail beyond the settled prefix. The prefix's
	// replacements survived every possible later tail cut (the boundary's
	// widened offsets entry covers them), so passing over it is normal —
	// resurrecting it would duplicate a live tombstone — while records
	// minted by the rolled statement or by a boundary attempt that never
	// reached adoption stay active and keep their seq >= rolledStmt
	// selection. The close drain still enumerates prefix plus tail.
	settled := txn.retiredTransfersSettled
	if settled > len(txn.retiredTransfers) {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"pk-move resurrection: settled prefix %d exceeds the retirement ledger length %d",
			settled, len(txn.retiredTransfers)))
	}
	if settled == len(txn.retiredTransfers) {
		return nil
	}
	kept := make([]retiredTransferRecord, 0, len(txn.retiredTransfers))
	kept = append(kept, txn.retiredTransfers[:settled]...)
	// D136: unique file names whose entries returned to the workspace —
	// each name's outstanding reservation settles exactly once here, with
	// the retirement's own unique-name discipline.
	resurrected := make(map[string]struct{})
	// Preserve the not-yet-resurrected tail on a mid-walk failure: like the
	// retirement's deposit-on-error discipline, the surviving records are the
	// only place close-time gc can collect those files from — and their
	// reservations stay booked for exactly that settlement. The already
	// resurrected names settle on every exit.
	defer func() {
		txn.retiredTransfers = kept
		if len(resurrected) > 0 {
			releaseDurableOrphanReservation(txn.engine.fs, len(resurrected))
		}
	}()
	for i := settled; i < len(txn.retiredTransfers); i++ {
		rec := &txn.retiredTransfers[i]
		if rec.statementSeq < rolledStmt {
			kept = append(kept, *rec)
			continue
		}
		for j := range rec.entries {
			re := &rec.entries[j]
			bat := colexec.AllocCNS3ResultBat(true)
			if err = bat.PreExtend(txn.proc.Mp(), len(re.stats)); err != nil {
				bat.Clean(txn.proc.Mp())
				rec.entries = rec.entries[j:]
				kept = append(kept, txn.retiredTransfers[i:]...)
				return err
			}
			for k := range re.stats {
				ss := re.stats[k]
				if err = pkMoveVecAppendBytes(
					bat.Vecs[0], ss.Marshal(), false, txn.proc.Mp()); err != nil {
					bat.Clean(txn.proc.Mp())
					rec.entries = rec.entries[j:]
					kept = append(kept, txn.retiredTransfers[i:]...)
					return err
				}
			}
			bat.SetRowCount(bat.Vecs[0].Length())
			for k := range re.stats {
				txn.StashFlushedTombstones(re.stats[k])
			}
			txn.appendWorkspaceEntryLocked(Entry{
				typ:                DELETE,
				accountId:          re.accountId,
				databaseId:         re.databaseId,
				tableId:            re.tableId,
				databaseName:       re.databaseName,
				tableName:          re.tableName,
				fileName:           re.fileName,
				note:               re.note,
				bat:                bat,
				tnStore:            re.tnStore,
				pkChkByTN:          re.pkChkByTN,
				pkCheckPos:         re.pkCheckPos,
				pkCheckReady:       re.pkCheckReady,
				pkMovePkPos:        re.pkMovePkPos,
				skipTransfer:       re.skipTransfer,
				autoIncrEpoch:      re.autoIncrEpoch,
				autoIncrEpochKnown: re.autoIncrEpochKnown,
			})
			// D136: the entry is fully back in the workspace — its files
			// publish again, so their retirement reservations settle here.
			for k := range re.stats {
				resurrected[re.stats[k].ObjectName().String()] = struct{}{}
			}
		}
	}
	txn.assertWorkspaceAccountingLocked()
	return nil
}

// gcRetiredTransferFiles deletes the object files behind the surviving
// retired-transfer records at transaction close. Retired entries never
// appear in commit serialization, so their files are unpublished garbage in
// EVERY close outcome — committed, rolled back, or unknown-result alike —
// and skipping them leaks S3 objects the TN never heard about. Runs at the
// quiescent close like its caller delTransaction. Ownership transfers to
// the durable orphan registry before the fallible reclamation attempt and
// is released only on confirmed deletion (r30 review F3, D128), so a
// pool/admission failure or the sweep's storage error never leaves these
// files ownerless after close.
func (txn *Transaction) gcRetiredTransferFiles() {
	if len(txn.retiredTransfers) == 0 {
		return
	}
	names := make([]string, 0, len(txn.retiredTransfers))
	for i := range txn.retiredTransfers {
		for _, re := range txn.retiredTransfers[i].entries {
			for j := range re.stats {
				names = append(names, re.stats[j].ObjectName().String())
			}
		}
	}
	txn.retiredTransfers = nil
	txn.retiredTransfersSettled = 0
	// CCPR entries never enter a pass-through transfer (skipTransfer marks
	// them and the provenance gate is unreachable), so this guard mirrors the
	// Rollback GC exemption on principle, not for a reachable case.
	if txn.isCCPRTxn {
		// D133: the delegated CCPR cleanup never hands these files to the
		// registry, so their outstanding reservations return unused.
		releaseDurableOrphanReservation(txn.engine.fs, len(names))
		return
	}
	// r30 review F3 (D128): ownership transfers to the durable orphan
	// registry BEFORE the fallible reclamation attempt, and is released
	// only by a confirmed deletion — a pool/admission failure or the
	// sweep's storage error retains it for the piggyback sweep of later
	// GC traffic instead of leaving these unpublished files ownerless.
	// The clone-txn shared-file exemption mirrors the gcFiles filter this
	// call replaced (cloneGCTxnRollback scope).
	if txn.isCloneTxn {
		before := len(names)
		names = readutil.RemoveIf(names, func(name string) bool {
			return txn.engine.cloneTxnCache.IsSharedFile(txn.op.Txn().ID, name)
		})
		// D133: clone-shared files stay owned by their origin lifecycle —
		// this txn's reservations for them return unused.
		releaseDurableOrphanReservation(txn.engine.fs, before-len(names))
	}
	registerDurableOrphanGC(txn.engine.fs, names...)
	retryDurableOrphanGC(txn.engine)
}

// reclaimUnregisteredDumpLocked owns the dump's persisted output for the
// whole interval between a successful Sync and the workspace registration
// that adopts it (r34 review F2, D138): any failure inside that interval
// — the provenance shadow's read-back, FillBlockInfoBat, or the
// registration call itself — exits with the files on disk owned by
// nothing (the rollback tail-cut knows nothing of them since no entry
// was appended, and the deferred writer Close releases memory only). It
// mirrors cleanFailedWave: a cancellation-independent
// DeleteUnpublishedObjects through the same filesystem the writer
// persisted with attempts immediate reclamation; names that attempt
// cannot reconcile are retained in txn.failedWaveOrphans for the
// transaction close's GC drain under D133/D135's exactly-once
// settlement. r35 review F2 (D140): every name's cleanup ownership was
// booked BEFORE its first byte persisted — the dump writer's
// unpublished-name observer books per object at the persistence choke
// point — so the terminals reconcile exactly: a confirmed reclaim
// releases the bookings it made redundant, and retention CARRIES the
// bookings with the names, making the close drain's conversion always
// reconciled name-for-name (never anonymous and never against a peer
// producer's reservations).
// DeleteUnpublishedObjects tolerates partial progress and missing names,
// so retaining the whole batch after an error is safe even if the
// attempt stopped partway through it. Called with txn.Lock held, exactly
// like the wave cleanup.
func (txn *Transaction) reclaimUnregisteredDumpLocked(
	fs fileservice.FileService,
	stats []objectio.ObjectStats,
) {
	if len(stats) == 0 {
		return
	}
	orphans := make([]string, 0, len(stats))
	for i := range stats {
		orphans = append(orphans, stats[i].ObjectName().String())
	}
	// Cleanup must survive query cancellation, mirroring cleanFailedWave.
	_, dErr := ioutil.DeleteUnpublishedObjects(context.Background(), fs, orphans...)
	if dErr == nil {
		// D140: reclaim reconciled — the pre-placed admission bookings
		// return unused, every name gone before the error surfaced.
		releaseDurableOrphanReservation(txn.engine.fs, len(orphans))
		return
	}
	// D140: retention keeps the admission bookings the observer placed
	// before persistence; no further reservation is needed or permitted.
	txn.failedWaveOrphans = append(txn.failedWaveOrphans, orphans...)
	logutil.Warn("pk-move dump output persisted but never registered; retained for the close-time GC drain",
		zap.String("txn", txn.op.Txn().DebugString()),
		zap.Strings("objs", orphans),
		zap.Error(dErr))
}

// reclaimDumpWriterOutputLocked owns the dump writer's persisted-or-
// ambiguous output when the boundary dump fails inside Write or Sync
// (r35 review F2, D140): the observer refused nothing yet (or booked
// only earlier objects), but a completed object flush or a
// commit-ambiguous Sync attempt may already have reached storage while
// no registration can ever adopt it. The writer's own ownership
// snapshot — DeletePersisted tracks every persisted object plus every
// name reserved by a Sync attempt that did not return publishable
// stats — is reclaimed on a cancellation-independent context. Exactly
// one booking exists per snapshot name (the observer books before the
// first byte), so a reconciled reclaim releases every booking and a
// failed reclaim retains the snapshot's names with their bookings in
// failedWaveOrphans for the close drain — the same terminals as
// reclaimUnregisteredDumpLocked, one choke point earlier. Called with
// txn.Lock held; the dump's defer closes the writer afterwards (memory
// only).
func (txn *Transaction) reclaimDumpWriterOutputLocked(
	writer *colexec.CNS3Writer,
	booked int,
) {
	names, dErr := writer.DeletePersisted(context.Background())
	if dErr == nil {
		releaseDurableOrphanReservation(txn.engine.fs, booked)
		return
	}
	txn.failedWaveOrphans = append(txn.failedWaveOrphans, names...)
	logutil.Warn("pk-move dump writer output could not be reclaimed after a write failure; retained for the close-time GC drain",
		zap.String("txn", txn.op.Txn().DebugString()),
		zap.Strings("objs", names),
		zap.Int("booked", booked),
		zap.Error(dErr))
}

// gcFailedWaveOrphans reclaims the unpublished compaction replacement
// files whose local cleanup owners all failed: the rewrite writer's own
// in-defer reclaim (D118/D122) and the failed wave's cleanup pass itself
// (r29 review F3, D125). The replacements never reached commit
// serialization in any outcome — the wave that persisted them failed —
// so, exactly like the retired-transfer records above, they are
// unpublished garbage on every close path, and the transaction close is
// their owner of last resort. Runs at the quiescent close from
// delTransaction; ownership transfers to the durable orphan registry
// before the fallible reclamation attempt and is released only on
// confirmed deletion (r30 review F3, D128).
func (txn *Transaction) gcFailedWaveOrphans() {
	if len(txn.failedWaveOrphans) == 0 {
		return
	}
	names := append([]string(nil), txn.failedWaveOrphans...)
	txn.failedWaveOrphans = nil
	// CCPR cleanup is delegated to CCPRTxnCache on every path; mirror the
	// exemption (unreachable today: CCPR never stages pk-move compactions).
	if txn.isCCPRTxn {
		// D133: the delegated CCPR cleanup never hands these files to the
		// registry, so their outstanding reservations return unused.
		releaseDurableOrphanReservation(txn.engine.fs, len(names))
		return
	}
	// r30 review F3 (D128): same ownership transfer as the retired-
	// transfer drain above — registered before any fallible step,
	// released only on confirmed deletion, retried by later GC traffic.
	if txn.isCloneTxn {
		before := len(names)
		names = readutil.RemoveIf(names, func(name string) bool {
			return txn.engine.cloneTxnCache.IsSharedFile(txn.op.Txn().ID, name)
		})
		// D133: clone-shared files stay owned by their origin lifecycle —
		// this txn's reservations for them return unused.
		releaseDurableOrphanReservation(txn.engine.fs, before-len(names))
	}
	registerDurableOrphanGC(txn.engine.fs, names...)
	retryDurableOrphanGC(txn.engine)
}

// remapPkMoveLinkRowIDsLocked carries staged link rows through one
// compaction re-key: mergeTxnWorkspaceLocked rewrites the row ids of every
// merged insert batch, so a recorded old/new replacement identity that
// named a pre-merge workspace row id would dangle (and the prune would
// then retire a perfectly live link). Rewrites both row-id columns of
// every staged link batch through oldIds[i] -> newIds[i]. dead names the
// rowids the rewrite removed for good (a shrink's victims, whose values
// the renumbering re-mints for survivors): provenance keyed by them is
// retired instead of carried. A whole-batch re-key removes nothing and
// passes nil. Must be called with txn.Lock held.
//
// A remap write error is RETURNED, never swallowed: the merged user rows
// were already re-keyed, so silently continuing would let the prune retire
// now-dangling links and commit the user's update without its provenance —
// which the apply side then replays as a destructive DELETE+INSERT. Failing
// the compaction fails the enclosing statement/commit instead, which is
// the only outcome that keeps provenance and user rows on the same side.
// Partial rewrites inside one entry are harmless for the same reason: the
// error discards the whole workspace region through statement rollback.
func (txn *Transaction) remapPkMoveLinkRowIDsLocked(oldIds, newIds []types.Rowid, dead map[types.Rowid]struct{}) error {
	if len(oldIds) == 0 || len(oldIds) != len(newIds) {
		return nil
	}
	m := make(map[types.Rowid]types.Rowid, len(oldIds))
	for i := range oldIds {
		m[oldIds[i]] = newIds[i]
	}
	// The durable-origin map must ride the same re-key: its keys (and,
	// once a composition chained through staged generations, values) are
	// workspace identities minted before this compaction. Leaving them
	// stale makes a later statement's move look up its old side under the
	// pre-merge id and miss, so the link names the netted placeholder
	// instead of the durable departing generation. This must run even
	// when no link rows are staged yet — the edit-first, compaction, then
	// move ordering stages the link only after the re-key happened.
	//
	// The remap reads the old map but publishes a FRESH one: a shrink
	// renumbers survivors onto offsets their deleted neighbours used to
	// hold, so the mapping chains onto itself (w2 -> w1's old id while
	// w1 -> w0's old id), and mutating the map while ranging it lets one
	// visit clobber or re-remap another entry depending on Go's random
	// iteration order. Building the replacement from the untouched source
	// makes the composite remap order-independent; m is injective, so
	// rewritten keys never collide with each other, and untouched keys
	// belong to other batches, whose blocks never share these row ids.
	// Entries naming a dead row (keys removed by the shrink, or origins
	// recorded for a removed generation) are retired outright: the dead
	// rowid value is immediately re-minted for a survivor, and keeping
	// the stale entry would graft the victim's lineage onto it.
	if len(txn.pkMoveRowidOrigins) != 0 {
		remapped := make(map[types.Rowid]types.Rowid, len(txn.pkMoveRowidOrigins))
		for k, v := range txn.pkMoveRowidOrigins {
			if _, isDead := dead[k]; isDead {
				continue
			}
			if _, isDead := dead[v]; isDead {
				continue
			}
			nk, kok := m[k]
			nv, vok := m[v]
			switch {
			case kok && vok:
				remapped[nk] = nv
			case kok:
				remapped[nk] = v
			case vok:
				remapped[k] = nv
			default:
				remapped[k] = v
			}
		}
		txn.pkMoveRowidOrigins = remapped
	}
	// Heal bindings name pre-merge workspace identities just like origin
	// keys do; ride the same re-key, and retire bindings whose target row
	// the shrink removed — the dead rowid value is re-minted for a
	// survivor, so a stale binding would heal the wrong row.
	if len(txn.pkMoveNullHeals) != 0 {
		remapped := make(map[string]pkMoveNullHeal, len(txn.pkMoveNullHeals))
		for id, h := range txn.pkMoveNullHeals {
			if _, isDead := dead[h.rid]; isDead {
				continue
			}
			if nv, ok := m[h.rid]; ok {
				h.rid = nv
			}
			remapped[id] = h
		}
		txn.pkMoveNullHeals = remapped
	}
	// r26 review F4 (D111): lineage bindings pin a key to the durable
	// ORIGIN an S3-route replacement established. An RC transfer (or a
	// compaction re-key) that re-mints exactly that identity must carry
	// the binding with it — a later move's by-key heal otherwise targets
	// a rowid the committed tombstones no longer name, and the apply
	// replays DELETE+INSERT. Bindings whose origin the wave retired die
	// with it: the dead rowid value is re-minted for a survivor, so
	// keeping them would graft the victim's lineage onto that survivor.
	// Rollback safety is unaffected: wm/stmt tags are preserved, and
	// statement rollback only tail-cuts by stmt, never resurrecting an
	// origin this stampede re-keyed.
	if len(txn.pkMoveKeyLineage) != 0 {
		for acctTbl, perKey := range txn.pkMoveKeyLineage {
			for k, stack := range perKey {
				kept := stack[:0]
				for _, b := range stack {
					if _, isDead := dead[b.origin]; isDead {
						continue
					}
					if nv, ok := m[b.origin]; ok {
						b.origin = nv
					}
					kept = append(kept, b)
				}
				if len(kept) == 0 {
					delete(perKey, k)
				} else {
					perKey[k] = kept
				}
			}
			if len(perKey) == 0 {
				delete(txn.pkMoveKeyLineage, acctTbl)
			}
		}
	}
	var linkEntries []int
	for i := range txn.writes {
		we := &txn.writes[i]
		if isPkMoveLinkTableInsert(we.databaseName, we.tableName) &&
			we.typ == INSERT && we.bat != nil && we.bat.RowCount() > 0 {
			linkEntries = append(linkEntries, i)
		}
	}
	if len(linkEntries) == 0 {
		return nil
	}
	for _, idx := range linkEntries {
		we := &txn.writes[idx]
		oldRowIdx, newRowIdx := -1, -1
		for j, attr := range we.bat.Attrs {
			switch attr {
			case catalog.DataBranchUpdateLinkColOldRow:
				oldRowIdx = j
			case catalog.DataBranchUpdateLinkColNewRow:
				newRowIdx = j
			}
		}
		if oldRowIdx < 0 || newRowIdx < 0 ||
			oldRowIdx >= len(we.bat.Vecs) || newRowIdx >= len(we.bat.Vecs) {
			continue
		}
		// The row-id columns are binary(24) varlenas replaced IN PLACE
		// (r40 review F2, D148): the overwrite leaves len(v.area), every
		// descriptor, and the batch's measured size untouched — forward
		// carry and inverse alike — so a round trip returns the measured
		// footprint to its pre-round bytes exactly instead of consuming
		// the coordinator budget across retries. The unaccount/account
		// wrap stays around the mutation so it never runs outside the
		// workspace accounting helpers (delTransaction trips the
		// "changed outside accounting helpers" BUG guard otherwise); it
		// observes zero net delta across the loop.
		accounted := we.accountedSize != 0
		if accounted {
			txn.unaccountWorkspaceEntryLocked(we)
		}
		var writeErr error
		for _, vecIdx := range [2]int{oldRowIdx, newRowIdx} {
			vec := we.bat.Vecs[vecIdx]
			for r := 0; r < we.bat.RowCount(); r++ {
				if vec.IsNull(uint64(r)) {
					continue
				}
				raw := vec.GetBytesAt(r)
				if len(raw) != types.RowidSize {
					continue
				}
				if replacement, ok := m[types.DecodeFixed[types.Rowid](raw)]; ok {
					if err := pkMoveVecOverwriteRowidAt(vec, r, replacement[:], txn.proc.Mp()); err != nil {
						writeErr = err
						break
					}
				}
			}
			if writeErr != nil {
				break
			}
		}
		if accounted {
			txn.accountWorkspaceEntryLocked(we)
		}
		if writeErr != nil {
			return writeErr
		}
	}
	return nil
}

// pruneDeadPkMoveLinkRowsLocked drops, in place, every staged link row whose
// recorded workspace-stage replacement did not survive the transaction and
// whose dead tail is not rescued by the in-transaction successor chain. A
// replacement deleted (and possibly reincarnated as an unrelated row)
// inside the moving transaction leaves no stream rows, so its placeholder
// is the ONLY record that the lineage's landed generation vanished:
// keeping the link would let the diff consumer claim whichever row the key
// holds at the same commit timestamp, cascading destination children onto
// an unrelated row. Survival is a fixpoint over the successor chain: a
// link with a dead replacement is kept only when the chain of staged
// successor links continuing from that replacement reaches a link that
// itself survives, so a dead tail takes its ancestors down in one pass and
// re-running the prune changes nothing. Non-workspace identities are left
// alone: the single-shot claim governs them. NULL inhabitants (the owning
// insert never staged, for example the S3 buffering route) undergo the
// death-ledger evaluation: an uncommitted-object delete of a staged S3
// replacement never reaches the committed stream (the delete is diverted
// to the per-block list and the object is rewritten minus the row), so
// only the ledger recorded at the divert can tell a live staged
// replacement from a rewritten-away one. A NULL link whose key died AFTER
// the link's birth statement survives only if a live continuer (a staged
// successor link born at or after the death, or a same-statement
// continuity marker) carries the key onward.
//
// Called at the top of EVERY IncrStatementID (before the statement
// transition compacts or spills staged rows) and once more at commit after
// transferTombstonesByCommit: the prune matches recorded replacement
// identities against staged row ids, and compaction rewrites or S3 spills
// move exactly those identities. remapPkMoveLinkRowIDsLocked rides every
// compaction re-key and the dump pins every row a staged link names
// (pkMoveStagedLinkPinnedRowIDsLocked), so between prunes the recorded
// identities stay matched to live, in-memory rows. The prune is
// idempotent, so commit retries and back-to-back statement boundaries
// change nothing. The shrink rides the workspace accounting helpers
// exactly like shrinkWorkspaceEntryBatchLocked: mutating a staged batch
// outside those helpers corrupts the entry size accounting.
func (txn *Transaction) pruneDeadPkMoveLinkRowsLocked() {
	keeps := pkMoveLinkRowsToKeep(txn.writes,
		txn.pkMoveLedgerForPruneLocked())
	if len(keeps) > 0 {
		txn.beginCatalogMutation()
		defer txn.endCatalogMutation()
		for idx, keep := range keeps {
			we := &txn.writes[idx]
			if we.bat == nil || we.bat.RowCount() == 0 || len(keep) == we.bat.RowCount() {
				continue
			}
			txn.unaccountWorkspaceEntryLocked(we)
			we.bat.Shrink(keep, false)
			we.bat.SetRowCount(we.bat.Vecs[0].Length())
			txn.accountWorkspaceEntryLocked(we)
			// The selection map keyed by this batch is stale after the shrink.
			delete(txn.batchSelectList, we.bat)
		}
	}
	// Retention genesis (D161(A.1)): an OWNED address whose registration this
	// transaction staged cancels — staged row and map entry destroyed in one
	// step — when the shrink above (or an earlier one) removed its last
	// staged link, unless the first-touch probe found surviving committed
	// links. The prune runs only at settled cadence points (successful
	// statement boundaries and the commit prune), so the destruction needs
	// no undo journal.
	txn.cancelGenesisRegistrationsWithoutLinksLocked()
}

// pkMoveLinkRowsToKeep computes, per staging-order entry index, the rows a
// prune keeps; entries absent from the result are untouched. Pure over the
// writes slice so tests can drive the decision without a transaction.
//
// A link whose recorded workspace-stage replacement does not survive the
// transaction — the replacement row is absent from the staged inserts, or
// a later same-transaction write deletes it — is dropped: after commit
// nothing the link describes exists, and keeping it would let the diff
// consumer claim whichever row the vacated key holds at the same commit
// timestamp. Whether a link survives is a FIXPOINT over the
// in-transaction successor chain, not a one-hop check: a link with a dead
// replacement still survives when the chain of links continuing from that
// replacement (successor.old_row_id == this link's new_row_id) reaches a
// link that itself survives. A dead tail therefore takes its ancestors
// down in the same pass, which also makes the prune idempotent — a second
// pass over the shrunk workspace keeps exactly the rows already kept.
//
// The staged set needs no spill supplement: mid-transaction dumps pin
// every row a staged link cell names (pkMoveStagedLinkPinnedRowIDsLocked
// recomputed per dump), so a named workspace identity is always staged
// here — an identity absent from the staged set is one no staged link
// could ever record, and treating it as dead keeps the prune fail-closed.
// deaths, when non-nil, gates the NULL-identity (S3-route) inhabitants
// through the diverted-death ledger described on pruneDeadPkMoveLinkRowsLocked.
func pkMoveLinkRowsToKeep(
	writes []Entry,
	deaths *pkMoveDeathLedger,
) map[int][]int64 {
	var keeps map[int][]int64
	hasLink := false
	for i := range writes {
		we := &writes[i]
		if we.typ == INSERT && we.bat != nil && we.bat.RowCount() > 0 &&
			isPkMoveLinkTableInsert(we.databaseName, we.tableName) {
			hasLink = true
			break
		}
	}
	if !hasLink {
		return nil
	}

	rowIDsOf := func(we *Entry) []types.Rowid {
		if we.bat == nil || we.bat.RowCount() == 0 ||
			we.bat.Vecs[0].GetType().Oid != types.T_Rowid {
			return nil
		}
		vec := we.bat.Vecs[0]
		n := min(we.bat.RowCount(), vec.Length())
		if n <= 0 {
			return nil
		}
		return vector.MustFixedColNoTypeCheck[types.Rowid](vec)[:n]
	}

	staged := make(map[pkMoveAcctTbl]map[types.Rowid]struct{})
	deleted := make(map[pkMoveAcctTbl]map[types.Rowid]struct{})
	var links []pkMoveLinkRow
	for i := range writes {
		we := &writes[i]
		if isPkMoveLinkTableInsert(we.databaseName, we.tableName) {
			if we.typ != INSERT || we.bat == nil || we.bat.RowCount() == 0 {
				continue
			}
			tableIdx, newRowIdx, oldRowIdx := -1, -1, -1
			oldPKIdx, newPKIdx, linkIDIdx := -1, -1, -1
			for j, attr := range we.bat.Attrs {
				switch attr {
				case catalog.DataBranchUpdateLinkColTableID:
					tableIdx = j
				case catalog.DataBranchUpdateLinkColNewRow:
					newRowIdx = j
				case catalog.DataBranchUpdateLinkColOldRow:
					oldRowIdx = j
				case catalog.DataBranchUpdateLinkColOldPK:
					oldPKIdx = j
				case catalog.DataBranchUpdateLinkColNewPK:
					newPKIdx = j
				case catalog.DataBranchUpdateLinkColLinkID:
					linkIDIdx = j
				}
			}
			if tableIdx < 0 || newRowIdx < 0 ||
				tableIdx >= len(we.bat.Vecs) || newRowIdx >= len(we.bat.Vecs) ||
				we.bat.Vecs[tableIdx].GetType().Oid != types.T_uint64 {
				continue
			}
			tableIDs := vector.MustFixedColNoTypeCheck[uint64](we.bat.Vecs[tableIdx])
			rowCount := we.bat.RowCount()
			if len(tableIDs) < rowCount {
				continue
			}
			newRowVec := we.bat.Vecs[newRowIdx]
			var oldRowVec *vector.Vector
			if 0 <= oldRowIdx && oldRowIdx < len(we.bat.Vecs) {
				oldRowVec = we.bat.Vecs[oldRowIdx]
			}
			var oldPKVec, newPKVec, linkIDVec *vector.Vector
			if 0 <= oldPKIdx && oldPKIdx < len(we.bat.Vecs) {
				oldPKVec = we.bat.Vecs[oldPKIdx]
			}
			if 0 <= newPKIdx && newPKIdx < len(we.bat.Vecs) {
				newPKVec = we.bat.Vecs[newPKIdx]
			}
			if 0 <= linkIDIdx && linkIDIdx < len(we.bat.Vecs) {
				linkIDVec = we.bat.Vecs[linkIDIdx]
			}
			for r := 0; r < rowCount; r++ {
				lr := pkMoveLinkRow{
					entry: i,
					row:   int64(r),
					acct:  pkMoveAcctTbl{account: we.accountId, table: tableIDs[r]},
				}
				if !newRowVec.IsNull(uint64(r)) {
					if raw := newRowVec.GetBytesAt(r); len(raw) == types.RowidSize {
						lr.hasNewRID = true
						lr.newRID = types.DecodeFixed[types.Rowid](raw)
					}
				}
				if oldRowVec != nil && !oldRowVec.IsNull(uint64(r)) {
					if raw := oldRowVec.GetBytesAt(r); len(raw) == types.RowidSize {
						lr.hasOld = true
						lr.oldRID = types.DecodeFixed[types.Rowid](raw)
					}
				}
				if oldPKVec != nil && !oldPKVec.IsNull(uint64(r)) {
					lr.oldPK = string(oldPKVec.GetBytesAt(r))
				}
				if newPKVec != nil && !newPKVec.IsNull(uint64(r)) {
					lr.newPK = string(newPKVec.GetBytesAt(r))
				}
				if linkIDVec != nil && !linkIDVec.IsNull(uint64(r)) {
					lr.linkID = string(linkIDVec.GetBytesAt(r))
				}
				if !lr.hasNewRID && deaths != nil && lr.linkID != "" {
					if hr, ok := deaths.heals[lr.linkID]; ok {
						lr.hasNewRID = true
						lr.newRID = hr
					}
				}
				links = append(links, lr)
			}
			continue
		}
		key := pkMoveAcctTbl{account: we.accountId, table: we.tableId}
		switch we.typ {
		case INSERT:
			for _, rid := range rowIDsOf(we) {
				set := staged[key]
				if set == nil {
					set = make(map[types.Rowid]struct{})
					staged[key] = set
				}
				set[rid] = struct{}{}
			}
		case DELETE:
			for _, rid := range rowIDsOf(we) {
				set := deleted[key]
				if set == nil {
					set = make(map[types.Rowid]struct{})
					deleted[key] = set
				}
				set[rid] = struct{}{}
			}
		}
	}

	// Fixpoint keep-set: links needing no chain evidence (a non-workspace
	// replacement — the commit-ts single-shot claim governs it; a NULL one
	// gates on the death ledger below) and links whose staged replacement
	// survives hold directly. A remaining candidate survives iff its dead
	// replacement is the recorded old row of an already-kept link; the
	// queue propagates that backwards along the chain. Links never
	// reached are dropped.
	kept := make([]bool, len(links))
	byNew := make(map[types.Rowid][]int)
	for i, lr := range links {
		// A link the fill KILLED (its lineage proven dead before the
		// generation it moved) stays dropped regardless of aliveness
		// evidence, and never seeds the rescue queue or the death-ledger
		// continuer index below.
		if deaths != nil && lr.linkID != "" {
			if _, dead := deaths.killed[lr.linkID]; dead {
				continue
			}
		}
		if lr.hasNewRID && pkMoveIsWorkspaceStageRowid(&lr.newRID) {
			_, alive := staged[lr.acct][lr.newRID]
			_, gone := deleted[lr.acct][lr.newRID]
			survives := alive && !gone
			if !survives {
				byNew[lr.newRID] = append(byNew[lr.newRID], i)
			}
			kept[i] = survives
			continue
		}
		kept[i] = true
	}
	// The diverted-death ledger decides between a genuinely live staged
	// replacement and one an uncommitted-object rewrite erased: for
	// NULL-identity (S3-route) links it is the ONLY survival evidence,
	// and for identity-carrying links it closes the spill hole — a link
	// whose new_row_id still names a rewrite-erased workspace identity
	// looks alive in the staged/deleted maps even though the delete of
	// the physical row was diverted into pk-level death records. Since
	// the ledger only carries divert deaths, table-route replacements
	// (whose deletes keep workspace identity and are caught by the maps
	// above) see no entries here. Continuers must be ALIVE links, so
	// this is a fixpoint of its own and runs before the rescue queue
	// below is seeded: a killed link must not rescue anyone.
	if deaths != nil {
		pkMoveApplyDeathLedger(links, kept, deaths)
	}
	var queue []types.Rowid
	for i, lr := range links {
		if kept[i] && lr.hasOld {
			queue = append(queue, lr.oldRID)
		}
	}
	for len(queue) > 0 {
		rid := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		for _, i := range byNew[rid] {
			if kept[i] {
				continue
			}
			kept[i] = true
			if links[i].hasOld {
				queue = append(queue, links[i].oldRID)
			}
		}
	}

	keptByEntry := make(map[int][]int64)
	collected := make(map[int]struct{})
	for i, lr := range links {
		collected[lr.entry] = struct{}{}
		if kept[i] {
			keptByEntry[lr.entry] = append(keptByEntry[lr.entry], lr.row)
		}
	}
	for idx := range collected {
		keep := keptByEntry[idx]
		if len(keep) == writes[idx].bat.RowCount() {
			continue
		}
		if keeps == nil {
			keeps = make(map[int][]int64)
		}
		keeps[idx] = keep
	}
	return keeps
}

// pkMoveLedgerForPruneLocked exposes the transaction's diverted-death
// evidence to the keep computation, or nil when the transaction recorded
// none (the common case keeps the ledger evaluation off entirely).
func (txn *Transaction) pkMoveLedgerForPruneLocked() *pkMoveDeathLedger {
	if len(txn.pkMoveDeadPks) == 0 && len(txn.pkMoveMarkerStmts) == 0 &&
		len(txn.pkMoveNullHeals) == 0 && len(txn.pkMoveKilledLinks) == 0 {
		return nil
	}
	heals := make(map[string]types.Rowid, len(txn.pkMoveNullHeals))
	for id, h := range txn.pkMoveNullHeals {
		heals[id] = h.rid
	}
	var killed map[string]struct{}
	if len(txn.pkMoveKilledLinks) > 0 {
		killed = make(map[string]struct{}, len(txn.pkMoveKilledLinks))
		for id := range txn.pkMoveKilledLinks {
			killed[id] = struct{}{}
		}
	}
	return &pkMoveDeathLedger{
		dead:    txn.pkMoveDeadPks,
		markers: txn.pkMoveMarkerStmts,
		birth:   txn.pkMoveLinkBirth,
		heals:   heals,
		killed:  killed,
	}
}

// pkMoveApplyDeathLedger retires NULL-identity link rows whose replacement
// key died in an uncommitted CN object after the link was staged, unless a
// still-living continuation of the same key explains the death. Pure over
// (links, kept, deaths) so tests drive it directly.
//
// A NULL new_row_id means the owning insert never reached the row-entry
// workspace (the S3 buffering route stages it straight into object files).
// The committed stream then shows the replacement only as its final
// committed image, and a mid-transaction uncommitted-object delete of the
// replacement is invisible two ways at once: the tombstone batch never
// carries the row (the delete diverts to the per-block list) AND the
// eventual object rewrite erases the staged row. Without the ledger the
// keep-set had no choice but to keep such links unconditionally, letting
// the consumer claim whichever row the key holds at commit — including an
// unrelated reinsert — and cascade destination children onto it.
//
// Rule: link L (NULL identity, new key K, born in statement bL) dies when
// the ledger holds ANY death of K in a statement d > bL that has no
// explanation, where an explanation is
//   - a same-key continuity marker processed in the SAME statement d (the
//     edit consumed exactly that deceased generation onward; successive
//     edits each keep their own statement, so an explained death stays
//     explained when a later edit lands), or
//   - an ALIVE staged link row with old key K born in a statement
//     >= d (an onward move consumed it; statement-equal is enough because
//     the link write and its own old-side divert share the statement).
//
// Continuers must be alive, so killing runs to a fixpoint: a dead tail
// stops rescuing its ancestors in the same pass.
// pkMoveLinkRow is the keep-set's per-link-row view: identity columns,
// lineage columns, and the statement bookkeeping keys the death ledger
// needs.
type pkMoveLinkRow struct {
	entry     int
	row       int64
	acct      pkMoveAcctTbl
	hasNewRID bool
	newRID    types.Rowid
	hasOld    bool
	oldRID    types.Rowid
	oldPK     string
	newPK     string
	linkID    string
}

func pkMoveApplyDeathLedger(
	links []pkMoveLinkRow,
	kept []bool,
	deaths *pkMoveDeathLedger,
) {
	changed := true
	for changed {
		changed = false
		continuer := make(map[pkMoveAcctTbl]map[string]int)
		for i := range links {
			if !kept[i] || links[i].oldPK == "" {
				continue
			}
			key := links[i].acct
			birth := 0
			if deaths.birth != nil {
				birth = deaths.birth[links[i].linkID]
			}
			// A continuer whose OWN old key died before it was staged is
			// post-mortem reincarnation lineage: it continues the row the
			// reinsert created, never the pre-death move, and must not
			// rescue anything. The latest recorded death decides: when
			// even the newest death of the old key predates the staging,
			// every recorded generation of that key was dead on arrival.
			if od, ok := pkMoveLatestStmt(deaths.dead[key][links[i].oldPK]); ok && od < birth {
				continue
			}
			set := continuer[key]
			if set == nil {
				set = make(map[string]int)
				continuer[key] = set
			}
			if birth > set[links[i].oldPK] {
				set[links[i].oldPK] = birth
			}
		}
		for i := range links {
			if !kept[i] {
				continue
			}
			// Death-ledger evaluation gates EVERY link, not only the
			// NULL-identity (S3-route) ones. A staged/workspace or healed
			// identity proves the replacement WAS staged; it does not
			// prove the replacement survived a same-transaction delete
			// routed through an uncommitted-object rewrite: that death is
			// recorded by pk only (the physical object row id never names
			// the identity the link carries), so looking the link's
			// identity up in staged/spilled/deleted can report alive
			// while the row is gone. The pk-level death ledger is the
			// only evidence that spans the identity change.
			dset, ok := deaths.dead[links[i].acct][links[i].newPK]
			if !ok {
				continue
			}
			for d := range dset {
				if d <= deaths.birth[links[i].linkID] {
					// Deaths that did not outlive the link's own staging
					// statement name an earlier generation, never this
					// replacement.
					continue
				}
				// A marker recorded in the SAME statement as the death
				// is the divert's own continuity edit consuming the
				// deceased generation onward; earlier or later markers
				// never explain this death (a later one continues
				// whatever reincarnation followed, not this lineage),
				// while EVERY recorded death needs an explanation.
				if _, explained := deaths.markers[links[i].acct][links[i].newPK][d]; explained {
					continue
				}
				if continuer[links[i].acct][links[i].newPK] >= d {
					continue
				}
				kept[i] = false
				changed = true
				break
			}
		}
	}
}
