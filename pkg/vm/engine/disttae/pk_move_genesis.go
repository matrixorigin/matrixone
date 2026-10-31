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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

// isPkMoveRetentionTableInsert reports whether a staged insert targets the
// pk-move retention-ledger catalog table. Tables are matched by name because
// the internal catalog assigns ids dynamically per cluster (mirrors
// isPkMoveLinkTableInsert).
func isPkMoveRetentionTableInsert(databaseName, tableName string) bool {
	return databaseName == catalog.MO_CATALOG &&
		tableName == catalog.MO_DATA_BRANCH_LINK_RETENTION
}

// pkMoveGenesisWriteKey marks a retention-ledger identity row write staged
// by the pk-move link producer. Only marked retention inserts are admitted
// through the genesis epoch machinery: the refresh/KEEP upserts the funnel
// issues as ordinary internal SQL must never be deduplicated (or swallowed
// as probe hits) by this path — they simply spill and stay cancelable by
// the ordinary statement cut (D166).
type pkMoveGenesisWriteKey struct{}

// WithPkMoveGenesisWrite marks ctx so the workspace admission recognizes the
// batch as a genesis identity-row write.
func WithPkMoveGenesisWrite(ctx context.Context) context.Context {
	return context.WithValue(ctx, pkMoveGenesisWriteKey{}, true)
}

// PkMoveGenesisWriteMarked reports whether ctx carries the genesis-write
// mark. It is exported for the producer side, which must assert the mark
// survives its write-context stacking.
func PkMoveGenesisWriteMarked(ctx context.Context) bool {
	marked, _ := ctx.Value(pkMoveGenesisWriteKey{}).(bool)
	return marked
}

// PkMoveGenesisProbe executes the one keyed committed-state probe a
// registration epoch pays per address (D159/D161/D165): registration
// existence in the retention ledger, committed link presence for the same
// address (own-transaction rows excluded via ownTxnIDHex), and — only when
// resolveLogical is set — the address's durable rel_logical_id. The live
// implementation is registered by the link producer at package init because
// the internal-SQL executor chain must not become an engine dependency;
// fabric tests override this variable directly. A nil probe fails the
// staging statement loudly rather than guessing: a guessed insert can abort
// a long transaction on a duplicate key at commit.
//
// eng carries the probing transaction's OWN engine: the HIT-lock half
// resolves the retention relation through it, because a live executor
// process inside the coordinator pipeline does not populate
// SessionInfo.StorageEngine.
var PkMoveGenesisProbe func(
	ctx context.Context,
	proc *process.Process,
	eng engine.Engine,
	accountID uint32,
	tableID uint64,
	ownTxnIDHex string,
	resolveLogical bool,
) (registered, committedLinks bool, logicalID uint64, err error)

// pkMoveGenesisProbeVerdict captures one address's first-touch probe answer.
type pkMoveGenesisProbeVerdict struct {
	registered     bool
	committedLinks bool
	logicalID      uint64
}

// pkMoveGenesisState enumerates the txn-private epoch states of the D161(B)
// dedup map; absence of a map entry is the ABSENT resting state (a CANCELED
// resting state does not exist — pruning destroys the staged row and the map
// entry in one step, D163).
type pkMoveGenesisState int8

const (
	// pkMoveGenesisOwned: this transaction staged the address's identity row.
	pkMoveGenesisOwned pkMoveGenesisState = 1
	// pkMoveGenesisExternal: the first-touch probe found a committed
	// registration; the memo carries no row, no cancel authority, and no
	// refresh authority.
	pkMoveGenesisExternal pkMoveGenesisState = 2
)

// pkMoveGenesisEpoch is one address's live epoch record. committedLinks is
// the first-touch committed-link presence answer: it disciplines the prune
// cancellation (D161(A.1)) — an address holding committed links keeps its
// registration even when every link this transaction staged dies. It is
// re-derived per epoch and never carried across a cancellation.
type pkMoveGenesisEpoch struct {
	state          pkMoveGenesisState
	committedLinks bool
}

// pkMoveGenesisJournalKind tags which genesis transition a journal entry
// records so statement undo can invert exactly that transition.
type pkMoveGenesisJournalKind uint8

const (
	// pkMoveGenesisJournalRegister: the statement placed the address into
	// the epoch map (OWNED or EXTERNAL) from ABSENT. Undo deletes the entry.
	pkMoveGenesisJournalRegister pkMoveGenesisJournalKind = 1
	// pkMoveGenesisJournalRetire: the statement deleted a pre-existing epoch
	// (ledger retirement, D167). Undo restores the journaled epoch.
	pkMoveGenesisJournalRetire pkMoveGenesisJournalKind = 2
	// pkMoveGenesisJournalRefresh: the statement promoted an EXTERNAL epoch
	// to OWNED (retention refresh re-staged the identity on this
	// transaction, D168). Undo restores the journaled EXTERNAL epoch.
	pkMoveGenesisJournalRefresh pkMoveGenesisJournalKind = 3
)

// pkMoveGenesisJournalEntry journals one statement's registration transition
// for statement-atomic rollback. Register undo deletes the map entry (back
// to ABSENT) and the staged row dies with the positional workspace cut.
// Retire/Refresh undo restores the exact pre-transition epoch carried in
// prev. Entries settle at the statement boundary — a completed statement is
// beyond statement-rollback reach — so the boundary clear bounds the journal
// to the in-flight statement.
type pkMoveGenesisJournalEntry struct {
	seq     int
	addr    pkMoveAcctTbl
	kind    pkMoveGenesisJournalKind
	prev    pkMoveGenesisEpoch
	hasPrev bool
}

// probePkMoveGenesis runs the keyed first-touch probe for every address the
// marked retention batch carries and the dedup map does not yet know. It
// runs BEFORE the workspace lock is taken (same discipline as
// resolvePKPositionsForWrite: catalog-resolution machinery may reenter
// txn.Lock) and mutates nothing; the locked admission re-consults the map as
// the authority. One internal-SQL round trip per unknown address per epoch —
// repeat touches dedup at zero probes because their addresses already have
// map entries.
func (txn *Transaction) probePkMoveGenesis(
	ctx context.Context,
	accountId uint32,
	bat *batch.Batch,
) (map[uint64]pkMoveGenesisProbeVerdict, error) {
	rowKindIdx, tableIDIdx, logicalIdx := -1, -1, -1
	for j, attr := range bat.Attrs {
		switch attr {
		case catalog.DataBranchLinkRetentionColRowKind:
			rowKindIdx = j
		case catalog.DataBranchLinkRetentionColTableID:
			tableIDIdx = j
		case catalog.DataBranchLinkRetentionColLogicalID:
			logicalIdx = j
		}
	}
	if rowKindIdx < 0 || tableIDIdx < 0 ||
		rowKindIdx >= len(bat.Vecs) || tableIDIdx >= len(bat.Vecs) ||
		bat.Vecs[rowKindIdx].GetType().Oid != types.T_int8 ||
		bat.Vecs[rowKindIdx].Length() < bat.RowCount() ||
		bat.Vecs[tableIDIdx].GetType().Oid != types.T_uint64 ||
		bat.Vecs[tableIDIdx].Length() < bat.RowCount() {
		return nil, moerr.NewInternalErrorNoCtx(
			"pk-move genesis retention batch lacks its row_kind/table_id columns")
	}
	tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[tableIDIdx])
	rowKinds := vector.MustFixedColNoTypeCheck[int8](bat.Vecs[rowKindIdx])
	for i := 0; i < bat.RowCount(); i++ {
		if rowKinds[i] != int8(catalog.DataBranchLinkRetentionRowKindReal) {
			// Fencing scratch rows travel their own committed lane and are
			// never producer-marked; a marked batch carrying a non-real row
			// is corrupted input — refuse it loudly instead of registering
			// an address the epoch machinery must never track.
			return nil, moerr.NewInternalErrorNoCtxf(
				"pk-move genesis admission refuses non-real retention row (row_kind=%d, table_id=%d)",
				rowKinds[i], tableIDs[i])
		}
	}
	var logicals []uint64
	if logicalIdx >= 0 && logicalIdx < len(bat.Vecs) &&
		bat.Vecs[logicalIdx].GetType().Oid == types.T_uint64 &&
		bat.Vecs[logicalIdx].Length() >= bat.RowCount() {
		logicals = vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[logicalIdx])
	}
	ownTxnIDHex := pkMoveGenesisOwnTxnIDHex(txn)
	verdicts := make(map[uint64]pkMoveGenesisProbeVerdict)
	for i := 0; i < bat.RowCount(); i++ {
		tableID := tableIDs[i]
		if _, done := verdicts[tableID]; done {
			continue
		}
		addr := pkMoveAcctTbl{account: accountId, table: tableID}
		txn.Lock()
		_, known := txn.pkMoveGenesisStates[addr]
		txn.Unlock()
		if known {
			// Repeat touch inside a live epoch: zero probes, zero inserts
			// (the locked admission drops the duplicate row).
			continue
		}
		resolveLogical := logicals == nil || logicals[i] == 0
		if PkMoveGenesisProbe == nil {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"pk-move genesis probe unavailable for table %d: the producer registration sets it at init; refusing to guess", tableID))
		}
		reg, links, logical, perr := PkMoveGenesisProbe(
			ctx, txn.proc, txn.engine, accountId, tableID, ownTxnIDHex, resolveLogical)
		if perr != nil {
			if moerr.IsMoErrCode(perr, moerr.ErrNoSuchTable) {
				// The retention catalog is absent (pre-upgrade tenant); the
				// producer gates the same condition, so reaching it here
				// means a resolution race — degrade with a WARN instead of
				// committing rows the epoch machinery cannot track.
				logutil.Warn(
					"pk-move link retention genesis degraded: retention catalog unavailable",
					zap.Uint64("table_id", tableID), zap.Error(perr))
				verdicts[tableID] = pkMoveGenesisProbeVerdict{registered: true}
				continue
			}
			return nil, perr
		}
		verdicts[tableID] = pkMoveGenesisProbeVerdict{
			registered: reg, committedLinks: links, logicalID: logical,
		}
	}
	return verdicts, nil
}

// admitPkMoveGenesisBatchLocked applies the probe verdicts to a marked
// retention identity batch, deduplicating through the epoch map: an address
// with a live entry drops its row (repeat touch), a probe hit records a
// read-only EXTERNAL memo and drops the row, and only a fresh miss stages an
// OWNED row, back-filling a resolved logical id when the producer's
// descriptor lacked it. Every first-touch decision journals under the
// current statement for statement-atomic rollback. Must be called with
// txn.Lock held.
func (txn *Transaction) admitPkMoveGenesisBatchLocked(
	accountId uint32,
	bat *batch.Batch,
	verdicts map[uint64]pkMoveGenesisProbeVerdict,
) error {
	tableIDIdx, logicalIdx := -1, -1
	for j, attr := range bat.Attrs {
		switch attr {
		case catalog.DataBranchLinkRetentionColTableID:
			tableIDIdx = j
		case catalog.DataBranchLinkRetentionColLogicalID:
			logicalIdx = j
		}
	}
	if tableIDIdx < 0 || tableIDIdx >= len(bat.Vecs) ||
		bat.Vecs[tableIDIdx].GetType().Oid != types.T_uint64 {
		return moerr.NewInternalErrorNoCtx(
			"pk-move genesis admission: retention batch lost its table_id column")
	}
	tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[tableIDIdx])
	var logicals []uint64
	if logicalIdx >= 0 && logicalIdx < len(bat.Vecs) &&
		bat.Vecs[logicalIdx].GetType().Oid == types.T_uint64 &&
		bat.Vecs[logicalIdx].Length() >= bat.RowCount() {
		logicals = vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[logicalIdx])
	}
	var keep []int64
	for i := 0; i < bat.RowCount(); i++ {
		tableID := tableIDs[i]
		addr := pkMoveAcctTbl{account: accountId, table: tableID}
		if _, live := txn.pkMoveGenesisStates[addr]; live {
			// Repeat touch: deduped at zero inserts.
			continue
		}
		verdict := verdicts[tableID]
		if txn.pkMoveGenesisStates == nil {
			txn.pkMoveGenesisStates = make(map[pkMoveAcctTbl]*pkMoveGenesisEpoch)
		}
		if verdict.registered {
			// EXTERNAL: read-only memo. The probe-hit degradation above
			// (retention catalog unavailable) funnels here as well: swallow
			// the row rather than commit an untracked duplicate.
			txn.pkMoveGenesisStates[addr] = &pkMoveGenesisEpoch{
				state:          pkMoveGenesisExternal,
				committedLinks: verdict.committedLinks,
			}
			txn.pkMoveGenesisJournal = append(txn.pkMoveGenesisJournal,
				pkMoveGenesisJournalEntry{
					seq:  txn.statementID,
					addr: addr,
					kind: pkMoveGenesisJournalRegister,
				})
			continue
		}
		txn.pkMoveGenesisStates[addr] = &pkMoveGenesisEpoch{
			state:          pkMoveGenesisOwned,
			committedLinks: verdict.committedLinks,
		}
		txn.pkMoveGenesisJournal = append(txn.pkMoveGenesisJournal,
			pkMoveGenesisJournalEntry{
				seq:  txn.statementID,
				addr: addr,
				kind: pkMoveGenesisJournalRegister,
			})
		if verdict.logicalID != 0 && logicals != nil && logicals[i] == 0 &&
			uint64(len(logicals)) >= uint64(bat.RowCount()) {
			// The producer's descriptor lacked the logical id and the keyed
			// mo_tables read resolved it: publish the durable identity with
			// the staged row.
			if err := vector.SetFixedAtNoTypeCheck[uint64](
				bat.Vecs[logicalIdx], int(i), verdict.logicalID); err != nil {
				return err
			}
		}
		keep = append(keep, int64(i))
	}
	if len(keep) == bat.RowCount() {
		return nil
	}
	bat.Shrink(keep, false)
	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
}

// PkMoveRetireAddresses drops the genesis epoch of every retired address
// (D167: the link-ledger destruction unregisters this transaction's epoch so
// later retention spills provoke a fresh keyed first-touch instead of
// deduping against a ledger that no longer exists). Dropping the entry also
// withdraws the address from the forced-dump keep lane automatically:
// pkMoveGenesisAnyOwnedRowLocked derives membership from this same map. Each
// deletion journals the pre-transition epoch so statement rollback restores
// it exactly. An absent address (or nil map) is a uniform no-op — undo has
// nothing to invert. Takes txn.Lock internally; callers run OUTSIDE the
// locked region (ledger-deletion paths already do).
func (txn *Transaction) PkMoveRetireAddresses(accountID uint32, tableIDs []uint64) {
	if len(tableIDs) == 0 {
		return
	}
	txn.Lock()
	defer txn.Unlock()
	if len(txn.pkMoveGenesisStates) == 0 {
		return
	}
	for _, tableID := range tableIDs {
		addr := pkMoveAcctTbl{account: accountID, table: tableID}
		epoch, live := txn.pkMoveGenesisStates[addr]
		if !live {
			continue
		}
		txn.pkMoveGenesisJournal = append(txn.pkMoveGenesisJournal,
			pkMoveGenesisJournalEntry{
				seq:     txn.statementID,
				addr:    addr,
				kind:    pkMoveGenesisJournalRetire,
				prev:    *epoch,
				hasPrev: true,
			})
		delete(txn.pkMoveGenesisStates, addr)
	}
}

// PkMoveNoteGenesisRefresh records the refresh lane's ownership transfer
// (D168: insertPkMoveLinkRetention re-staging the retention identity row on
// this transaction promotes its registration from the EXTERNAL read-only
// memo to OWNED — this transaction's workspace now carries the identity, so
// the keep lane and the prune cancellation must treat the address as
// self-staged). Untracked or already-OWNED addresses need no map change and
// journal nothing. An EXTERNAL epoch journals its pre-transition value, then
// promotes in place, keeping the first-touch committedLinks memo. Takes
// txn.Lock internally; callers run OUTSIDE the locked region.
func (txn *Transaction) PkMoveNoteGenesisRefresh(accountID uint32, tableIDs []uint64) {
	if len(tableIDs) == 0 {
		return
	}
	txn.Lock()
	defer txn.Unlock()
	if len(txn.pkMoveGenesisStates) == 0 {
		return
	}
	for _, tableID := range tableIDs {
		addr := pkMoveAcctTbl{account: accountID, table: tableID}
		epoch, live := txn.pkMoveGenesisStates[addr]
		if !live || epoch.state != pkMoveGenesisExternal {
			continue
		}
		txn.pkMoveGenesisJournal = append(txn.pkMoveGenesisJournal,
			pkMoveGenesisJournalEntry{
				seq:     txn.statementID,
				addr:    addr,
				kind:    pkMoveGenesisJournalRefresh,
				prev:    *epoch,
				hasPrev: true,
			})
		epoch.state = pkMoveGenesisOwned
	}
}

// pkMoveGenesisOwnTxnIDHex normalizes the transaction's operator identity to
// the 16-byte catalog width the link producer persists (the same
// normalization multi_update.applyPkMoveLinkNormTxnID uses) and hex-encodes
// it for the committed-link probe's own-txn exclusion.
func pkMoveGenesisOwnTxnIDHex(txn *Transaction) string {
	var norm [catalog.DataBranchUpdateLinkTxnIDBytes]byte
	id := txn.op.Txn().ID
	if len(id) == catalog.DataBranchUpdateLinkTxnIDBytes {
		copy(norm[:], id)
	}
	out := make([]byte, 0, 2*len(norm))
	const hexdigits = "0123456789abcdef"
	for _, b := range norm {
		out = append(out, hexdigits[b>>4], hexdigits[b&0x0f])
	}
	return string(out)
}

// pkMoveGenesisAnyOwnedRowLocked reports whether the retention batch holds
// at least one row whose address this transaction OWNS — the membership half
// of the forced-dump keep lane's conjunction (D166(A)). Name-match alone
// would retain unowned funnel upsert batches staged by ordinary internal SQL
// on this transaction's workspace; they must spill freely. A nil map admits
// nothing. Must be called with txn.Lock held.
func (txn *Transaction) pkMoveGenesisAnyOwnedRowLocked(we *Entry) bool {
	if len(txn.pkMoveGenesisStates) == 0 || we.bat == nil || we.bat.RowCount() == 0 {
		return false
	}
	tableIDIdx := -1
	for j, attr := range we.bat.Attrs {
		if attr == catalog.DataBranchLinkRetentionColTableID {
			tableIDIdx = j
			break
		}
	}
	if tableIDIdx < 0 || tableIDIdx >= len(we.bat.Vecs) ||
		we.bat.Vecs[tableIDIdx].GetType().Oid != types.T_uint64 ||
		we.bat.Vecs[tableIDIdx].Length() < we.bat.RowCount() {
		return false
	}
	tableIDs := vector.MustFixedColNoTypeCheck[uint64](we.bat.Vecs[tableIDIdx])
	for i := 0; i < we.bat.RowCount(); i++ {
		epoch := txn.pkMoveGenesisStates[pkMoveAcctTbl{account: we.accountId, table: tableIDs[i]}]
		if epoch != nil && epoch.state == pkMoveGenesisOwned {
			return true
		}
	}
	return false
}

// cancelGenesisRegistrationsWithoutLinksLocked drops the staged identity row
// and the map entry — one destruction step, no resting CANCELED state — for
// every OWNED address whose registration this transaction staged, whose
// staged links all just died in the prune, and for which the first-touch
// committed-link probe found no surviving committed links (D161(A.1)).
// Registrations staged by other transactions are never touched (their epochs
// live on their own transactions). Runs only at settled points — successful
// statement boundaries and the final commit prune — never from a statement
// that could still roll back, so the destruction needs no undo journal.
// Must be called with txn.Lock held.
func (txn *Transaction) cancelGenesisRegistrationsWithoutLinksLocked() {
	if len(txn.pkMoveGenesisStates) == 0 {
		return
	}
	// Surviving staged link rows per address after the prune decision.
	live := make(map[pkMoveAcctTbl]bool)
	for i := range txn.writes {
		we := &txn.writes[i]
		if we.typ != INSERT || we.bat == nil || we.bat.RowCount() == 0 ||
			!isPkMoveLinkTableInsert(we.databaseName, we.tableName) {
			continue
		}
		tableIDIdx := -1
		for j, attr := range we.bat.Attrs {
			if attr == catalog.DataBranchUpdateLinkColTableID {
				tableIDIdx = j
				break
			}
		}
		if tableIDIdx < 0 || tableIDIdx >= len(we.bat.Vecs) ||
			we.bat.Vecs[tableIDIdx].GetType().Oid != types.T_uint64 ||
			we.bat.Vecs[tableIDIdx].Length() < we.bat.RowCount() {
			continue
		}
		tableIDs := vector.MustFixedColNoTypeCheck[uint64](we.bat.Vecs[tableIDIdx])
		for r := 0; r < we.bat.RowCount(); r++ {
			live[pkMoveAcctTbl{account: we.accountId, table: tableIDs[r]}] = true
		}
	}
	var cancels []pkMoveAcctTbl
	for addr, epoch := range txn.pkMoveGenesisStates {
		if epoch.state != pkMoveGenesisOwned || epoch.committedLinks || live[addr] {
			continue
		}
		cancels = append(cancels, addr)
	}
	for _, addr := range cancels {
		for i := range txn.writes {
			we := &txn.writes[i]
			if we.typ != INSERT || we.bat == nil || we.bat.RowCount() == 0 ||
				!isPkMoveRetentionTableInsert(we.databaseName, we.tableName) {
				continue
			}
			tableIDIdx := -1
			for j, attr := range we.bat.Attrs {
				if attr == catalog.DataBranchLinkRetentionColTableID {
					tableIDIdx = j
					break
				}
			}
			if tableIDIdx < 0 || tableIDIdx >= len(we.bat.Vecs) ||
				we.bat.Vecs[tableIDIdx].GetType().Oid != types.T_uint64 ||
				we.bat.Vecs[tableIDIdx].Length() < we.bat.RowCount() {
				continue
			}
			tableIDs := vector.MustFixedColNoTypeCheck[uint64](we.bat.Vecs[tableIDIdx])
			var keep []int64
			for r := 0; r < we.bat.RowCount(); r++ {
				if we.accountId == addr.account && tableIDs[r] == addr.table {
					continue
				}
				keep = append(keep, int64(r))
			}
			if len(keep) == we.bat.RowCount() {
				continue
			}
			txn.unaccountWorkspaceEntryLocked(we)
			we.bat.Shrink(keep, false)
			we.bat.SetRowCount(we.bat.Vecs[0].Length())
			txn.accountWorkspaceEntryLocked(we)
			delete(txn.batchSelectList, we.bat)
		}
		delete(txn.pkMoveGenesisStates, addr)
	}
}

// unwindPkMoveGenesisJournalLocked inverts every transition the rolled-back
// statement journaled; the staged rows die with the positional workspace
// cut. Mirrors the statement-tagged undo discipline of the surrounding
// provenance ledgers (`seq > txn.statementID` after the caller's statement
// decrement). The inversion walks the journal in REVERSE order: a statement
// that registered an address and later retired it (register then retire)
// must end at the pre-statement ABSENT, so the retire's restore applies
// first and the register's delete erases it — a forward pass would wrongly
// leave the statement-staged epoch standing. Must be called with txn.Lock
// held.
func (txn *Transaction) unwindPkMoveGenesisJournalLocked() {
	if len(txn.pkMoveGenesisJournal) == 0 {
		return
	}
	kept := make([]pkMoveGenesisJournalEntry, 0, len(txn.pkMoveGenesisJournal))
	for _, entry := range txn.pkMoveGenesisJournal {
		if entry.seq <= txn.statementID {
			kept = append(kept, entry)
		}
	}
	for i := len(txn.pkMoveGenesisJournal) - 1; i >= 0; i-- {
		entry := txn.pkMoveGenesisJournal[i]
		if entry.seq <= txn.statementID {
			continue
		}
		switch entry.kind {
		case pkMoveGenesisJournalRegister:
			delete(txn.pkMoveGenesisStates, entry.addr)
		case pkMoveGenesisJournalRetire, pkMoveGenesisJournalRefresh:
			if entry.hasPrev {
				prev := entry.prev
				txn.pkMoveGenesisStates[entry.addr] = &prev
			} else {
				delete(txn.pkMoveGenesisStates, entry.addr)
			}
		}
	}
	txn.pkMoveGenesisJournal = kept
}
