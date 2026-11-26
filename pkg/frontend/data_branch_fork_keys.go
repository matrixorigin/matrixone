// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type dataBranchFork struct {
	child, parent           uint64
	snapshot, childUntil    types.TS
	parentOwner, childOwner uint32
}

// A clone can include transaction-local rows even if its parent was already
// committed. Record its initial key membership in the clone transaction,
// before later statements can compact inherited rows away.
func captureDataBranchForkKeys(
	ctx context.Context, ses *Session, bh BackgroundExec, receipt *cloneReceipt,
	txn TxnOperator, destination *plan.TableDef,
) error {
	if destination.Pkey == nil || destination.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return nil
	}
	uncommitted, err := forkSourceHasUncommittedData(ctx, ses, receipt, txn)
	if err != nil {
		return err
	}

	ownerCtx := context.WithValue(defines.AttachAccountId(ctx, receipt.toAccount), defines.BranchForkKeyAccess{}, true)
	ledger := catalog.MO_CATALOG + "." + catalog.MO_BRANCH_FORK_KEYS
	bh.ClearExecResultSet()
	if uncommitted {
		pk := sqlquote.Ident(destination.Pkey.PkeyColName)
		if destination.Pkey.CompPkeyCol == nil {
			column := dataBranchColumnDefByName(destination, destination.Pkey.PkeyColName)
			if types.T(column.Typ.Id) == types.T_enum {
				// SQL serial normally binds ENUM to its display string; the key
				// probe uses the stored ordinal.
				pk = "cast(" + pk + " as smallint unsigned)"
			}
			pk = "serial(" + pk + ")"
		}
		if err := bh.Exec(ownerCtx, fmt.Sprintf("insert into %s (table_id,key_bytes) select %d,%s from %s.%s",
			ledger, receipt.dstTableID, pk, sqlquote.Ident(receipt.dstDb), sqlquote.Ident(receipt.dstTbl))); err != nil {
			return err
		}
	}
	return bh.Exec(ownerCtx, fmt.Sprintf("insert into %s (table_id,key_bytes,full_image) values (%d,null,%t)", ledger, receipt.dstTableID, uncommitted))
}

func forkSourceHasUncommittedData(
	ctx context.Context, ses *Session, receipt *cloneReceipt, txn TxnOperator,
) (bool, error) {
	if receipt.snapshot != nil {
		return false, nil
	}
	sourceCtx := defines.AttachAccountId(ctx, receipt.srcAccount)
	_, _, source, err := ses.proc.GetSessionInfo().StorageEngine.GetRelationById(sourceCtx, txn, receipt.srcTableID)
	if err != nil {
		return false, err
	}
	if creation, ok := source.(engine.RelationCreatedInCurrentTxn); ok {
		created, err := creation.CreatedInCurrentTxn(sourceCtx)
		if err != nil || created {
			return created, err
		}
	}
	if workspace, ok := source.(engine.RelationWithUncommittedData); ok {
		return workspace.HasUncommittedData()
	}
	return false, nil
}

func encodeDataBranchForkKeys(ses *Session, keys *vector.Vector, composite bool) (*vector.Vector, error) {
	if composite {
		return keys, nil
	}
	if keys.GetType().Oid == types.T_enum {
		ordinals := vector.NewVec(types.T_uint16.ToType(), ses.proc.Mp())
		defer ordinals.Free(ses.proc.Mp())
		for row := range keys.Length() {
			ordinal := vector.GetFixedAtNoTypeCheck[types.Enum](keys, row)
			if err := vector.AppendFixed(ordinals, uint16(ordinal), false, ses.proc.Mp()); err != nil {
				return nil, err
			}
		}
		keys = ordinals
	}
	return function.RunFunctionDirectly(ses.proc, function.SerialFunctionEncodeID, []*vector.Vector{keys}, keys.Length())
}

// lookupDataBranchForkKeys returns indexes in the input key vector. The
// marker distinguishes an empty captured fork from a legacy fork.
func lookupDataBranchForkKeys(
	ctx context.Context, ses *Session, bh BackgroundExec, tableID uint64,
	keys *vector.Vector, composite bool,
) (found []int64, recorded, fullImage bool, err error) {
	raw := keys
	keys, err = encodeDataBranchForkKeys(ses, keys, composite)
	if err != nil {
		return nil, false, false, err
	}
	if keys != raw {
		defer keys.Free(ses.proc.Mp())
	}
	ctx = context.WithValue(defines.AttachAccountId(ctx, catalog.System_Account), defines.BranchForkKeyAccess{}, true)
	ledger := catalog.MO_CATALOG + "." + catalog.MO_BRANCH_FORK_KEYS
	prefix := fmt.Sprintf("select key_bytes,full_image from %s where table_id=%d and (key_bytes is null or key_bytes in (", ledger, tableID)
	for lo := 0; lo < keys.Length(); {
		hi := lo
		var query strings.Builder
		query.WriteString(prefix)
		for hi < keys.Length() {
			key := keys.GetBytesAt(hi)
			if hi > lo && (hi-lo >= dataBranchMetadataIDBatchSize || query.Len()+2*len(key)+4 > maxSqlBatchSize) {
				break
			}
			if hi != lo {
				query.WriteByte(',')
			}
			query.WriteString("x'")
			query.WriteString(hex.EncodeToString(key))
			query.WriteByte('\'')
			hi++
		}
		query.WriteString("))")
		result, err := runSql(ctx, ses, bh, query.String(), nil, nil)
		if err != nil {
			return nil, false, false, err
		}
		present := make(map[string]struct{}, hi-lo)
		result.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				if cols[0].IsNull(uint64(i)) {
					recorded = true
					fullImage = vector.GetFixedAtNoTypeCheck[bool](cols[1], i)
				} else {
					present[string(cols[0].GetBytesAt(i))] = struct{}{}
				}
			}
			return true
		})
		result.Close()
		for i := lo; i < hi; i++ {
			if _, ok := present[string(keys.GetBytesAt(i))]; ok {
				found = append(found, int64(i))
			}
		}
		lo = hi
	}
	return found, recorded, fullImage, nil
}

// pickForkHistory is scoped to a protected PICK. For missing destination
// keys, inspect every relevant logical fork, including intermediate children
// created and dropped in the same transaction.
type pickForkHistory struct {
	branches []uint64
	forks    []dataBranchFork
}

func (h *pickForkHistory) emitMissingConflicts(
	ctx context.Context, ses *Session, bh BackgroundExec, tables tableStuff, source *batch.Batch,
	keys *vector.Vector, fromUpdate bool, emit emitFunc,
) (bool, error) {
	// The destination change map need not contain unchanged inherited rows.
	// Probe the live endpoint before interpreting any absence as a deletion.
	_, owner := tables.endpointIdentities(ctx, ses)
	_, snapshot := tables.resolvedSnapshots(ses)
	_, openedAt := tables.resolvedRelSnapshots(ses)
	probe := dataBranchHistoricalProbeStuff(ctx, tables, tables.baseRel)
	probe.lcaIdent, probe.lcaRelSnapshot = &owner, openedAt
	rows, missing, err := probeDataBranchRows(ctx, ses, bh, keys, probe, snapshot.ToTimestamp())
	if err != nil {
		return false, err
	}
	tables.retPool.releaseRetBatch(rows, false)
	if len(missing) == 0 {
		return false, nil
	}
	if len(missing) != keys.Length() {
		if err = keys.Shrink(missing, false, ses.proc.Mp()); err != nil {
			return false, err
		}
	}

	proof := newDataBranchRenameProof(tables.pickForkColumns)
	matches := make([]bool, keys.Length())
	matched := 0
	uncertain := make([]bool, keys.Length())
	for _, fork := range h.forks {
		// A transaction-local intermediate can be absent from the catalog.
		// Its first visible descendant supplies the key layout for that path.
		start := slices.Index(h.branches, fork.child)
		var first *plan.TableDef
		for _, id := range h.branches[start:] {
			i := slices.IndexFunc(tables.pickBaseDefs, func(def *plan.TableDef) bool { return def.TblId == id })
			if i >= 0 {
				first = tables.pickBaseDefs[i]
				break
			}
		}
		if first != nil && len(historicalPickPrimaryKeyNames(first, tables.pickBaseDefs, tables.pickBaseEdges, proof)) == 0 {
			continue
		}

		found, recorded, full, err := lookupDataBranchForkKeys(ctx, ses, bh, fork.child, keys, tables.def.pkKind == compositeKind)
		if err != nil {
			return false, err
		}
		if !full {
			found, err = historicalForkMembers(ctx, ses, bh, tables, fork, keys, uncertain, proof, recorded, fromUpdate)
			if err != nil {
				return false, err
			}
		}
		for _, i := range found {
			if !matches[i] {
				matches[i] = true
				matched++
			}
		}
	}
	for i, ambiguous := range uncertain {
		if ambiguous && !matches[i] {
			return false, moerr.NewNotSupportedNoCtx("DATA BRANCH PICK cannot distinguish an inherited deletion from an absent key without fork history")
		}
	}
	if matched == 0 {
		return false, nil
	}

	// Missing keys retain the order of the original source rows. Use the
	// source batch's complete key layout for the existing conflict markers.
	pk := source.Vecs[tables.def.pkColIdx]
	selected := make([]int64, 0, matched)
	next := 0
	for i := 0; i < pk.Length() && next < keys.Length(); i++ {
		cmp, err := compareDataBranchPrimaryKeyInVectors(ctx, ses, i, next, pk, keys)
		if err != nil {
			return false, err
		}
		if cmp == 0 {
			if matches[next] {
				selected = append(selected, int64(i))
			}
			next++
		}
	}
	if len(selected) != matched {
		return false, moerr.NewInternalErrorNoCtx("data branch fork evidence has no matching source row")
	}
	markers := tables.retPool.acquireRetBatch(tables, false, ses.proc.Mp())
	if err := markers.Union(source, selected, ses.proc.Mp()); err != nil {
		tables.retPool.releaseRetBatch(markers, false)
		return false, err
	}
	return emitBatch(emit, batchWithKind{
		name: tables.baseRel.GetTableName(), kind: diffDelete, side: diffSideBase,
		batch: markers, hasReplacement: true, fromUpdate: true,
	}, false, tables.retPool)
}

// A clean fork reads its committed parent image. A legacy fork from a
// committed parent can still expose writes made in the clone transaction.
// Observe membership across the child's exact commit to detect those keys.
func historicalForkMembers(
	ctx context.Context, ses *Session, bh BackgroundExec, tables tableStuff,
	fork dataBranchFork, keys *vector.Vector, uncertain []bool, proof dataBranchRenameProof,
	recorded, fromUpdate bool,
) ([]int64, error) {
	source, destination := tables.endpointIdentities(ctx, ses)
	candidates := dedupeBranchIdentities(localOperandIdentity(ctx, fork.parentOwner), localOperandIdentity(ctx, fork.childOwner))
	for _, ident := range []branchIdentity{destination, source} {
		if !slices.ContainsFunc(candidates, func(other branchIdentity) bool { return other.account == ident.account }) {
			candidates = append(candidates, ident)
		}
	}
	markUnknown := func() {
		// Without a before-image, an update can restore a missing key.
		// A pure new insert needs some historical evidence of that key.
		if fromUpdate {
			for i := range uncertain {
				uncertain[i] = true
			}
		}
	}
	_, parentOwner, err := reopenDataBranchNodeByID(ctx, ses, bh, candidates, fork.parent, fork.snapshot)
	if err != nil && !isMissingDataBranchRelationByID(err) {
		return nil, err
	}
	until := fork.childUntil
	if until.IsEmpty() {
		_, until = tables.resolvedSnapshots(ses)
	}
	var commits []types.TS
	if !recorded {
		child, owner, childErr := reopenDataBranchNodeByID(ctx, ses, bh, candidates, fork.child, until)
		if isMissingDataBranchRelationByID(childErr) {
			markUnknown()
			return nil, nil
		}
		if childErr != nil {
			return nil, childErr
		}
		commits, childErr = getTablesCreationCommitTS(ctx, ses, owner, child, owner, child, []types.TS{until, until})
		if childErr != nil {
			return nil, childErr
		}
	}
	probe := func(identity branchIdentity, at types.TS) (missing []int64, available bool, err error) {
		start := dataBranchPickWindowStart{tableID: fork.parent, snapshot: at, identity: identity}
		rows, absent, err := probePickKeyMembership(ctx, ses, bh, tables, keys, start, tables.pickBaseDefs, tables.pickBaseEdges, proof)
		if isMissingDataBranchRelationByID(err) {
			return nil, false, nil
		}
		if rows != nil {
			tables.retPool.releaseRetBatch(rows, false)
		}
		return absent, rows != nil, err
	}
	if err != nil {
		markUnknown()
		if !fromUpdate && !recorded {
			if _, ident, openErr := reopenDataBranchNodeByID(ctx, ses, bh, candidates, fork.parent, commits[0]); openErr == nil {
				missing, available, probeErr := probe(ident, commits[0])
				if probeErr != nil {
					return nil, probeErr
				}
				for i := 0; available && i < keys.Length(); i++ {
					if _, absent := slices.BinarySearch(missing, int64(i)); !absent {
						uncertain[i] = true
					}
				}
			} else if !isMissingDataBranchRelationByID(openErr) {
				return nil, openErr
			}
		}
		return nil, nil
	}
	atFork, available, err := probe(parentOwner, fork.snapshot)
	if err != nil {
		return nil, err
	}
	if !available {
		markUnknown()
		return nil, nil
	}
	touched := make([]bool, keys.Length())
	if !recorded {
		if err := legacyForkParentChanges(ctx, ses, bh, fork.parent, parentOwner, commits[0], keys,
			tables.def.pkKind == compositeKind, touched); err != nil {
			return nil, err
		}
	}

	var found []int64
	for i := range keys.Length() {
		key := int64(i)
		if touched[i] {
			uncertain[i] = true
		} else if _, absent := slices.BinarySearch(atFork, key); !absent {
			found = append(found, key)
		} else if !recorded && fromUpdate {
			// An insert followed by a delete in the parent workspace can
			// disappear from committed history even if the clone inherited
			// the key between those writes.
			uncertain[i] = true
		}
	}
	return found, nil
}

// Inspect versions written by the child's transaction. Membership can be
// unchanged across the commit when the parent deleted and reinserted a key.
func legacyForkParentChanges(
	ctx context.Context, ses *Session, bh BackgroundExec, parent uint64, owner branchIdentity, commit types.TS,
	keys *vector.Vector, composite bool, touched []bool,
) error {
	rel, ident, err := reopenDataBranchNodeByID(ctx, ses, bh, []branchIdentity{owner}, parent, commit)
	if isMissingDataBranchRelationByID(err) {
		for i := range touched {
			touched[i] = true
		}
		return nil
	}
	if err != nil {
		return err
	}
	ctx = identityCtx(ctx, ident)
	def := rel.GetTableDef(ctx)
	if def.Pkey == nil {
		return moerr.NewInternalErrorNoCtx("legacy fork parent has no primary key")
	}
	keyColumn := 1 // Leading row ID in change data.
	for _, col := range def.Cols {
		if col.Name == def.Pkey.PkeyColName {
			break
		}
		if col.Name != catalog.Row_ID {
			keyColumn++
		}
	}
	packed, err := encodeDataBranchForkKeys(ses, keys, composite)
	if err != nil {
		return err
	}
	mp := ses.proc.Mp()
	if packed != keys {
		defer packed.Free(mp)
	}
	selected := make(map[string]int, keys.Length())
	for row := range keys.Length() {
		selected[string(packed.GetBytesAt(row))] = row
	}
	mark := func(values *vector.Vector) error {
		bytes, err := encodeDataBranchForkKeys(ses, values, composite)
		if err != nil {
			return err
		}
		if bytes != values {
			defer bytes.Free(mp)
		}
		for i := range bytes.Length() {
			if row, found := selected[string(bytes.GetBytesAt(i))]; found {
				touched[row] = true
			}
		}
		return nil
	}
	ctx = engine.WithRetainRowID(ctx, true)
	ctx = engine.WithCollectChangesPreserveAllVersions(ctx)
	ctx = engine.WithCollectChangesKeepTrailingDeletes(ctx)
	changes, err := rel.CollectChanges(ctx, commit, commit, false, mp)
	if err != nil || changes == nil {
		return err
	}
	defer changes.Close()
	for {
		data, deleted, _, err := changes.Next(ctx, mp)
		if err != nil || data == nil && deleted == nil {
			return err
		}
		if data != nil {
			if data.RowCount() != 0 {
				err = mark(data.Vecs[keyColumn])
			}
			data.Clean(mp)
		}
		if deleted != nil {
			if err == nil && deleted.RowCount() != 0 {
				err = mark(deleted.Vecs[deleted.VectorCount()-2])
			}
			deleted.Clean(mp)
		}
		if err != nil {
			return err
		}
	}
}

// A fork comparison can synthesize a deletion using a row that was removed
// from the parent's workspace before the clone. Keep real inherited
// deletions. The handle is scoped to the fork's creation-commit comparison;
// subsequent destination tombstones come from other handles.
type forkEvidenceTombstones struct {
	engine.ChangesHandle
	ses       *Session
	bh        BackgroundExec
	childID   uint64
	composite bool
}

func (h *forkEvidenceTombstones) Next(ctx context.Context, mp *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	data, deleted, hint, err := h.ChangesHandle.Next(ctx, mp)
	if err != nil || deleted == nil || deleted.RowCount() == 0 {
		return data, deleted, hint, err
	}
	// The primary key immediately precedes the commit timestamp, with
	// or without a leading row ID.
	keys := deleted.Vecs[deleted.VectorCount()-2]
	found, _, full, err := lookupDataBranchForkKeys(ctx, h.ses, h.bh, h.childID, keys, h.composite)
	if err == nil && full && len(found) != deleted.RowCount() {
		err = deleted.Shrink(found, false, mp)
	}
	if err != nil {
		if data != nil {
			data.Clean(mp)
		}
		deleted.Clean(mp)
		return nil, nil, hint, err
	}
	return data, deleted, hint, nil
}
