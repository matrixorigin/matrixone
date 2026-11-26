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
	"crypto/sha256"
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
	"github.com/matrixorigin/matrixone/pkg/sql/features"
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
	fullImage, err := forkNeedsFullImage(ctx, ses, receipt, txn)
	if err != nil {
		return err
	}

	ownerCtx := context.WithValue(defines.AttachAccountId(ctx, receipt.toAccount), defines.BranchForkKeyAccess{}, true)
	ledger := catalog.MO_CATALOG + "." + catalog.MO_BRANCH_FORK_KEYS
	bh.ClearExecResultSet()
	if fullImage {
		pk := "fork_image." + sqlquote.Ident(destination.Pkey.PkeyColName)
		if destination.Pkey.CompPkeyCol == nil {
			column := dataBranchColumnDefByName(destination, destination.Pkey.PkeyColName)
			pk = "serial(" + forkColumnValueSQL(column) + ")"
		}
		tables := []string{receipt.dstTbl}
		if partitions := ses.proc.GetPartitionService(); partitions.Enabled() && features.IsPartitioned(destination.FeatureFlag) {
			// Read the same physical relations the clone wrote. The logical
			// partition scan can omit their uncommitted objects.
			metadata, err := partitions.GetPartitionMetadata(ownerCtx, receipt.dstTableID, txn)
			if err != nil {
				return err
			}
			if len(metadata.Partitions) == 0 {
				return nil // An empty logical scan cannot certify the fork.
			}
			tables = make([]string, len(metadata.Partitions))
			for i, partition := range metadata.Partitions {
				tables[i] = partition.PartitionTableName
			}
		}
		var join string
		if columns := forkRowColumns(destination); len(columns) != 0 {
			names := make([]string, len(columns))
			for i, col := range columns {
				names[i] = forkColumnValueSQL(col)
			}
			// The row proof includes the entire visible row, so identical
			// writes made independently after a fork cannot hide conflicts.
			pk = fmt.Sprintf("case fork_evidence.kind when 0 then %s else concat(x'%02x',unhex(sha2(serial_full(%s),256))) end",
				pk, dataBranchForkRowProofPrefix, strings.Join(names, ","))
			join = " cross join (select 0 as kind union all select 1) as fork_evidence"
		}
		for _, table := range tables {
			from := sqlquote.Ident(receipt.dstDb) + "." + sqlquote.Ident(table) + " as fork_image" + join
			if err := bh.Exec(ownerCtx, fmt.Sprintf("insert into %s (table_id,key_bytes) select %d,%s from %s",
				ledger, receipt.dstTableID, pk, from)); err != nil {
				return err
			}
		}
	}
	marker := "x''"
	if fullImage {
		marker = "x'fe'"
	}
	return bh.Exec(ownerCtx, fmt.Sprintf("insert into %s (table_id,key_bytes) values (%d,%s)", ledger, receipt.dstTableID, marker))
}

// Packed primary keys start with a tuple type byte. The reserved 0xff
// prefix identifies SHA-256 row proofs without changing the ledger schema.
const dataBranchForkRowProofPrefix = 0xff

func forkColumnValueSQL(col *plan.ColDef) string {
	name := "fork_image." + sqlquote.Ident(col.Name)
	// Use the stored ordinal or bitmap, matching the probe vectors.
	if types.T(col.Typ.Id) == types.T_enum {
		return "cast(" + name + " as smallint unsigned)"
	}
	if col.Typ.Enumvalues != "" {
		return "cast(" + name + " as bigint unsigned)"
	}
	return name
}

func forkRowColumns(def *plan.TableDef) []*plan.ColDef {
	var columns []*plan.ColDef
	for _, col := range def.Cols {
		if !isDataBranchUserVisibleColumn(col) {
			continue
		}
		oid := types.T(col.Typ.Id)
		// Keep key evidence for domains without a common SQL and vector
		// serialization.
		if !function.SerialTypeSupported(oid) {
			return nil
		}
		columns = append(columns, col)
	}
	return columns
}

func lookupDataBranchForkRows(
	ctx context.Context, ses *Session, bh BackgroundExec, forks []uint64, columns []*vector.Vector, length int,
) ([]int64, error) {
	// SQL binds ENUM functions to the display label unless explicitly
	// cast. Use the same ordinal on both sides of the proof.
	for i, col := range columns {
		if col.GetType().Oid != types.T_enum {
			continue
		}
		ordinals, err := dataBranchForkEnumOrdinals(col, ses.proc.Mp())
		if err != nil {
			return nil, err
		}
		defer ordinals.Free(ses.proc.Mp())
		columns[i] = ordinals
	}
	serialized, err := function.RunFunctionDirectly(ses.proc, function.SerialFullFunctionEncodeID, columns, length)
	if err != nil {
		return nil, err
	}
	defer serialized.Free(ses.proc.Mp())
	keys := vector.NewVec(types.T_varbinary.ToType(), ses.proc.Mp())
	defer keys.Free(ses.proc.Mp())
	for row := range length {
		var proof [1 + sha256.Size]byte
		proof[0] = dataBranchForkRowProofPrefix
		sum := sha256.Sum256(serialized.GetBytesAt(row))
		copy(proof[1:], sum[:])
		if err := vector.AppendBytes(keys, proof[:], false, ses.proc.Mp()); err != nil {
			return nil, err
		}
	}
	var inherited []int64
	for i, tableID := range forks {
		found, _, _, err := lookupDataBranchForkKeys(ctx, ses, bh, tableID, keys, true)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			inherited = found
		} else {
			inherited = slices.DeleteFunc(inherited, func(row int64) bool {
				_, present := slices.BinarySearch(found, row)
				return !present
			})
		}
		if len(inherited) == 0 {
			break
		}
	}
	return inherited, nil
}

func forkNeedsFullImage(
	ctx context.Context, ses *Session, receipt *cloneReceipt, txn TxnOperator,
) (bool, error) {
	// A subscriber's fork may outlive the publishing account and its
	// historical catalog. Keep a self-contained image for this edge.
	if receipt.srcAccount != receipt.toAccount {
		return true, nil
	}
	if receipt.snapshot != nil {
		return false, nil
	}
	sourceCtx := defines.AttachAccountId(ctx, receipt.srcAccount)
	_, _, source, err := ses.proc.GetSessionInfo().StorageEngine.GetRelationById(sourceCtx, txn, receipt.srcTableID)
	if err != nil {
		return false, err
	}
	provider, ok := source.(engine.HistoricalSourceProvider)
	if !ok {
		return true, nil
	}
	visible, err := provider.SourceVisibleAtSnapshot(sourceCtx)
	if moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
		// The engine cannot prove a committed image for this relation.
		return true, nil
	}
	return !visible, err
}

func encodeDataBranchForkKeys(ses *Session, keys *vector.Vector, composite bool) (*vector.Vector, error) {
	if composite {
		return keys, nil
	}
	if keys.GetType().Oid == types.T_enum {
		ordinals, err := dataBranchForkEnumOrdinals(keys, ses.proc.Mp())
		if err != nil {
			return nil, err
		}
		defer ordinals.Free(ses.proc.Mp())
		keys = ordinals
	}
	return function.RunFunctionDirectly(ses.proc, function.SerialFunctionEncodeID, []*vector.Vector{keys}, keys.Length())
}

func dataBranchForkEnumOrdinals(values *vector.Vector, mp *mpool.MPool) (*vector.Vector, error) {
	ordinals := vector.NewVec(types.T_uint16.ToType(), mp)
	for row := range values.Length() {
		isNull := values.IsNull(uint64(row))
		var ordinal uint16
		if !isNull {
			ordinal = uint16(vector.GetFixedAtNoTypeCheck[types.Enum](values, row))
		}
		if err := vector.AppendFixed(ordinals, ordinal, isNull, mp); err != nil {
			ordinals.Free(mp)
			return nil, err
		}
	}
	return ordinals, nil
}

// lookupDataBranchForkKeys returns indexes in the input key vector. The
// marker distinguishes an empty captured fork from a legacy fork. Old
// NULL markers cannot prove absence because a partition scan could be empty
// before its clone transaction committed.
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
	prefix := fmt.Sprintf("select key_bytes from %s where table_id=%d and (key_bytes in (x'',x'fe') or key_bytes in (", ledger, tableID)
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
				switch key := string(cols[0].GetBytesAt(i)); key {
				case "\xfe":
					recorded, fullImage = true, true
				case "":
					recorded = true
				default:
					present[key] = struct{}{}
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
	keys *vector.Vector, emit emitFunc,
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
		if !full && len(found) < keys.Length() {
			historical, err := historicalForkMembers(ctx, ses, bh, tables, fork, keys, uncertain, proof, recorded)
			if err != nil {
				return false, err
			}
			found = append(found, historical...)
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
	recorded bool,
) ([]int64, error) {
	source, destination := tables.endpointIdentities(ctx, ses)
	candidates := dedupeBranchIdentities(localOperandIdentity(ctx, fork.parentOwner), localOperandIdentity(ctx, fork.childOwner))
	for _, ident := range []branchIdentity{destination, source} {
		if !slices.ContainsFunc(candidates, func(other branchIdentity) bool { return other.account == ident.account }) {
			candidates = append(candidates, ident)
		}
	}
	markUnknown := func() {
		for i := range uncertain {
			uncertain[i] = true
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
	if err != nil {
		markUnknown()
		return nil, nil
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
		} else if !recorded {
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
