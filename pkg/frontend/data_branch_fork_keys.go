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
	snapshot                types.TS
	parentOwner, childOwner uint32
}

// A branch created from a transaction-local table has no committed snapshot
// of its fork image. Record its initial key membership in the clone
// transaction, before later statements can compact inherited rows away.
func captureDataBranchForkKeys(
	ctx context.Context, ses *Session, bh BackgroundExec, receipt *cloneReceipt,
	txn TxnOperator, destination *plan.TableDef,
) error {
	if receipt.snapshot != nil || destination.Pkey == nil || destination.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return nil
	}
	sourceCtx := defines.AttachAccountId(ctx, receipt.srcAccount)
	_, _, source, err := ses.proc.GetSessionInfo().StorageEngine.GetRelationById(sourceCtx, txn, receipt.srcTableID)
	if err != nil {
		return err
	}
	creation, ok := source.(engine.RelationCreatedInCurrentTxn)
	if !ok {
		return nil
	}
	uncommitted, err := creation.CreatedInCurrentTxn(sourceCtx)
	if err != nil || !uncommitted {
		return err
	}

	ownerCtx := context.WithValue(defines.AttachAccountId(ctx, receipt.toAccount), defines.BranchForkKeyAccess{}, true)
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
	ledger := catalog.MO_CATALOG + "." + catalog.MO_BRANCH_FORK_KEYS
	bh.ClearExecResultSet()
	if err := bh.Exec(ownerCtx, fmt.Sprintf("insert into %s (table_id,key_bytes) select %d,%s from %s.%s",
		ledger, receipt.dstTableID, pk, sqlquote.Ident(receipt.dstDb), sqlquote.Ident(receipt.dstTbl))); err != nil {
		return err
	}
	return bh.Exec(ownerCtx, fmt.Sprintf("insert into %s (table_id,key_bytes) values (%d,null)", ledger, receipt.dstTableID))
}

// lookupDataBranchForkKeys returns indexes in the input key vector. The
// marker distinguishes an empty captured fork from a legacy fork.
func lookupDataBranchForkKeys(
	ctx context.Context, ses *Session, bh BackgroundExec, tableID uint64,
	keys *vector.Vector, composite bool,
) (found []int64, recorded bool, err error) {
	if !composite {
		if keys.GetType().Oid == types.T_enum {
			ordinals := vector.NewVec(types.T_uint16.ToType(), ses.proc.Mp())
			defer ordinals.Free(ses.proc.Mp())
			for row := range keys.Length() {
				ordinal := vector.GetFixedAtNoTypeCheck[types.Enum](keys, row)
				if err := vector.AppendFixed(ordinals, uint16(ordinal), false, ses.proc.Mp()); err != nil {
					return nil, false, err
				}
			}
			keys = ordinals
		}
		keys, err = function.RunFunctionDirectly(ses.proc, function.SerialFunctionEncodeID, []*vector.Vector{keys}, keys.Length())
		if err != nil {
			return nil, false, err
		}
		defer keys.Free(ses.proc.Mp())
	}
	ctx = context.WithValue(defines.AttachAccountId(ctx, catalog.System_Account), defines.BranchForkKeyAccess{}, true)
	ledger := catalog.MO_CATALOG + "." + catalog.MO_BRANCH_FORK_KEYS
	prefix := fmt.Sprintf("select key_bytes from %s where table_id=%d and (key_bytes is null or key_bytes in (", ledger, tableID)
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
			return nil, false, err
		}
		present := make(map[string]struct{}, hi-lo)
		result.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				if cols[0].IsNull(uint64(i)) {
					recorded = true
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
	return found, recorded, nil
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
	unknown := false
	checked := h.forks[:0]
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

		found, recorded, err := lookupDataBranchForkKeys(ctx, ses, bh, fork.child, keys, tables.def.pkKind == compositeKind)
		if err != nil {
			return false, err
		}
		if !recorded {
			// Ordinary forks have a committed parent before-image. A legacy
			// fork without either source of evidence cannot classify this key.
			sourceOwner, _ := tables.endpointIdentities(ctx, ses)
			candidates := dedupeBranchIdentities(
				localOperandIdentity(ctx, fork.parentOwner), localOperandIdentity(ctx, fork.childOwner))
			for _, ident := range []branchIdentity{owner, sourceOwner} {
				if !slices.ContainsFunc(candidates, func(other branchIdentity) bool { return other.account == ident.account }) {
					candidates = append(candidates, ident)
				}
			}
			_, _, err = reopenDataBranchNodeByID(ctx, ses, bh, candidates, fork.parent, fork.snapshot)
			if err == nil {
				continue
			}
			if !isMissingDataBranchRelationByID(err) {
				return false, err
			}
			unknown = true
		}
		checked = append(checked, fork)
		for _, i := range found {
			if !matches[i] {
				matches[i] = true
				matched++
			}
		}
	}
	h.forks = checked
	if unknown && matched != keys.Length() {
		return false, moerr.NewNotSupportedNoCtx("DATA BRANCH PICK cannot distinguish an inherited deletion from an absent key without fork history")
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
