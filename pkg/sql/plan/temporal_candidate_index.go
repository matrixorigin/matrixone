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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// makeTemporalIndexHint constructs a necessary physical filter from the atom
// just proved by temporalIndexOperands. The caller owns placement, cost and the
// final relation/transaction proof; this filter never replaces the SQL residual.
func makeTemporalIndexHint(ctx context.Context, atom temporalCandidateAtom, index *plan.IndexDef,
	indexTable *plan.TableDef, indexTag int32,
) (*plan.Expr, bool, error) {
	if indexTable == nil || len(indexTable.Cols) == 0 || indexTable.Cols[0] == nil {
		return nil, false, nil
	}
	physical := GetColExpr(indexTable.Cols[0].Typ, indexTag, 0)
	physical.GetCol().Name = indexTable.Cols[0].Name
	var hint *plan.Expr
	var err error
	if indexTableStoresSerializedKey(index) {
		if types.T(physical.Typ.Id) != types.T_varchar {
			return nil, false, nil
		}
		hint, err = makeEncodedTemporalCandidate(ctx, physical, atom.column, atom.bound, atom.op, atom.scale)
	} else {
		if physical.Typ.Id != atom.column.Typ.Id || physical.Typ.Scale != atom.column.Typ.Scale {
			return nil, false, nil
		}
		hint, err = makeDirectTemporalCandidate(ctx, physical, atom.bound, atom.op, atom.scale)
	}
	if err != nil || hint == nil {
		return nil, false, err
	}
	hint.CandidateFilter = true
	return hint, true, nil
}

// temporalIndexOperands rechecks the current path and index at publication.
// Catalog resolution may reenter planning, so an earlier proof is not enough.
func temporalIndexOperands(ctx context.Context, query *plan.Query, scanID int32, index *plan.IndexDef,
	filterPos int,
) (temporalCandidateAtom, bool, error) {
	if ctx == nil {
		return temporalCandidateAtom{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return temporalCandidateAtom{}, false, err
	}
	if !temporalCandidateSimplePathSafe(query, scanID) {
		return temporalCandidateAtom{}, false, nil
	}
	scan := query.Nodes[scanID]
	if len(scan.FilterList) != 1 || filterPos != 0 {
		return temporalCandidateAtom{}, false, nil
	}
	atom, ok := temporalCandidateAtomForExpr(scan.FilterList[filterPos])
	if !ok || !temporalIndexMatches(scan, index, atom) {
		return temporalCandidateAtom{}, false, nil
	}
	return atom, true, nil
}

// temporalIndexMatches checks only index-specific facts. Enumeration already
// proved the current path and atom once; publication repeats the full proof.
func temporalIndexMatches(scan *plan.Node, index *plan.IndexDef, atom temporalCandidateAtom) bool {
	if scan == nil || scan.TableDef == nil || scan.TableDef.Pkey == nil || len(scan.BindingTags) == 0 ||
		index == nil || !index.TableExist || !catalog.IsRegularIndexAlgo(index.IndexAlgo) || isSpatialIndexDef(index) ||
		index.KeyFormat != 0 || len(index.Parts) == 0 || len(index.Parts) > 16 || regularIndexHasDeclaredPrefix(index) {
		return false
	}
	for _, part := range index.Parts {
		pos, exists := scan.TableDef.Name2ColIndex[catalog.ResolveAlias(part)]
		if !exists || pos < 0 || int(pos) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[pos] == nil {
			return false
		}
	}
	if indexUsesUnicodeCollation(index, scan.TableDef) || functionalIndexColumn(scan.TableDef, index) != nil || !hintedIndexContainsQualifyingRows(index, scan) {
		return false
	}
	position, ok := scan.TableDef.Name2ColIndex[catalog.ResolveAlias(index.Parts[0])]
	if !ok || position < 0 || int(position) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[position] == nil {
		return false
	}
	return atom.column.GetCol().RelPos == scan.BindingTags[0] && atom.column.GetCol().ColPos == position &&
		atom.column.Typ.Id == scan.TableDef.Cols[position].Typ.Id && atom.column.Typ.Scale == scan.TableDef.Cols[position].Typ.Scale
}
