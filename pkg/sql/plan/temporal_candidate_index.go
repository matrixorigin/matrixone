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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// temporalIndexHint builds and marks a leading-component candidate only after
// the original plan and index pass the admission checks. paramFree is a proof
// from the pre-binding owner, not an inference from materialized literals.
// Callers must separately establish access-path cost and placement permission.
func temporalIndexHint(ctx context.Context, query *plan.Query, scanID int32, paramFree bool, index *plan.IndexDef,
	indexTable *plan.TableDef, indexTag int32, filterPos int,
) (*plan.Expr, bool, error) {
	return temporalIndexHintWithPrefix(ctx, query, scanID, paramFree, index, indexTable, indexTag, nil, filterPos)
}

// temporalIndexHintWithPrefix also admits a proven exact equality prefix.
// Each prefix position is an original filter, ordered by index component.
func temporalIndexHintWithPrefix(ctx context.Context, query *plan.Query, scanID int32, paramFree bool, index *plan.IndexDef,
	indexTable *plan.TableDef, indexTag int32, prefixPositions []int, filterPos int,
) (*plan.Expr, bool, error) {
	atom, prefix, admitted, err := temporalIndexOperands(ctx, query, scanID, paramFree, index, prefixPositions, filterPos)
	if err != nil || !admitted {
		return nil, false, err
	}
	if indexTable == nil || len(indexTable.Cols) == 0 || indexTable.Cols[0] == nil {
		return nil, false, nil
	}
	physical := GetColExpr(indexTable.Cols[0].Typ, indexTag, 0)
	physical.GetCol().Name = indexTable.Cols[0].Name
	var hint, guard *plan.Expr
	if indexTableStoresSerializedKey(index) {
		if types.T(physical.Typ.Id) != types.T_varchar {
			return nil, false, nil
		}
		hint, guard, err = makeEncodedTemporalCandidate(ctx, physical, atom.column, atom.bound, prefix, len(index.Parts) > len(prefix)+1, atom.op, atom.scale)
	} else {
		if physical.Typ.Id != atom.column.Typ.Id || physical.Typ.Scale != atom.column.Typ.Scale {
			return nil, false, nil
		}
		hint, guard, err = makeDirectTemporalCandidate(ctx, physical, atom.bound, atom.op, atom.scale)
	}
	if err != nil {
		return nil, false, err
	}
	hint.TemporalCandidateFilter = true
	guard.TemporalCandidateGuard = true
	return hint, true, nil
}

// temporalIndexOperands proves the original owner, index completeness and
// exact prefix before optional catalog resolution or candidate construction.
func temporalIndexOperands(ctx context.Context, query *plan.Query, scanID int32, paramFree bool, index *plan.IndexDef,
	prefixPositions []int, filterPos int,
) (temporalCandidateAtom, []*plan.Expr, bool, error) {
	if ctx == nil {
		return temporalCandidateAtom{}, nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return temporalCandidateAtom{}, nil, false, err
	}
	if !paramFree || !temporalCandidateSimplePathSafe(query, scanID) || index == nil || !index.TableExist ||
		!catalog.IsRegularIndexAlgo(index.IndexAlgo) || isSpatialIndexDef(index) || index.KeyFormat != 0 || len(prefixPositions) >= len(index.Parts) ||
		regularIndexHasDeclaredPrefix(index) {
		return temporalCandidateAtom{}, nil, false, nil
	}
	scan := query.Nodes[scanID]
	if scan.TableDef == nil || scan.TableDef.Pkey == nil || len(scan.BindingTags) == 0 || filterPos < 0 || filterPos >= len(scan.FilterList) {
		return temporalCandidateAtom{}, nil, false, nil
	}
	for _, part := range index.Parts {
		pos, exists := scan.TableDef.Name2ColIndex[catalog.ResolveAlias(part)]
		if !exists || pos < 0 || int(pos) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[pos] == nil {
			return temporalCandidateAtom{}, nil, false, nil
		}
	}
	if indexUsesUnicodeCollation(index, scan.TableDef) || functionalIndexColumn(scan.TableDef, index) != nil || !hintedIndexContainsQualifyingRows(index, scan) {
		return temporalCandidateAtom{}, nil, false, nil
	}
	prefix := make([]*plan.Expr, 0, len(prefixPositions))
	for part, filter := range prefixPositions {
		normalized, ok, err := temporalIndexPrefixBound(ctx, scan, index, part, filter)
		if err != nil || !ok {
			return temporalCandidateAtom{}, nil, false, err
		}
		prefix = append(prefix, normalized)
	}
	position, ok := scan.TableDef.Name2ColIndex[catalog.ResolveAlias(index.Parts[len(prefixPositions)])]
	if !ok || position < 0 || int(position) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[position] == nil {
		return temporalCandidateAtom{}, nil, false, nil
	}
	atom, ok := temporalCandidateAtomForExpr(scan.FilterList[filterPos])
	if !ok || atom.column.GetCol().RelPos != scan.BindingTags[0] || atom.column.GetCol().ColPos != position ||
		atom.column.Typ.Id != scan.TableDef.Cols[position].Typ.Id || atom.column.Typ.Scale != scan.TableDef.Cols[position].Typ.Scale {
		return temporalCandidateAtom{}, nil, false, nil
	}
	return atom, prefix, true, nil
}

// temporalIndexPrefixBound validates one equality independently so discovery
// and construction share exactly the same encoded-key admission rules.
func temporalIndexPrefixBound(ctx context.Context, scan *plan.Node, index *plan.IndexDef, part, filter int) (*plan.Expr, bool, error) {
	if ctx == nil || scan == nil || scan.TableDef == nil || len(scan.BindingTags) == 0 || index == nil ||
		part < 0 || part >= len(index.Parts) || filter < 0 || filter >= len(scan.FilterList) || scan.FilterList[filter] == nil {
		return nil, false, nil
	}
	position, exists := scan.TableDef.Name2ColIndex[catalog.ResolveAlias(index.Parts[part])]
	if !exists || position < 0 || int(position) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[position] == nil {
		return nil, false, nil
	}
	fn := scan.FilterList[filter].GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 ||
		!serializedIndexComparisonUsable(ctx, fn, scan.TableDef.Cols[position].Typ) {
		return nil, false, nil
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	col := indexComparisonColumn(fn)
	if id != function.EQUAL || col == nil || col.GetCol().RelPos != scan.BindingTags[0] || col.GetCol().ColPos != position {
		return nil, false, nil
	}
	bound := fn.Args[1]
	if bound == col {
		bound = fn.Args[0]
	}
	if bound == nil || !indexEqualityPinsSingleValue(col.Typ, bound.Typ) {
		return nil, false, nil
	}
	normalized, err := normalizeIndexComparisonBound(ctx, bound, col.Typ)
	return normalized, err == nil, err
}

type temporalIndexMatch struct {
	prefix []int
	filter int
}

// findTemporalIndexMatch is the first-match convenience for callers which do
// not rank access paths. Costing must compare all discovered temporal bounds.
func findTemporalIndexMatch(ctx context.Context, query *plan.Query, scanID int32, paramFree bool, index *plan.IndexDef) (prefix []int, filter int, found bool, err error) {
	matches, err := findTemporalIndexMatches(ctx, query, scanID, paramFree, index)
	if err != nil || len(matches) == 0 {
		return nil, 0, false, err
	}
	return matches[0].prefix, matches[0].filter, true, nil
}

// findTemporalIndexMatches discovers exact equality prefixes followed by
// temporal predicates without catalog lookup or plan mutation. The limits
// bound work for unusually large filter lists and index keys.
func findTemporalIndexMatches(ctx context.Context, query *plan.Query, scanID int32, paramFree bool, index *plan.IndexDef) ([]temporalIndexMatch, error) {
	if ctx == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !paramFree || !temporalCandidateSimplePathSafe(query, scanID) || index == nil || len(index.Parts) == 0 || len(index.Parts) > 16 {
		return nil, nil
	}
	scan := query.Nodes[scanID]
	if len(scan.FilterList) > 64 {
		return nil, nil
	}
	var matches []temporalIndexMatch
	var prefix []int
	for part := range index.Parts {
		for pos := range scan.FilterList {
			if _, ok := temporalCandidateAtomForExpr(scan.FilterList[pos]); !ok {
				continue
			}
			_, _, ok, err := temporalIndexOperands(ctx, query, scanID, paramFree, index, prefix, pos)
			if err != nil {
				return nil, err
			}
			if ok {
				matches = append(matches, temporalIndexMatch{prefix: append([]int(nil), prefix...), filter: pos})
			}
		}
		match := -1
		for pos := range scan.FilterList {
			_, ok, err := temporalIndexPrefixBound(ctx, scan, index, part, pos)
			if err != nil {
				return nil, err
			}
			if ok {
				match = pos
				break
			}
		}
		if match < 0 {
			break
		}
		prefix = append(prefix, match)
	}
	return matches, nil
}
