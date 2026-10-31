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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// potentialTemporalIndexAccess detects a possible execution-dependent choice
// before admission and costing. This is a cache-lifetime fact, never permission
// to create an index candidate. In particular, an unresolved or diagnostically
// rejected parameter must remain visible to the caller.
func potentialTemporalIndexAccess(query *plan.Query) bool {
	if query == nil || query.StmtType != plan.Query_SELECT {
		return false
	}
	var nodes []*plan.Node
	seen := make(map[int32]bool)
	pending := append([]int32(nil), query.Steps...)
	for len(pending) != 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id < 0 || int(id) >= len(query.Nodes) || seen[id] || query.Nodes[id] == nil {
			continue
		}
		seen[id] = true
		node := query.Nodes[id]
		nodes = append(nodes, node)
		pending = append(pending, node.Children...)
	}
	indexed := make(map[[2]int32]types.T)
	for _, node := range nodes {
		if node.NodeType != plan.Node_TABLE_SCAN || node.TableDef == nil || len(node.BindingTags) == 0 {
			continue
		}
		for _, index := range node.TableDef.Indexes {
			if index == nil || !index.TableExist || !catalog.IsRegularIndexAlgo(index.IndexAlgo) {
				continue
			}
			for _, part := range index.Parts {
				pos, ok := node.TableDef.Name2ColIndex[catalog.ResolveAlias(part)]
				if !ok || pos < 0 || int(pos) >= len(node.TableDef.Cols) || node.TableDef.Cols[pos] == nil {
					continue
				}
				oid := types.T(node.TableDef.Cols[pos].Typ.Id)
				if oid == types.T_datetime || oid == types.T_timestamp {
					indexed[[2]int32{node.BindingTags[0], pos}] = oid
				}
			}
		}
	}
	if len(indexed) == 0 {
		return false
	}
	found := false
	for _, node := range nodes {
		for _, list := range [][]*plan.Expr{node.FilterList, node.OnList} {
			for _, root := range list {
				_ = plan.VisitExprTree(root, func(expr *plan.Expr) error {
					fn := expr.GetF()
					if found || fn == nil || fn.Func == nil || len(fn.Args) < 2 {
						return nil
					}
					id, _ := function.DecodeOverloadID(fn.Func.Obj)
					switch id {
					case function.EQUAL, function.LESS_THAN, function.LESS_EQUAL, function.GREAT_THAN, function.GREAT_EQUAL:
						if len(fn.Args) != 2 {
							return nil
						}
						found = temporalPotentialPair(indexed, fn.Args[0], fn.Args[1]) || temporalPotentialPair(indexed, fn.Args[1], fn.Args[0])
					case function.BETWEEN, function.IN_RANGE:
						if len(fn.Args) >= 3 {
							found = temporalPotentialPair(indexed, fn.Args[0], fn.Args[1]) || temporalPotentialPair(indexed, fn.Args[0], fn.Args[2])
						}
					case function.IN:
						if len(fn.Args) == 2 && fn.Args[1] != nil {
							found = temporalPotentialPair(indexed, fn.Args[0], fn.Args[1])
							if list := fn.Args[1].GetList(); list != nil {
								for _, bound := range list.List {
									found = found || temporalPotentialPair(indexed, fn.Args[0], bound)
								}
							}
						}
					default:
						return nil
					}
					return nil
				})
				if found {
					return true
				}
			}
		}
	}
	return false
}

// markTemporalAccessLifetime must run on the bound original owner, before
// diagnostic and access-path decisions. It does not enable index selection.
func (builder *QueryBuilder) markTemporalAccessLifetime(rootID int32) {
	if builder == nil || builder.compCtx == nil || builder.qry == nil {
		return
	}
	// The newly bound root is not yet appended to Steps at this boundary.
	owner := *builder.qry
	owner.Steps = append(append([]int32(nil), builder.qry.Steps...), rootID)
	if !potentialTemporalIndexAccess(&owner) {
		return
	}
	builder.qry.TemporalIndexNeedsReplan = true
	if state := preparedBindingState(builder.GetContext()); state != nil {
		state.valueDependent = true
	}
}

func temporalPotentialPair(indexed map[[2]int32]types.T, candidate, bound *plan.Expr) bool {
	if candidate == nil || bound == nil || candidate.GetCol() == nil {
		return false
	}
	col := candidate.GetCol()
	oid, ok := indexed[[2]int32{col.RelPos, col.ColPos}]
	return ok && (isDatetimeTimestampTypePair(oid, types.T(bound.Typ.Id)) || temporalCandidateHasUnprovedSource(bound))
}
