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

package plan

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type indexHintSet struct {
	scan           indexHintScopeSet
	order          indexHintScopeSet
	group          indexHintScopeSet
	join           indexHintScopeSet
	useSpecified   bool
	forceSpecified bool
}

type indexHintScopeSet struct {
	useSpecified   bool
	forceSpecified bool
	use            map[string]struct{}
	force          map[string]struct{}
	ignore         map[string]struct{}
}

func (builder *QueryBuilder) recordIndexHints(nodeID int32, tableDef *plan.TableDef, hints []*tree.IndexHint) error {
	if len(hints) == 0 || tableDef == nil {
		return nil
	}

	hintSet := &indexHintSet{}
	for _, hint := range hints {
		if hint == nil {
			continue
		}
		names, err := validateIndexHintNames(builder.GetContext(), tableDef, hint.IndexNames)
		if err != nil {
			return err
		}
		if hint.HintType == tree.HintUse {
			if hintSet.forceSpecified {
				return moerr.NewSyntaxErrorNoCtx("USE INDEX and FORCE INDEX cannot be used together")
			}
			hintSet.useSpecified = true
		} else if hint.HintType == tree.HintForce {
			if hintSet.useSpecified {
				return moerr.NewSyntaxErrorNoCtx("USE INDEX and FORCE INDEX cannot be used together")
			}
			hintSet.forceSpecified = true
		}
		scopes := hintSet.scopes(hint.HintScope)
		for _, scope := range scopes {
			if err := addIndexHint(scope, hint.HintType, names); err != nil {
				return err
			}
		}
	}

	if hintSet.empty() {
		return nil
	}
	if builder.indexHintsByScan == nil {
		builder.indexHintsByScan = make(map[int32]*indexHintSet)
	}
	builder.indexHintsByScan[nodeID] = hintSet
	if builder.indexHintOwnerByNode == nil {
		builder.indexHintOwnerByNode = make(map[int32]int32)
	}
	builder.indexHintOwnerByNode[nodeID] = nodeID
	return nil
}

func (hintSet *indexHintSet) scopes(scope tree.IndexHintScope) []*indexHintScopeSet {
	switch scope {
	case tree.HintForOrderBy:
		return []*indexHintScopeSet{&hintSet.order}
	case tree.HintForGroupBy:
		return []*indexHintScopeSet{&hintSet.group}
	case tree.HintForJoin:
		return []*indexHintScopeSet{&hintSet.join}
	default:
		return []*indexHintScopeSet{&hintSet.scan, &hintSet.join, &hintSet.order, &hintSet.group}
	}
}

func (hintSet *indexHintSet) empty() bool {
	return hintSet == nil ||
		(hintSet.scan.empty() && hintSet.order.empty() && hintSet.group.empty() && hintSet.join.empty())
}

func (scope indexHintScopeSet) empty() bool {
	return !scope.useSpecified && !scope.forceSpecified && len(scope.ignore) == 0
}

func validateIndexHintNames(ctx context.Context, tableDef *plan.TableDef, names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	existing := make(map[string]string, len(tableDef.Indexes))
	if tableDef.Pkey != nil && !strings.EqualFold(tableDef.Pkey.PkeyColName, catalog.FakePrimaryKeyColName) {
		existing[strings.ToLower(PrimaryKeyName)] = strings.ToLower(PrimaryKeyName)
	}
	for _, idx := range tableDef.Indexes {
		if idx == nil || !idx.TableExist {
			continue
		}
		lowerName := strings.ToLower(idx.IndexName)
		existing[lowerName] = lowerName
	}
	normalized := make([]string, 0, len(names))
	for _, name := range names {
		lowerName := strings.ToLower(name)
		if exact, ok := existing[lowerName]; ok {
			normalized = append(normalized, exact)
			continue
		}
		var match string
		for existingName := range existing {
			if strings.HasPrefix(existingName, lowerName) {
				if match != "" {
					return nil, moerr.NewSyntaxErrorf(ctx, "index hint %q is ambiguous", name)
				}
				match = existingName
			}
		}
		if match == "" {
			return nil, moerr.NewErrKeyDoesNotExist(ctx, name, tableDef.Name)
		}
		normalized = append(normalized, match)
	}
	return normalized, nil
}

func addIndexHint(scope *indexHintScopeSet, hintType tree.IndexHintType, names []string) error {
	switch hintType {
	case tree.HintUse:
		if scope.forceSpecified {
			return moerr.NewSyntaxErrorNoCtx("USE INDEX and FORCE INDEX cannot be used together")
		}
		scope.useSpecified = true
		addIndexHintNames(&scope.use, names)
	case tree.HintForce:
		if len(names) == 0 {
			return moerr.NewSyntaxErrorNoCtx("FORCE INDEX requires at least one index")
		}
		if scope.useSpecified {
			return moerr.NewSyntaxErrorNoCtx("USE INDEX and FORCE INDEX cannot be used together")
		}
		scope.forceSpecified = true
		addIndexHintNames(&scope.force, names)
	case tree.HintIgnore:
		if len(names) == 0 {
			return moerr.NewSyntaxErrorNoCtx("IGNORE INDEX requires at least one index")
		}
		addIndexHintNames(&scope.ignore, names)
	}
	return nil
}

func addIndexHintNames(dst *map[string]struct{}, names []string) {
	if *dst == nil {
		*dst = make(map[string]struct{}, len(names))
	}
	for _, name := range names {
		(*dst)[name] = struct{}{}
	}
}

func (builder *QueryBuilder) filterRegularIndexesByScanHints(node *plan.Node, indexes []*plan.IndexDef) []*plan.IndexDef {
	return builder.filterRegularIndexesByHints(node, indexes, func(hintSet *indexHintSet) indexHintScopeSet {
		return hintSet.scan
	})
}

func (builder *QueryBuilder) filterIndexesByScanHints(node *plan.Node, indexes []*plan.IndexDef) []*plan.IndexDef {
	return builder.filterRegularIndexesByScanHints(node, indexes)
}

func (builder *QueryBuilder) scanHintsForceIndexes(node *plan.Node) bool {
	if builder == nil || node == nil {
		return false
	}
	hintSet := builder.indexHintsByScan[node.NodeId]
	return hintSet != nil && hintSet.scan.forceSpecified
}

func (builder *QueryBuilder) filterRegularIndexesByJoinHints(node *plan.Node, indexes []*plan.IndexDef) []*plan.IndexDef {
	return builder.filterRegularIndexesByHints(node, indexes, func(hintSet *indexHintSet) indexHintScopeSet {
		return hintSet.join
	})
}

func (builder *QueryBuilder) filterRegularIndexesByHints(node *plan.Node, indexes []*plan.IndexDef, getScope func(*indexHintSet) indexHintScopeSet) []*plan.IndexDef {
	if builder == nil || node == nil || len(indexes) == 0 {
		return indexes
	}
	hintSet := builder.indexHintsByScan[node.NodeId]
	if hintSet == nil {
		return indexes
	}
	scope := getScope(hintSet)
	if scope.empty() {
		return indexes
	}
	return filterIndexesByHintScope(indexes, scope)
}

func filterIndexesByHintScope(indexes []*plan.IndexDef, scope indexHintScopeSet) []*plan.IndexDef {
	if scope.empty() {
		return indexes
	}
	filtered := make([]*plan.IndexDef, 0, len(indexes))
	for _, idx := range indexes {
		if idx == nil {
			continue
		}
		name := strings.ToLower(idx.IndexName)
		if _, ignored := scope.ignore[name]; ignored {
			continue
		}
		if scope.forceSpecified {
			if _, ok := scope.force[name]; !ok {
				continue
			}
		} else if scope.useSpecified {
			if _, ok := scope.use[name]; !ok {
				continue
			}
		}
		filtered = append(filtered, idx)
	}
	return filtered
}

func (builder *QueryBuilder) regularIndexScanAllowedByOrderHints(node *plan.Node) bool {
	if builder == nil || node == nil || !node.IndexScanInfo.IsIndexScan {
		return true
	}
	hintSet := builder.indexHintsByScan[node.NodeId]
	if hintSet == nil || hintSet.order.empty() {
		return true
	}
	name := strings.ToLower(node.IndexScanInfo.IndexName)
	if _, ignored := hintSet.order.ignore[name]; ignored {
		return false
	}
	if hintSet.order.forceSpecified {
		_, ok := hintSet.order.force[name]
		return ok
	}
	if hintSet.order.useSpecified {
		_, ok := hintSet.order.use[name]
		return ok
	}
	return true
}

func (builder *QueryBuilder) inheritIndexHints(dstNodeID, srcNodeID int32) {
	if builder == nil || builder.indexHintsByScan == nil {
		return
	}
	hintSet := builder.indexHintsByScan[srcNodeID]
	if hintSet == nil {
		return
	}
	builder.indexHintsByScan[dstNodeID] = hintSet
	if builder.indexHintOwnerByNode == nil {
		builder.indexHintOwnerByNode = make(map[int32]int32)
	}
	owner := srcNodeID
	if inheritedOwner, ok := builder.indexHintOwnerByNode[srcNodeID]; ok {
		owner = inheritedOwner
	}
	builder.indexHintOwnerByNode[dstNodeID] = owner
}
