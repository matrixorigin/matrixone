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
	scan  indexHintScopeSet
	order indexHintScopeSet
	group indexHintScopeSet
	join  indexHintScopeSet
}

type indexHintScopeSet struct {
	use    map[string]struct{}
	force  map[string]struct{}
	ignore map[string]struct{}
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
		scope := hintSet.scope(hint.HintScope)
		switch hint.HintType {
		case tree.HintUse:
			addIndexHintNames(&scope.use, names)
		case tree.HintForce:
			addIndexHintNames(&scope.force, names)
		case tree.HintIgnore:
			addIndexHintNames(&scope.ignore, names)
		}
	}

	if hintSet.empty() {
		return nil
	}
	if builder.indexHintsByScan == nil {
		builder.indexHintsByScan = make(map[int32]*indexHintSet)
	}
	builder.indexHintsByScan[nodeID] = hintSet
	return nil
}

func (hintSet *indexHintSet) scope(scope tree.IndexHintScope) *indexHintScopeSet {
	switch scope {
	case tree.HintForOrderBy:
		return &hintSet.order
	case tree.HintForGroupBy:
		return &hintSet.group
	case tree.HintForJoin:
		return &hintSet.join
	default:
		return &hintSet.scan
	}
}

func (hintSet *indexHintSet) empty() bool {
	return hintSet == nil ||
		(hintSet.scan.empty() && hintSet.order.empty() && hintSet.group.empty() && hintSet.join.empty())
}

func (scope indexHintScopeSet) empty() bool {
	return len(scope.use) == 0 && len(scope.force) == 0 && len(scope.ignore) == 0
}

func validateIndexHintNames(ctx context.Context, tableDef *plan.TableDef, names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	existing := make(map[string]struct{}, len(tableDef.Indexes))
	if tableDef.Pkey != nil && !strings.EqualFold(tableDef.Pkey.PkeyColName, catalog.FakePrimaryKeyColName) {
		existing[strings.ToLower(PrimaryKeyName)] = struct{}{}
	}
	for _, idx := range tableDef.Indexes {
		if idx == nil {
			continue
		}
		existing[strings.ToLower(idx.IndexName)] = struct{}{}
	}
	normalized := make([]string, 0, len(names))
	for _, name := range names {
		lowerName := strings.ToLower(name)
		if _, ok := existing[lowerName]; !ok {
			return nil, moerr.NewErrKeyDoesNotExist(ctx, name, tableDef.Name)
		}
		normalized = append(normalized, lowerName)
	}
	return normalized, nil
}

func addIndexHintNames(dst *map[string]struct{}, names []string) {
	if len(names) == 0 {
		return
	}
	if *dst == nil {
		*dst = make(map[string]struct{}, len(names))
	}
	for _, name := range names {
		(*dst)[name] = struct{}{}
	}
}

func (builder *QueryBuilder) filterRegularIndexesByScanHints(node *plan.Node, indexes []*plan.IndexDef) []*plan.IndexDef {
	if builder == nil || node == nil || len(indexes) == 0 {
		return indexes
	}
	hintSet := builder.indexHintsByScan[node.NodeId]
	if hintSet == nil || hintSet.scan.empty() {
		return indexes
	}
	return filterIndexesByHintScope(indexes, hintSet.scan)
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
		if len(scope.force) > 0 {
			if _, ok := scope.force[name]; !ok {
				continue
			}
		} else if len(scope.use) > 0 {
			if _, ok := scope.use[name]; !ok {
				continue
			}
		}
		filtered = append(filtered, idx)
	}
	return filtered
}
