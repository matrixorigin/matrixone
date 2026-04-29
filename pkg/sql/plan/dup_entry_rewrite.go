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
	"regexp"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// hiddenDupKeyRe matches duplicate-entry messages whose key name is the
// internal hidden unique-index column "__mo_index_idx_col". Lower-level engines
// (e.g. tae DedupSnapByPK, the Optimistic commit-time dedup in disttae) only
// see the hidden-index table and surface the internal column name; we rewrite
// the message using the plan-level index definition so users see the original
// column name.
var hiddenDupKeyRe = regexp.MustCompile(
	`^Duplicate entry '(.*)' for key '` + catalog.IndexTableIndexColName + `'$`,
)

// RewriteHiddenIndexDupEntry rewrites duplicate-entry errors that surface the
// internal __mo_index_idx_col column name. It walks the plan to find the
// unique index being written, then substitutes the user-visible column name
// (for a single-column unique index) or the index name (for a composite one).
// When multiple unique indexes exist on the target table, we cannot tell from
// the engine-level error which one was hit, so we fall back to the original
// error to avoid misattributing the key.
func RewriteHiddenIndexDupEntry(p *plan.Plan, err error) error {
	if err == nil || p == nil {
		return err
	}
	msg := err.Error()
	m := hiddenDupKeyRe.FindStringSubmatch(msg)
	if m == nil {
		return err
	}
	entry := m[1]

	qry, ok := p.Plan.(*plan.Plan_Query)
	if !ok || qry.Query == nil {
		return err
	}

	type uniqueKey struct {
		parts     []string
		indexName string
	}
	var uniques []uniqueKey
	for _, n := range qry.Query.Nodes {
		if n == nil || n.TableDef == nil {
			continue
		}
		for _, idx := range n.TableDef.Indexes {
			if !idx.Unique {
				continue
			}
			uniques = append(uniques, uniqueKey{parts: idx.Parts, indexName: idx.IndexName})
		}
		// All DML plans share a single target table, so stop after the first
		// node that carries a TableDef with index definitions.
		if len(uniques) > 0 {
			break
		}
	}

	if len(uniques) != 1 {
		// TODO: when multiple unique indexes exist, extract the duplicated
		// value from the error and match it against each index's declared
		// column type so we can pick the right key name. For now we
		// deliberately keep the raw __mo_index_idx_col message because
		// misattribution is worse than a leaked internal name.
		return err
	}
	u := uniques[0]
	var indexKey string
	if len(u.parts) == 1 {
		indexKey = catalog.ResolveAlias(u.parts[0])
	} else {
		indexKey = u.indexName
	}
	if indexKey == "" {
		return err
	}
	return moerr.NewDuplicateEntryNoCtx(entry, indexKey)
}
