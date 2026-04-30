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

// hiddenDupKeyRe matches duplicate-entry messages whose key name is an
// internal hidden-index column. Lower-level engines (e.g. tae DedupSnapByPK,
// the Optimistic commit-time dedup in disttae) only see the hidden index
// table and surface the internal column name:
//   - single-column UNIQUE: the hidden-index table's PK is __mo_index_idx_col.
//   - composite UNIQUE: the hidden-index table's PK is __mo_cpkey_col
//     (compound primary key column).
//
// __mo_cpkey_col is ALSO used on regular user tables with a composite primary
// key, so we only rewrite that variant when the plan is writing into a hidden
// index table — otherwise a user-table composite-PK duplicate would be
// mis-rewritten as the first unique-index name we happen to find.
var hiddenDupKeyRe = regexp.MustCompile(
	`^Duplicate entry '(.*)' for key '(` + catalog.IndexTableIndexColName +
		`|` + catalog.CPrimaryKeyColName + `)'$`,
)

// RewriteHiddenIndexDupEntry rewrites duplicate-entry errors that surface an
// internal hidden-index column name. It walks the plan to find the unique
// index being written, then substitutes the user-visible column name (for a
// single-column unique index) or the index name (for a composite one).
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
	matchedKey := m[2]

	qry, ok := p.Plan.(*plan.Plan_Query)
	if !ok || qry.Query == nil {
		return err
	}

	type uniqueKey struct {
		parts     []string
		indexName string
	}
	var uniques []uniqueKey
	var hasHiddenIndexTarget bool
	for _, n := range qry.Query.Nodes {
		if n == nil || n.TableDef == nil {
			continue
		}
		if catalog.IsHiddenTable(n.TableDef.Name) {
			hasHiddenIndexTarget = true
		}
		if len(uniques) > 0 {
			// All DML plans share a single target user-table; the first node
			// carrying unique-index definitions is authoritative. Keep scanning
			// subsequent nodes only to detect hidden-index targets.
			continue
		}
		for _, idx := range n.TableDef.Indexes {
			if !idx.Unique {
				continue
			}
			uniques = append(uniques, uniqueKey{parts: idx.Parts, indexName: idx.IndexName})
		}
	}

	// __mo_cpkey_col is shared with user tables that have a composite PK.
	// Only rewrite it when the plan is actually writing into a hidden index
	// table, otherwise a PK duplicate on a regular table would be
	// mislabelled as a unique-index hit.
	if matchedKey == catalog.CPrimaryKeyColName && !hasHiddenIndexTarget {
		return err
	}

	if len(uniques) != 1 {
		// TODO: when multiple unique indexes exist, extract the duplicated
		// value from the error and match it against each index's declared
		// column type so we can pick the right key name. For now we
		// deliberately keep the raw internal-column message because
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
