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

package compile

import (
	"regexp"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// hiddenDupKeyRe matches duplicate-entry messages whose key name is the
// internal hidden unique-index column "__mo_index_idx_col". Lower-level engines
// (e.g. tae DedupSnapByPK) only see the hidden-index table and surface the
// internal column name; we rewrite the message using the plan-level index
// definition so users see the original column name.
var hiddenDupKeyRe = regexp.MustCompile(
	`^Duplicate entry '(.*)' for key '` + catalog.IndexTableIndexColName + `'$`,
)

// rewriteHiddenIndexDupEntry rewrites duplicate-entry errors that surface the
// internal __mo_index_idx_col column name. It walks the plan to find the unique
// index being written, then substitutes the user-visible column name (for a
// single-column unique index) or the index name (for a composite one).
func rewriteHiddenIndexDupEntry(p *plan.Plan, err error) error {
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

	// Find the first unique index defined on any table referenced by the
	// query. For INSERT/UPDATE/REPLACE statements there is exactly one target
	// table, so this is unambiguous in practice.
	var indexKey string
	for _, n := range qry.Query.Nodes {
		if n == nil || n.TableDef == nil {
			continue
		}
		for _, idx := range n.TableDef.Indexes {
			if !idx.Unique {
				continue
			}
			if len(idx.Parts) == 1 {
				indexKey = catalog.ResolveAlias(idx.Parts[0])
			} else {
				indexKey = idx.IndexName
			}
			break
		}
		if indexKey != "" {
			break
		}
	}

	if indexKey == "" {
		return err
	}
	return moerr.NewDuplicateEntryNoCtx(entry, indexKey)
}
