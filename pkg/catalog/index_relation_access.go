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

package catalog

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
)

const PhysicalIndexPredicateFunction = "mo_is_physical_index"

// NonIndexRelationSQLPredicate excludes physical index children from metadata
// listings. It must not be used by clone or restore scans, which need children.
func NonIndexRelationSQLPredicate(alias string) string {
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}
	return NonIndexRelationSQLPredicateForColumns(prefix+SystemRelAttr_Name, prefix+SystemRelAttr_Kind, prefix+SystemRelAttr_ExtraInfo)
}

// NonIndexRelationSQLPredicateForColumns accepts the catalog column expressions
// of a metadata source, including subscription columns whose name is att_relname.
func NonIndexRelationSQLPredicateForColumns(name, kind, extra string) string {
	return fmt.Sprintf("not %s(coalesce(%s, ''), coalesce(%s, ''), coalesce(%s, ''))", PhysicalIndexPredicateFunction, name, kind, extra)
}

// RequiresPrivilegedRelationAccess checks an explicit physical relation target.
// Unknown nonempty kinds fail closed: they may belong to an index plugin not
// compiled into this binary. Empty kind remains valid for old ordinary tables
// and name-only checks. New children also carry the durable IndexTable flag.
func RequiresPrivilegedRelationAccess(name, kind string, flags uint64) bool {
	if features.IsIndexTable(flags) {
		return true
	}
	if len(name) >= len(IndexTableNamePrefix) && strings.EqualFold(name[:len(IndexTableNamePrefix)], IndexTableNamePrefix) {
		return true
	}
	if len(name) >= len(defines.TempTableNamePrefix) && strings.EqualFold(name[:len(defines.TempTableNamePrefix)], defines.TempTableNamePrefix) {
		lower := strings.ToLower(name)
		if IsUniqueIndexTable(lower) || IsSecondaryIndexTable(lower) {
			return true
		}
	}
	if kind == "" {
		return false
	}
	if strings.EqualFold(kind, SystemTemporaryTable) || strings.EqualFold(kind, SystemPersistRel) || strings.EqualFold(kind, SystemTransientRel) {
		return false
	}
	for _, coreKind := range userVisibleRelationKinds {
		if strings.EqualFold(kind, coreKind) {
			return false
		}
	}
	return true
}
