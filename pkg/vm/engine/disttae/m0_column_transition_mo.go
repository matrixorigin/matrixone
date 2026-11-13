//go:build mo_m0

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

package disttae

import (
	"slices"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func m0AppendPositionAllowed(pos int32, columnCount int) bool {
	return columnCount > 0 && (pos == -1 || pos == int32(columnCount-1))
}

// validateM0CNColumnTransition compares semantic column attributes, rather
// than the object header's type OID or transient type provenance. Expression
// source text is retained: SHOW CREATE and rebinding paths consume it.
// This validates this ADD transition; it does not prove a relation's history.
func validateM0CNColumnTransition(before, after *plan.TableDef) error {
	if before == nil || after == nil || len(before.Cols) < 1 || len(after.Cols) != len(before.Cols)+1 ||
		before.Pkey == nil || after.Pkey == nil || before.Pkey.PkeyColName != after.Pkey.PkeyColName ||
		before.Pkey.PkeyColId != after.Pkey.PkeyColId || !slices.Equal(before.Pkey.Cols, after.Pkey.Cols) ||
		!slices.Equal(before.Pkey.Names, after.Pkey.Names) || !proto.Equal(before.Pkey.Option, after.Pkey.Option) {
		return moerr.NewInternalErrorNoCtx("M0: invalid ADD column transition")
	}
	added := after.Cols[len(before.Cols)-1]
	if added == nil || strings.TrimSpace(added.Name) == "" || added.Unique || added.NotNull || !m0PlanColumnSupportsAppend(added) {
		return moerr.NewInternalErrorNoCtx("M0: invalid appended column semantics")
	}
	// The rowid follows the newly inserted business column and is unchanged.
	for i, old := range before.Cols {
		if old != nil && strings.EqualFold(old.Name, added.Name) {
			return moerr.NewInternalErrorNoCtx("M0: duplicate appended column name")
		}
		pos := i
		if i == len(before.Cols)-1 {
			pos++
		}
		if !m0SameCNColumnSemantics(old, after.Cols[pos]) {
			return moerr.NewInternalErrorNoCtxf("M0: existing column semantics changed at position %d", i)
		}
	}
	return nil
}

func m0SameCNColumnSemantics(a, b *plan.ColDef) bool {
	if a == nil || b == nil {
		return false
	}
	at, bt := a.Typ, b.Typ
	if a.ColId != b.ColId || a.Name != b.Name || a.Seqnum != b.Seqnum ||
		a.Hidden != b.Hidden || a.Primary != b.Primary || a.Unique != b.Unique ||
		a.ClusterBy != b.ClusterBy || a.Pkidx != b.Pkidx || a.NotNull != b.NotNull ||
		at.Id != bt.Id || at.Width != bt.Width || at.Scale != bt.Scale ||
		at.NotNullable != bt.NotNullable || at.AutoIncr != bt.AutoIncr ||
		at.Enumvalues != bt.Enumvalues || at.Charset != bt.Charset || at.PadSpace != bt.PadSpace || at.CollationVersion != bt.CollationVersion ||
		at.CollationCoercibility != bt.CollationCoercibility || at.CollationCoercibilitySet != bt.CollationCoercibilitySet ||
		at.CollationMergeConflict != bt.CollationMergeConflict {
		return false
	}
	if (a.Default == nil) != (b.Default == nil) || (a.OnUpdate == nil) != (b.OnUpdate == nil) ||
		(a.GeneratedCol == nil) != (b.GeneratedCol == nil) {
		return false
	}
	if a.Default != nil && (a.Default.NullAbility != b.Default.NullAbility || a.Default.OriginString != b.Default.OriginString || !proto.Equal(a.Default.Expr, b.Default.Expr)) {
		return false
	}
	if a.OnUpdate != nil && (a.OnUpdate.OriginString != b.OnUpdate.OriginString || !proto.Equal(a.OnUpdate.Expr, b.OnUpdate.Expr)) {
		return false
	}
	return a.GeneratedCol == nil || (a.GeneratedCol.IsStored == b.GeneratedCol.IsStored && a.GeneratedCol.OriginString == b.GeneratedCol.OriginString &&
		proto.Equal(a.GeneratedCol.Expr, b.GeneratedCol.Expr))
}
