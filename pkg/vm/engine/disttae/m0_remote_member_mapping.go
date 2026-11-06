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
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Recreated partition members can encode an implicit nullable default as an
// explicit NULL. Both forms describe the same default; other differences must
// still pass the strict column-semantic comparison.
func m0NullableNullDefault(d *plan.Default) bool {
	if d == nil || !d.NullAbility {
		return false
	}
	origin := strings.TrimSpace(d.OriginString)
	if d.Expr == nil {
		return origin == ""
	}
	lit := d.Expr.GetLit()
	return lit != nil && lit.Isnull && (origin == "" || strings.EqualFold(origin, "null"))
}

func m0SameRemoteMemberColumn(expected, actual *plan.ColDef) bool {
	if m0SameCNColumnSemantics(expected, actual) {
		return true
	}
	if expected == nil || actual == nil || !m0NullableNullDefault(expected.Default) || !m0NullableNullDefault(actual.Default) {
		return false
	}
	copy := *actual
	copy.Default = expected.Default
	return m0SameCNColumnSemantics(expected, &copy)
}

// A parent plan is used to prune and read every member. This is a mapping
// check, not proof that an absent historical object column is an M0 append.
func validateM0RemoteMemberMappings(ctx context.Context, primary *txnTable, members []engine.Relation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if primary == nil {
		return moerr.NewInternalErrorNoCtx("M0: remote combined relation has no primary")
	}
	expected := primary.GetTableDef(ctx)
	if expected == nil {
		return moerr.NewInternalErrorNoCtx("M0: remote combined relation has no schema")
	}
	for _, member := range members {
		if err := ctx.Err(); err != nil {
			return err
		}
		if member == nil {
			return moerr.NewInternalErrorNoCtx("M0: remote partition member is missing")
		}
		actual := member.GetTableDef(ctx)
		if actual == nil || len(actual.Cols) != len(expected.Cols) {
			return moerr.NewInternalErrorNoCtx("M0: remote partition member schema mismatch")
		}
		for i, col := range expected.Cols {
			if !m0SameRemoteMemberColumn(col, actual.Cols[i]) {
				return moerr.NewInternalErrorNoCtxf("M0: remote partition member column mismatch at position %d", i)
			}
		}
	}
	return ctx.Err()
}
