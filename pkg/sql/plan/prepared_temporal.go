// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Only a direct marker and its ordinary implicit casts own dynamic temporal
// metadata. Explicit casts and functions containing a marker keep their contract.
func temporalDirectMarker(expr *Expr) bool {
	expr = unwrapPreparedImplicitCast(expr, true)
	return expr != nil && expr.GetP() != nil && expr.GetP().Pos >= 0
}

func temporalDirectMarkerArgument(args []*Expr, original *Expr, index int) bool {
	if original != nil && original.GetF() != nil {
		args = original.GetF().Args
	}
	return index >= 0 && index < len(args) && temporalDirectMarker(args[index])
}

func bindTemporalMarkerConsumer(ctx context.Context, source *Expr, target types.Type) (*Expr, error) {
	// TIMESTAMP owns calendar parsing, including invalid-to-NULL semantics.
	// The subsequent typed conversion can then safely fix this consumer's FSP.
	source = unwrapPreparedImplicitCast(source, true)
	datetime, err := BindFuncExprImplByPlanExpr(ctx, "timestamp", []*Expr{source})
	if err != nil {
		return nil, err
	}
	return appendCastBeforeExpr(ctx, datetime, makePlan2Type(&target))
}

// Raw markers need the arithmetic consumer's incomplete-calendar diagnostics.
// Explicit casts and known non-string sources keep the released typed path.
func rawPreparedCalendarMarker(expr *Expr) bool {
	if !temporalDirectMarker(expr) {
		return false
	}
	source := unwrapPreparedImplicitCast(expr, true)
	oid := types.T(source.Typ.Id)
	return oid == types.T_any || oid.IsMySQLString()
}

func preparedRawCalendarResult(name string, args []*Expr) *types.Type {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	date, interval := args[0], args[1]
	switch name {
	case "date_add", "date_sub":
	case "+":
		if args[0].Typ.Id == int32(types.T_interval) {
			date, interval = args[1], args[0]
		} else if args[1].Typ.Id != int32(types.T_interval) {
			return nil
		}
	case "-":
		if args[1].Typ.Id != int32(types.T_interval) {
			return nil
		}
	default:
		return nil
	}
	if !rawPreparedCalendarMarker(date) {
		return nil
	}
	list := interval.GetList()
	if list == nil || len(list.List) < 2 {
		return nil
	}
	unit, err := types.IntervalTypeOf(list.List[1].GetLit().GetSval())
	if err != nil {
		return nil
	}
	// Inspect the SQL unit before normalization can turn text DAY into
	// MICROSECOND. Rebinding the internal three-argument tree keeps this cast.
	target := types.New(types.T_datetime, 6, 6)
	if timestampAddDateUnit(unit) || unit == types.Year_Month {
		target = types.T_date.ToType()
	}
	return &target
}
