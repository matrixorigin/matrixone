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

// PreparedTemporalTypes is immutable once returned from binding. One prepared
// SELECT generation owns the snapshot; retries only borrow it. No values or
// executable expressions escape through this history.
type PreparedTemporalTypes struct {
	datetime []bool
	revision uint64
}

func (s *PreparedTemporalTypes) Revision() uint64 {
	if s == nil {
		return 0
	}
	return s.revision
}

func (s *PreparedTemporalTypes) widened(position int) bool {
	return s != nil && position >= 0 && position < len(s.datetime) && s.datetime[position]
}

type preparedTemporalTypesKey struct{}

func WithPreparedTemporalTypes(ctx context.Context, snapshot *PreparedTemporalTypes) context.Context {
	return context.WithValue(ctx, preparedTemporalTypesKey{}, snapshot)
}

func (state *preparedSourceBindingState) widenTemporal(position int) {
	if position < 0 || position >= len(state.bindings) || state.temporalTypes.widened(position) {
		return
	}
	if !state.temporalTypesDirty {
		next := &PreparedTemporalTypes{datetime: make([]bool, len(state.bindings))}
		if state.temporalTypes != nil {
			copy(next.datetime, state.temporalTypes.datetime)
			next.revision = state.temporalTypes.revision
		}
		state.temporalTypes = next
		state.temporalTypesDirty = true
	}
	state.temporalTypes.datetime[position] = true
	state.temporalTypes.revision++
}

func temporalParamValue(value any) any {
	if param, ok := value.(ParamValue); ok {
		if param.Value == nil {
			return nil
		}
		if param.MaterializedValue != "" {
			return param.MaterializedValue
		}
		value = param.Value
	}
	if bytes, ok := value.([]byte); ok {
		return string(bytes)
	}
	return value
}

// Only result inference observes spelling. Raw arithmetic remains responsible
// for converting values, warnings, incomplete calendars and NULL counts.
func inferPreparedCalendarResult(source types.Type, known bool, value any, calendarOnly bool) types.Type {
	target := types.New(types.T_datetime, 6, 6)
	if !calendarOnly {
		return target
	}
	date := types.T_date.ToType()
	if known && value == nil {
		return date
	}
	if source.Oid == types.T_datetime || source.Oid == types.T_timestamp {
		return target
	}
	if source.Oid.IsMySQLString() && known {
		if text, ok := value.(string); ok && types.ClassifyTemporalString(text) == types.TemporalStringDatetime {
			return target
		}
	}
	return date
}

func preparedCalendarMarkerResult(ctx context.Context, source *Expr, calendarOnly bool) types.Type {
	source = unwrapPreparedImplicitCast(source, true)
	state := preparedBindingState(ctx)
	position := int(source.GetP().Pos)
	var value any
	known := state != nil && position >= 0 && position < len(state.values)
	if known {
		value = temporalParamValue(state.values[position])
	}
	typ := makeTypeByPlan2Expr(source)
	target := inferPreparedCalendarResult(typ, known, value, calendarOnly)
	if state != nil {
		if typ.Oid.IsMySQLString() {
			state.valueDependent = true
		}
		if state.selectStatement && calendarOnly {
			if known && value != nil && target.Oid == types.T_datetime {
				state.widenTemporal(position)
			}
			if state.temporalTypes.widened(position) {
				target = types.New(types.T_datetime, 6, 6)
			}
		}
	}
	return target
}

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
	if temporalDirectMarker(source) {
		source = unwrapPreparedImplicitCast(source, true)
	}
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

func preparedRawCalendarResult(ctx context.Context, name string, args []*Expr) *types.Type {
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
	target := preparedCalendarMarkerResult(ctx, date, timestampAddDateUnit(unit) || unit == types.Year_Month)
	return &target
}

// The original generated result cast records the SQL unit category before
// interval normalization. Extract it before replacing any marker. Explicit
// SQL casts and functions containing a marker cannot own this contract.
func preparedCalendarResultCast(expr *Expr) (position int, calendarOnly bool, ok bool) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || isExplicitPreparedCast(expr) || len(fn.Args) != 2 || fn.Args[1].GetT() == nil || fn.Args[1].Typ.Id != expr.Typ.Id {
		return 0, false, false
	}
	if expr.Typ.Id != int32(types.T_date) && (expr.Typ.Id != int32(types.T_datetime) || expr.Typ.Scale != 6) {
		return 0, false, false
	}
	inner := fn.Args[0].GetF()
	if inner == nil || inner.Func == nil || (inner.Func.ObjName != "date_add" && inner.Func.ObjName != "date_sub") || len(inner.Args) != 3 || !rawPreparedCalendarMarker(inner.Args[0]) {
		return 0, false, false
	}
	marker := unwrapPreparedImplicitCast(inner.Args[0], true)
	return int(marker.GetP().Pos), expr.Typ.Id == int32(types.T_date), true
}
