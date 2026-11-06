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

package plan

import (
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// temporalInteriorTimestampMargin includes the fixed-zone offset envelope,
// the largest precision quantum and strict-bound arithmetic. Values inside
// this domain cannot trigger the calendar interval's lo>hi reset for a ray.
const temporalInteriorTimestampMargin = (int64(1)<<31+72*types.SecsPerHour+2)*types.MicroSecsPerSec + types.MicroSecsPerSec + 1

// Classify an immutable expression domain, never the observed statement clock.
// CURRENT_TIMESTAMP maps every int64 Unix nanosecond input into 1677..2262,
// including precision rounding, well inside this calendar margin. Other
// functions and unproved parameter/source values retain the original union.
func temporalCandidateInteriorTimestampBound(bound *plan.Expr) bool {
	if bound == nil || types.T(bound.Typ.Id) != types.T_timestamp || bound.Typ.Scale < 0 || bound.Typ.Scale > 6 || bound.AuxId < 0 || bound.PreparedNumeric != nil || len(bound.XXX_unrecognized) != 0 || len(bound.Typ.XXX_unrecognized) != 0 {
		return false
	}
	switch value := bound.Expr.(type) {
	case *plan.Expr_Lit:
		if value == nil || value.Lit == nil || value.Lit.Isnull || value.Lit.Src != nil || len(value.Lit.XXX_unrecognized) != 0 {
			return false
		}
		stamp, ok := value.Lit.Value.(*plan.Literal_Timestampval)
		if !ok || stamp == nil {
			return false
		}
		maximum := int64(types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999))
		return stamp.Timestampval >= temporalInteriorTimestampMargin && stamp.Timestampval <= maximum-temporalInteriorTimestampMargin
	case *plan.Expr_F:
		if value == nil || value.F == nil || value.F.Func == nil || value.F.Func.PubInfo != nil || value.F.Func.Snapshot != nil || len(value.F.AggConfig) != 0 || len(value.F.XXX_unrecognized) != 0 || len(value.F.Func.XXX_unrecognized) != 0 {
			return false
		}
		if len(value.F.Args) == 1 {
			arg := value.F.Args[0]
			if arg == nil || arg.AuxId < 0 || arg.PreparedNumeric != nil {
				return false
			}
			literal, ok := arg.Expr.(*plan.Expr_Lit)
			if !ok || literal == nil || literal.Lit == nil {
				return false
			}
			fsp, ok := literal.Lit.Value.(*plan.Literal_I64Val)
			if !ok || fsp == nil {
				return false
			}
		}
		return temporalCandidateBoundSafe(bound)
	default:
		return false
	}
}

// makeDirectTemporalCandidate constructs a typed direct-key hint. This helper
// does not admit or mark the hint: its caller must first establish the complete
// scan, ancestor, prepared-provenance, placement and index-completeness proofs.
func makeDirectTemporalCandidate(ctx context.Context, column, bound *plan.Expr, op types.TemporalOp, scale int32) (*plan.Expr, error) {
	return buildTemporalCandidate(ctx, column, bound, nil, op, scale)
}

// Serialized temporal access is supported only for a leading component of a
// composite binary key. Unsupported physical metadata declines the candidate.
func makeEncodedTemporalCandidate(ctx context.Context, physicalColumn, component, bound *plan.Expr, op types.TemporalOp, scale int32) (*plan.Expr, error) {
	if physicalColumn == nil || physicalColumn.GetCol() == nil || types.T(physicalColumn.Typ.Id) != types.T_varchar ||
		types.IsUnicodeCollation(uint8(physicalColumn.Typ.Charset)) || physicalColumn.Typ.ValidateCollation() != nil ||
		component == nil || component.GetCol() == nil || component.Typ.ValidateCollation() != nil {
		return nil, nil
	}
	candidate, err := buildTemporalCandidate(ctx, component, bound, physicalColumn, op, scale)
	if stop := context.Cause(ctx); stop != nil {
		return nil, stop
	}
	return candidate, err
}

// One topology owns the main interval, zero point and physical tails.
// A nonnil physical key emits serialized prefix leaves directly, avoiding an
// intermediate direct expression tree.
func buildTemporalCandidate(ctx context.Context, column, bound, physical *plan.Expr, op types.TemporalOp, scale int32) (candidate *plan.Expr, err error) {
	if column == nil || column.GetCol() == nil || bound == nil || scale < 0 || scale > 6 || op > types.TemporalGreaterEqual ||
		!isDatetimeTimestampTypePair(types.T(column.Typ.Id), types.T(bound.Typ.Id)) || !temporalCandidateBoundSafe(bound) {
		return nil, moerr.NewInternalError(ctx, "unsupported temporal candidate")
	}
	kind := types.T(column.Typ.Id)
	bind := func(name string, args ...*plan.Expr) (*plan.Expr, error) {
		return BindFuncExprImplByPlanExpr(ctx, name, args)
	}
	name := "__mo_temporal_datetime_bound"
	if kind == types.T_timestamp {
		name = "__mo_temporal_timestamp_bound"
	}
	control := func(value int32) *plan.Expr { return MakePlan2Int32ConstExprWithType(value) }
	endpoint := func(selector int32) (*plan.Expr, error) {
		expr, bindErr := bind(name, DeepCopyExpr(bound), control(int32(op)), control(scale), control(selector))
		if bindErr == nil {
			expr.Typ.Scale = column.Typ.Scale
		}
		return expr, bindErr
	}
	leaf := func(name string, values ...*plan.Expr) (*plan.Expr, error) {
		args := []*plan.Expr{DeepCopyExpr(column)}
		if physical != nil {
			args[0] = DeepCopyExpr(physical)
			if name == "between" {
				name = "prefix_between"
			} else {
				name = "prefix_eq"
			}
			for _, value := range values {
				serial, err := bind(indexTableComparisonSerialFunc(), DeepCopyExpr(value))
				if err != nil {
					return nil, err
				}
				args = append(args, serial)
			}
		} else {
			args = append(args, values...)
		}
		return bind(name, args...)
	}
	literal := func(v int64) *plan.Expr {
		return &plan.Expr{Typ: column.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Datetimeval{Datetimeval: v}}}}
	}
	interior := kind == types.T_datetime && temporalCandidateInteriorTimestampBound(bound)
	lowerRay := interior && (op == types.TemporalLess || op == types.TemporalLessEqual)
	upperRay := interior && (op == types.TemporalGreater || op == types.TemporalGreaterEqual)
	var lo, hi *plan.Expr
	if lowerRay {
		lo = literal(math.MinInt64)
	} else if lo, err = endpoint(0); err != nil {
		return nil, err
	}
	if upperRay {
		hi = literal(math.MaxInt64)
	} else if hi, err = endpoint(1); err != nil {
		return nil, err
	}
	candidate, err = leaf("between", lo, hi)
	if err != nil {
		return nil, err
	}
	if kind == types.T_datetime {
		if !interior {
			point, pointErr := endpoint(2)
			if pointErr != nil {
				return nil, pointErr
			}
			pointExpr, pointErr := leaf("=", point)
			if pointErr != nil {
				return nil, pointErr
			}
			candidate, err = bind("or", candidate, pointExpr)
			if err != nil {
				return nil, err
			}
		}
		maxDate := int64(types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999))
		tails := [][2]int64{{math.MinInt64, -2}, {maxDate + 1, math.MaxInt64}}
		if lowerRay {
			tails = tails[1:] // the main ray now includes the lower tail and zero
		} else if upperRay {
			tails = tails[:1] // the main ray now includes the upper tail
		}
		for _, tail := range tails {
			tailExpr, tailErr := leaf("between", literal(tail[0]), literal(tail[1]))
			if tailErr != nil {
				return nil, tailErr
			}
			candidate, err = bind("or", candidate, tailExpr)
			if err != nil {
				return nil, err
			}
		}
	}
	return candidate, nil
}
