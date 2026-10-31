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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// temporalCandidateWitness is issued only by the successful typed constructor.
// It carries no bound value and never becomes part of an executable plan.
type temporalCandidateWitness struct {
	kind       types.T
	op         types.TemporalOp
	scale      int32
	boundKind  types.T
	boundScale int32
	encoded    bool
	valid      bool
}

// temporalInteriorTimestampMargin includes the named-zone offset envelope,
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
// Permission is false until execution placement has been established.
func makeDirectTemporalCandidate(ctx context.Context, column, bound *plan.Expr, op types.TemporalOp, scale int32) (*plan.Expr, *plan.Expr, error) {
	candidate, guard, _, err := buildTemporalCandidate(ctx, column, bound, nil, op, scale)
	return candidate, guard, err
}

func makeEncodedTemporalCandidate(ctx context.Context, physicalColumn, component, bound *plan.Expr, prefix []*plan.Expr, hasSuffix bool, op types.TemporalOp, scale int32) (*plan.Expr, *plan.Expr, error) {
	candidate, guard, _, err := makeEncodedTemporalCandidateWithWitness(ctx, physicalColumn, component, bound, prefix, hasSuffix, op, scale)
	return candidate, guard, err
}

func makeEncodedTemporalCandidateWithWitness(ctx context.Context, physicalColumn, component, bound *plan.Expr, prefix []*plan.Expr, hasSuffix bool, op types.TemporalOp, scale int32) (*plan.Expr, *plan.Expr, temporalCandidateWitness, error) {
	if len(prefix) == 0 && hasSuffix && physicalColumn != nil && physicalColumn.GetCol() != nil &&
		types.T(physicalColumn.Typ.Id) == types.T_varchar && !types.IsUnicodeCollation(uint8(physicalColumn.Typ.Charset)) &&
		component != nil && component.GetCol() != nil && component.Typ.ValidateCollation() == nil && physicalColumn.Typ.ValidateCollation() == nil {
		candidate, guard, witness, err := buildTemporalCandidate(ctx, component, bound, physicalColumn, op, scale)
		if stop := context.Cause(ctx); stop != nil {
			return nil, nil, temporalCandidateWitness{}, stop
		}
		if err == nil && witness.valid {
			return candidate, guard, witness, nil
		}
	}
	candidate, guard, err := makeEncodedTemporalCandidateLegacy(ctx, physicalColumn, component, bound, prefix, hasSuffix, op, scale)
	return candidate, guard, temporalCandidateWitness{}, err
}

// One topology owns the main interval, zero point, physical tails and guard.
// A nonnil physical key emits serialized prefix leaves directly, avoiding an
// intermediate direct expression tree. Other encoded forms keep the translator.
func buildTemporalCandidate(ctx context.Context, column, bound, physical *plan.Expr, op types.TemporalOp, scale int32) (candidate, guard *plan.Expr, witness temporalCandidateWitness, err error) {
	if column == nil || column.GetCol() == nil || bound == nil || scale < 0 || scale > 6 || op > types.TemporalGreaterEqual ||
		!isDatetimeTimestampTypePair(types.T(column.Typ.Id), types.T(bound.Typ.Id)) || !temporalCandidateBoundSafe(bound) {
		return nil, nil, witness, moerr.NewInternalError(ctx, "unsupported temporal candidate")
	}
	witness = temporalCandidateWitness{kind: types.T(column.Typ.Id), op: op, scale: scale, boundKind: types.T(bound.Typ.Id), boundScale: bound.Typ.Scale, encoded: physical != nil, valid: true}
	// Exact temporal/control OIDs, same-type ranges, BOOL parents and a single
	// temporal serial argument bind without conversions. Certify the actual
	// local result; unfamiliar overloads/replaced arguments only disable the
	// cost shortcut, never change the returned expression or its error.
	bind := func(name string, id int32, overload int32, result types.T, args ...*plan.Expr) (*plan.Expr, error) {
		var submitted [5]*plan.Expr
		var kinds [5]int32
		var scales [5]int32
		for i, arg := range args {
			submitted[i], kinds[i], scales[i] = arg, arg.Typ.Id, arg.Typ.Scale
		}
		expr, bindErr := BindFuncExprImplByPlanExpr(ctx, name, args)
		if bindErr == nil {
			f := expr.GetF()
			if f == nil || f.Func == nil || expr.Typ.Id != int32(result) || len(f.Args) != len(args) {
				witness.valid = false
			} else {
				actualID, actualOverload := function.DecodeOverloadID(f.Func.Obj)
				witness.valid = witness.valid && actualID == id && actualOverload == overload
				for i, arg := range f.Args {
					witness.valid = witness.valid && arg == submitted[i] && arg.Typ.Id == kinds[i] && arg.Typ.Scale == scales[i]
				}
			}
		}
		return expr, bindErr
	}
	name, helper := "__mo_temporal_datetime_bound", int32(function.TEMPORAL_DATETIME_BOUND)
	guardOverload := int32(0)
	if witness.kind == types.T_timestamp {
		name, helper, guardOverload = "__mo_temporal_timestamp_bound", int32(function.TEMPORAL_TIMESTAMP_BOUND), 1
	}
	control := func(value int32) *plan.Expr { return MakePlan2Int32ConstExprWithType(value) }
	endpoint := func(selector int32) (*plan.Expr, error) {
		expr, bindErr := bind(name, helper, 0, witness.kind, DeepCopyExpr(bound), control(int32(op)), control(scale), control(selector), MakePlan2BoolConstExprWithType(false))
		if bindErr == nil {
			expr.Typ.Scale = column.Typ.Scale
		}
		return expr, bindErr
	}
	leaf := func(name string, values ...*plan.Expr) (*plan.Expr, error) {
		id := int32(function.EQUAL)
		if name == "between" {
			id = int32(function.BETWEEN)
		}
		args := []*plan.Expr{DeepCopyExpr(column)}
		if physical != nil {
			args[0] = DeepCopyExpr(physical)
			if name == "between" {
				name, id = "prefix_between", int32(function.PREFIX_BETWEEN)
			} else {
				name, id = "prefix_eq", int32(function.PREFIX_EQ)
			}
			for _, value := range values {
				serial, err := bind(indexTableComparisonSerialFunc(), int32(function.SERIAL), 0, types.T_varchar, DeepCopyExpr(value))
				if err != nil {
					return nil, err
				}
				args = append(args, serial)
			}
		} else {
			args = append(args, values...)
		}
		return bind(name, id, 0, types.T_bool, args...)
	}
	literal := func(v int64) *plan.Expr {
		return &plan.Expr{Typ: column.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Datetimeval{Datetimeval: v}}}}
	}
	interior := witness.kind == types.T_datetime && temporalCandidateInteriorTimestampBound(bound)
	lowerRay := interior && (op == types.TemporalLess || op == types.TemporalLessEqual)
	upperRay := interior && (op == types.TemporalGreater || op == types.TemporalGreaterEqual)
	var lo, hi *plan.Expr
	if lowerRay {
		lo = literal(math.MinInt64)
	} else if lo, err = endpoint(0); err != nil {
		return nil, nil, witness, err
	}
	if upperRay {
		hi = literal(math.MaxInt64)
	} else if hi, err = endpoint(1); err != nil {
		return nil, nil, witness, err
	}
	guard, err = bind("__mo_temporal_bound_not_empty", int32(function.TEMPORAL_BOUND_NOT_EMPTY), guardOverload, types.T_bool, DeepCopyExpr(bound), control(int32(op)), control(scale))
	if err != nil {
		return nil, nil, witness, err
	}
	candidate, err = leaf("between", lo, hi)
	if err != nil {
		return nil, nil, witness, err
	}
	if witness.kind == types.T_datetime {
		if !interior {
			point, pointErr := endpoint(2)
			if pointErr != nil {
				return nil, nil, witness, pointErr
			}
			pointExpr, pointErr := leaf("=", point)
			if pointErr != nil {
				return nil, nil, witness, pointErr
			}
			candidate, err = bind("or", int32(function.OR), 0, types.T_bool, candidate, pointExpr)
			if err != nil {
				return nil, nil, witness, err
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
				return nil, nil, witness, tailErr
			}
			candidate, err = bind("or", int32(function.OR), 0, types.T_bool, candidate, tailExpr)
			if err != nil {
				return nil, nil, witness, err
			}
		}
	}
	candidate, err = bind("and", int32(function.AND), 0, types.T_bool, guard, candidate)
	return candidate, guard, witness, err
}

// makeEncodedTemporalCandidate converts a direct candidate to the existing
// serialized-key operators. The caller must prove the prefix exact and the
// index complete, and separately establish admission before marking the hint.
func makeEncodedTemporalCandidateLegacy(ctx context.Context, physicalColumn, component, bound *plan.Expr, prefix []*plan.Expr, hasSuffix bool,
	op types.TemporalOp, scale int32,
) (candidate, guard *plan.Expr, err error) {
	if physicalColumn == nil || physicalColumn.GetCol() == nil || types.T(physicalColumn.Typ.Id) != types.T_varchar {
		return nil, nil, moerr.NewInternalError(ctx, "invalid serialized temporal candidate key")
	}
	direct, _, err := makeDirectTemporalCandidate(ctx, component, bound, op, scale)
	if err != nil {
		return nil, nil, err
	}
	var encode func(*plan.Expr) (*plan.Expr, error)
	encode = func(expr *plan.Expr) (*plan.Expr, error) {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil {
			return nil, moerr.NewInternalError(ctx, "invalid temporal candidate expression")
		}
		switch fn.Func.ObjName {
		case "__mo_temporal_bound_not_empty":
			return DeepCopyExpr(expr), nil
		case "and", "or":
			if len(fn.Args) != 2 {
				return nil, moerr.NewInternalError(ctx, "invalid temporal candidate boolean")
			}
			left, bindErr := encode(fn.Args[0])
			if bindErr != nil {
				return nil, bindErr
			}
			right, bindErr := encode(fn.Args[1])
			if bindErr != nil {
				return nil, bindErr
			}
			return BindFuncExprImplByPlanExpr(ctx, fn.Func.ObjName, []*plan.Expr{left, right})
		case "between", "=":
			if fn.Args[0].GetCol() == nil {
				return nil, moerr.NewInternalError(ctx, "missing temporal candidate component")
			}
			args := []*plan.Expr{DeepCopyExpr(physicalColumn)}
			for _, value := range fn.Args[1:] {
				parts := append(DeepCopyExprList(prefix), DeepCopyExpr(value))
				serialized, bindErr := BindFuncExprImplByPlanExpr(ctx, indexTableComparisonSerialFunc(), parts)
				if bindErr != nil {
					return nil, bindErr
				}
				args = append(args, serialized)
			}
			name := fn.Func.ObjName
			if hasSuffix {
				if name == "between" {
					name = "prefix_between"
				} else {
					name = "prefix_eq"
				}
			}
			return BindFuncExprImplByPlanExpr(ctx, name, args)
		default:
			return nil, moerr.NewInternalError(ctx, "unsupported temporal candidate operator")
		}
	}
	candidate, err = encode(direct)
	if err != nil {
		return nil, nil, err
	}
	return candidate, candidate.GetF().Args[0], nil
}
