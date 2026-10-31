// Copyright 2021 - 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package compile

import (
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// ewmFalse/ewmTrue are the two boolean policy constants.
var ewmFalse = plan2.MakePlan2BoolConstExprWithType(false)
var ewmTrue = plan2.MakePlan2BoolConstExprWithType(true)

func ewmValueArg() *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}}
}

// TestConstructEwmConfig pins the per-execution configuration channel
// (§9): valid calls trim the arguments to the value columns and encode
// the resolved configuration; invalid resolved values surface the §7
// families at the construction boundary (no plan mutation, no state).
func TestConstructEwmConfig(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// the row-decay forms
	args, config, err := constructEwmConfig(&plan.Function{
		Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
		Args: []*plan.Expr{
			ewmValueArg(), plan2.MakePlan2Float64ConstExprWithType(0.5),
		},
	}, proc)
	require.NoError(t, err)
	require.Equal(t, 1, len(args))
	cfg, err := aggexec.DecodeEwmConfig(config)
	require.NoError(t, err)
	require.Equal(t, aggexec.EwmModeRow, cfg.Mode)
	require.Equal(t, 0.5, cfg.Alpha)
	require.True(t, cfg.Adjust)
	require.False(t, cfg.IgnoreNA)
	require.Equal(t, int64(0), cfg.MinPeriods)

	// the full policy stack and the adjust=false shape
	args, config, err = constructEwmConfig(&plan.Function{
		Func: &plan.ObjectRef{ObjName: plan2.NameEwmStd},
		Args: []*plan.Expr{
			ewmValueArg(), plan2.MakePlan2Float64ConstExprWithType(0.5),
			ewmFalse, ewmTrue, plan2.MakePlan2Float64ConstExprWithType(3),
		},
	}, proc)
	require.NoError(t, err)
	require.Equal(t, 1, len(args))
	cfg, err = aggexec.DecodeEwmConfig(config)
	require.NoError(t, err)
	require.False(t, cfg.Adjust)
	require.True(t, cfg.IgnoreNA)
	require.Equal(t, int64(3), cfg.MinPeriods)

	// the event-time half-life form (h consumed, arguments trimmed)
	args, config, err = constructEwmConfig(&plan.Function{
		Func: &plan.ObjectRef{ObjName: plan2.NameEwmCorr},
		Args: []*plan.Expr{
			ewmValueArg(), ewmValueArg(),
			plan2.MakePlan2StringConstExprWithType("halflife"),
			plan2.MakePlan2Float64ConstExprWithType(3600),
			ewmTrue,
		},
	}, proc)
	require.NoError(t, err)
	require.Equal(t, 2, len(args))
	cfg, err = aggexec.DecodeEwmConfig(config)
	require.NoError(t, err)
	require.Equal(t, aggexec.EwmModeTime, cfg.Mode)
	require.Equal(t, 3600.0, cfg.HalfLife)
	require.True(t, cfg.Adjust)

	// the event-time tag form with the full policy suffix (the round-5
	// black-box challenge's five-argument shape): the explicit
	// adjust/ignoreNA positions shift behind the tag-h pair
	args, config, err = constructEwmConfig(&plan.Function{
		Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
		Args: []*plan.Expr{
			ewmValueArg(),
			plan2.MakePlan2StringConstExprWithType("halflife"),
			plan2.MakePlan2Float64ConstExprWithType(1),
			ewmFalse, ewmFalse,
		},
	}, proc)
	require.NoError(t, err)
	require.Equal(t, 1, len(args))
	cfg, err = aggexec.DecodeEwmConfig(config)
	require.NoError(t, err)
	require.Equal(t, aggexec.EwmModeTime, cfg.Mode)
	require.Equal(t, 1.0, cfg.HalfLife)
	require.False(t, cfg.Adjust)
	require.False(t, cfg.IgnoreNA)
	require.Equal(t, int64(0), cfg.MinPeriods)

	args, config, err = constructEwmConfig(&plan.Function{
		Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
		Args: []*plan.Expr{
			ewmValueArg(),
			plan2.MakePlan2StringConstExprWithType("halflife"),
			plan2.MakePlan2Float64ConstExprWithType(2),
			ewmFalse, ewmTrue,
		},
	}, proc)
	require.NoError(t, err)
	require.Equal(t, 1, len(args))
	cfg, err = aggexec.DecodeEwmConfig(config)
	require.NoError(t, err)
	require.Equal(t, aggexec.EwmModeTime, cfg.Mode)
	require.Equal(t, 2.0, cfg.HalfLife)
	require.False(t, cfg.Adjust)
	require.True(t, cfg.IgnoreNA)

	args, config, err = constructEwmConfig(&plan.Function{
		Func: &plan.ObjectRef{ObjName: plan2.NameEwmVar},
		Args: []*plan.Expr{
			ewmValueArg(),
			plan2.MakePlan2StringConstExprWithType("halflife"),
			plan2.MakePlan2Float64ConstExprWithType(4),
			ewmFalse, ewmFalse,
			plan2.MakePlan2Float64ConstExprWithType(2),
		},
	}, proc)
	require.NoError(t, err)
	require.Equal(t, 1, len(args))
	cfg, err = aggexec.DecodeEwmConfig(config)
	require.NoError(t, err)
	require.Equal(t, aggexec.EwmModeTime, cfg.Mode)
	require.Equal(t, 4.0, cfg.HalfLife)
	require.False(t, cfg.Adjust)
	require.False(t, cfg.IgnoreNA)
	require.Equal(t, int64(2), cfg.MinPeriods)

	// a prepared marker may resolve through the text form of a numeric
	// decay and h; the binder keeps quoted literals away from this path
	args, config, err = constructEwmConfig(&plan.Function{
		Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
		Args: []*plan.Expr{
			ewmValueArg(), plan2.MakePlan2StringConstExprWithType("0.25"),
		},
	}, proc)
	require.NoError(t, err)
	require.Equal(t, 1, len(args))
	cfg, err = aggexec.DecodeEwmConfig(config)
	require.NoError(t, err)
	require.Equal(t, aggexec.EwmModeRow, cfg.Mode)
	require.Equal(t, 0.25, cfg.Alpha)
	require.Equal(t, int64(0), cfg.MinPeriods)

	args, config, err = constructEwmConfig(&plan.Function{
		Func: &plan.ObjectRef{ObjName: plan2.NameEwmVar},
		Args: []*plan.Expr{
			ewmValueArg(),
			plan2.MakePlan2StringConstExprWithType("halflife"),
			plan2.MakePlan2StringConstExprWithType("60"),
		},
	}, proc)
	require.NoError(t, err)
	require.Equal(t, 1, len(args))
	cfg, err = aggexec.DecodeEwmConfig(config)
	require.NoError(t, err)
	require.Equal(t, aggexec.EwmModeTime, cfg.Mode)
	require.Equal(t, 60.0, cfg.HalfLife)

	// invalid resolved values (§7): the declared-decay family
	for _, tc := range []struct {
		name string
		fn   *plan.Function
		want string
	}{
		{
			"alpha zero",
			&plan.Function{Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
				Args: []*plan.Expr{ewmValueArg(), plan2.MakePlan2Float64ConstExprWithType(0.0)}},
			"ewm statistics require alpha in (0,1]",
		},
		{
			"alpha above one",
			&plan.Function{Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
				Args: []*plan.Expr{ewmValueArg(), plan2.MakePlan2Float64ConstExprWithType(1.5)}},
			"ewm statistics require alpha in (0,1]",
		},
		{
			"alpha negative",
			&plan.Function{Func: &plan.ObjectRef{ObjName: plan2.NameEwmVar},
				Args: []*plan.Expr{ewmValueArg(), plan2.MakePlan2Float64ConstExprWithType(-0.25)}},
			"ewm statistics require alpha in (0,1]",
		},
		{
			"halflife zero",
			&plan.Function{Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
				Args: []*plan.Expr{ewmValueArg(),
					plan2.MakePlan2StringConstExprWithType("halflife"),
					plan2.MakePlan2Float64ConstExprWithType(0)}},
			"ewm halflife decay requires a strictly positive half-life constant",
		},
		{
			"halflife negative",
			&plan.Function{Func: &plan.ObjectRef{ObjName: plan2.NameEwmCov},
				Args: []*plan.Expr{ewmValueArg(), ewmValueArg(),
					plan2.MakePlan2StringConstExprWithType("halflife"),
					plan2.MakePlan2Float64ConstExprWithType(-1)}},
			"ewm halflife decay requires a strictly positive half-life constant",
		},
		{
			"halflife missing after tag",
			&plan.Function{Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
				Args: []*plan.Expr{ewmValueArg(),
					plan2.MakePlan2StringConstExprWithType("halflife")}},
			"ewm halflife decay requires a strictly positive half-life constant",
		},
		{
			"unknown string tag",
			&plan.Function{Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
				Args: []*plan.Expr{ewmValueArg(), plan2.MakePlan2StringConstExprWithType("span")}},
			"ewm statistics require a declared decay (alpha or 'halflife' half-life)",
		},
		{
			"minPeriods negative",
			&plan.Function{Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
				Args: []*plan.Expr{ewmValueArg(), plan2.MakePlan2Float64ConstExprWithType(0.5),
					ewmTrue, ewmFalse, plan2.MakePlan2Float64ConstExprWithType(-1)}},
			"ewm statistics require minPeriods to be a constant integer >= 0",
		},
		{
			"minPeriods fractional",
			&plan.Function{Func: &plan.ObjectRef{ObjName: plan2.NameEwmStd},
				Args: []*plan.Expr{ewmValueArg(), plan2.MakePlan2Float64ConstExprWithType(0.5),
					ewmTrue, ewmFalse, plan2.MakePlan2Float64ConstExprWithType(1.5)}},
			"ewm statistics require minPeriods to be a constant integer >= 0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := constructEwmConfig(tc.fn, proc)
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), tc.want),
				"error %q missing %q", err.Error(), tc.want)
		})
	}
}

// TestValidateEwmWindowConfigsAfterModeResolution: after the per-execution
// configuration boundary resolves the decay mode, an event-time
// configuration must satisfy its ORDER BY shape (exactly one ascending
// numeric/temporal key) — a deferred marker that resolves to 'halflife'
// surfaces the §7 event-time texts at this boundary — while a numeric
// marker keeps the row-mode ORDER BY latitude at every compile.
func TestValidateEwmWindowConfigsAfterModeResolution(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	ewmWindowNode := func(decay string, h string, flags []int32, keyType int32) *plan.Node {
		args := []*plan.Expr{ewmValueArg(),
			plan2.MakePlan2StringConstExprWithType(decay)}
		if h != "" {
			args = append(args, plan2.MakePlan2StringConstExprWithType(h))
		}
		orderBy := make([]*plan.OrderBySpec, len(flags))
		for i, flag := range flags {
			orderBy[i] = &plan.OrderBySpec{
				Expr: &plan.Expr{Typ: plan.Type{Id: keyType}},
				Flag: plan.OrderBySpec_OrderByFlag(flag),
			}
		}
		return &plan.Node{WinSpecList: []*plan.Expr{{
			Typ: plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Name: plan2.NameEwmMean,
				WindowFunc: &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)},
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
						Args: args}}},
				OrderBy: orderBy,
			}},
		}}}
	}

	for _, tc := range []struct {
		name string
		node *plan.Node
		want string
	}{
		{
			"halflife marker with descending order",
			ewmWindowNode("halflife", "60", []int32{int32(plan.OrderBySpec_DESC)}, int32(types.T_int64)),
			"event-time ewm statistics require ascending ORDER BY",
		},
		{
			"halflife marker with multiple order keys",
			ewmWindowNode("halflife", "60", []int32{0, 0}, int32(types.T_int64)),
			"event-time ewm statistics require exactly one ORDER BY expression",
		},
		{
			"halflife marker with non-numeric key",
			ewmWindowNode("halflife", "60", []int32{0}, int32(types.T_varchar)),
			"event-time ewm statistics require a numeric or temporal ORDER BY key",
		},
		{
			"halflife marker with one ascending numeric key",
			ewmWindowNode("halflife", "60", []int32{0}, int32(types.T_int64)),
			"",
		},
		{
			"numeric marker with descending order stays row mode",
			ewmWindowNode("0.5", "", []int32{int32(plan.OrderBySpec_DESC)}, int32(types.T_int64)),
			"",
		},
		{
			"numeric marker with multiple order keys stays row mode",
			ewmWindowNode("0.5", "", []int32{0, int32(plan.OrderBySpec_DESC)}, int32(types.T_varchar)),
			"",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAggregateConfigs(tc.node, proc)
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

// TestConstructEwmConfigNonFinitePolicy pins the policy-domain rule of
// the eighth pre-PR review round at the per-execution configuration
// boundary (§7: non-finite resolved policy values are rejections,
// never silently saturated): a numeric NaN or either infinity in the
// adjust or ignoreNA position — reachable through a constant numeric
// expression such as a SQL cast, which the numeric registry and the
// constant-expression checks admit — rejects with the constant
// adjust/ignoreNA booleans family instead of resolving through the
// nonzero test to true, in both the row-decay and the tag-form
// argument shapes; the boolean literals and the finite 0/1 (and
// nonzero-finite) numerics keep resolving exactly at both positions.
func TestConstructEwmConfigNonFinitePolicy(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	for name, v := range map[string]float64{
		"NaN":  math.NaN(),
		"+Inf": math.Inf(1),
		"-Inf": math.Inf(-1),
	} {
		// the row-decay adjust position
		_, _, err := constructEwmConfig(&plan.Function{
			Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
			Args: []*plan.Expr{ewmValueArg(),
				plan2.MakePlan2Float64ConstExprWithType(0.5),
				plan2.MakePlan2Float64ConstExprWithType(v)},
		}, proc)
		require.Error(t, err, "row-decay adjust position with %s", name)
		require.Contains(t, err.Error(),
			"ewm statistics require constant adjust and ignoreNA booleans", name)

		// the row-decay ignoreNA position
		_, _, err = constructEwmConfig(&plan.Function{
			Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
			Args: []*plan.Expr{ewmValueArg(),
				plan2.MakePlan2Float64ConstExprWithType(0.5), ewmTrue,
				plan2.MakePlan2Float64ConstExprWithType(v)},
		}, proc)
		require.Error(t, err, "row-decay ignoreNA position with %s", name)
		require.Contains(t, err.Error(),
			"ewm statistics require constant adjust and ignoreNA booleans", name)

		// the tag-form cov family: the policy suffix shifts behind the
		// tag-h pair; both positions reject there too
		_, _, err = constructEwmConfig(&plan.Function{
			Func: &plan.ObjectRef{ObjName: plan2.NameEwmCov},
			Args: []*plan.Expr{ewmValueArg(), ewmValueArg(),
				plan2.MakePlan2StringConstExprWithType("halflife"),
				plan2.MakePlan2Float64ConstExprWithType(1),
				plan2.MakePlan2Float64ConstExprWithType(v), ewmTrue},
		}, proc)
		require.Error(t, err, "tag-form adjust position with %s", name)
		require.Contains(t, err.Error(),
			"ewm statistics require constant adjust and ignoreNA booleans", name)

		_, _, err = constructEwmConfig(&plan.Function{
			Func: &plan.ObjectRef{ObjName: plan2.NameEwmCorr},
			Args: []*plan.Expr{ewmValueArg(), ewmValueArg(),
				plan2.MakePlan2StringConstExprWithType("halflife"),
				plan2.MakePlan2Float64ConstExprWithType(1), ewmFalse,
				plan2.MakePlan2Float64ConstExprWithType(v)},
		}, proc)
		require.Error(t, err, "tag-form ignoreNA position with %s", name)
		require.Contains(t, err.Error(),
			"ewm statistics require constant adjust and ignoreNA booleans", name)
	}

	// the finite forms keep resolving exactly at both positions: the
	// boolean literals, the integer 0/1 spellings and a nonzero finite
	// numeric
	for name, tc := range map[string]struct {
		adj, ina         *plan.Expr
		wantAdj, wantIna bool
	}{
		"true literals":  {ewmTrue, ewmTrue, true, true},
		"false literals": {ewmFalse, ewmFalse, false, false},
		"integer 0/1": {plan2.MakePlan2Float64ConstExprWithType(1),
			plan2.MakePlan2Float64ConstExprWithType(0), true, false},
		"nonzero finite": {plan2.MakePlan2Float64ConstExprWithType(2),
			plan2.MakePlan2Float64ConstExprWithType(0.5), true, true},
	} {
		_, config, err := constructEwmConfig(&plan.Function{
			Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
			Args: []*plan.Expr{ewmValueArg(),
				plan2.MakePlan2Float64ConstExprWithType(0.5), tc.adj, tc.ina},
		}, proc)
		require.NoError(t, err, name)
		cfg, derr := aggexec.DecodeEwmConfig(config)
		require.NoError(t, derr, name)
		require.Equal(t, tc.wantAdj, cfg.Adjust, name)
		require.Equal(t, tc.wantIna, cfg.IgnoreNA, name)
	}
}

// ewmMinPeriodsConstExpr builders carry the constant in its ORIGINAL
// numeric domain — the declared integer/int/float widths, not a
// pre-converted float64 — so the per-execution boundary can judge
// integrality exactly. The decimal fixtures construct their raw
// significands directly (the SQL CAST path produced exactly these
// values for the adjacent-fraction spellings).
func ewmMinPeriodsDecimal64ConstExpr(
	t *testing.T, raw int64, width, scale int32,
) *plan.Expr {
	t.Helper()
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_decimal64), Width: width, Scale: scale},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: raw}},
		}},
	}
}

func ewmMinPeriodsDecimal128ConstExpr(
	t *testing.T, raw *big.Int, width, scale int32,
) *plan.Expr {
	t.Helper()
	lo := new(big.Int).And(raw, mask64).Uint64()
	hi := new(big.Int).Rsh(raw, 64).Uint64()
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_decimal128), Width: width, Scale: scale},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Decimal128Val{
				Decimal128Val: &plan.Decimal128{A: int64(lo), B: int64(hi)}},
		}},
	}
}

var mask64 = new(big.Int).Sub(
	new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(1))

// ewmMinPeriodsDecimal256CastConstExpr binds an explicit cast of a
// string literal into the DECIMAL256 carrier: decimal spellings wider
// than 38 digits (MatrixOne admits precision up to 65) have no plan
// literal variant, so the constant rides the registry's cast exactly
// as production SQL binds it — the resolver sees the original 256-bit
// significand and its scale, never a float64 view.
func ewmMinPeriodsDecimal256CastConstExpr(
	t *testing.T, proc *process.Process, text string, width, scale int32,
) *plan.Expr {
	t.Helper()
	source := types.T_varchar.ToType()
	target := types.New(types.T_decimal256, width, scale)
	resolved, err := function.GetFunctionByName(
		proc.Ctx, "cast", []types.Type{source, target})
	require.NoError(t, err)
	typ := plan.Type{Id: int32(target.Oid), Width: target.Width, Scale: target.Scale}
	return &plan.Expr{Typ: typ, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "cast", Obj: resolved.GetEncodedOverloadID()},
		Args: []*plan.Expr{
			{
				Typ: source.PlanType(),
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{
					Value: &plan.Literal_Sval{Sval: text},
				}},
			},
			{Typ: typ, Expr: &plan.Expr_T{T: &plan.TargetType{}}},
		},
	}}}
}

// pow10BigOf returns 10^n exactly.
func pow10BigOf(n int64) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(n), nil)
}

func ewmMeanCallWithMinPeriods(mp *plan.Expr) *plan.Function {
	return &plan.Function{
		Func: &plan.ObjectRef{ObjName: plan2.NameEwmMean},
		Args: []*plan.Expr{
			ewmValueArg(),
			plan2.MakePlan2Float64ConstExprWithType(0.5),
			ewmTrue, ewmFalse, mp,
		},
	}
}

// TestConstructEwmConfigMinPeriodsIntegrality (eleventh review round):
// minPeriods is a §4 constant INTEGER, judged in the constant's original
// numeric domain — a DECIMAL fraction whose float64 view rounds to an
// integer (999999999999999999 * 10^-18 is exactly 1.0 after the
// DECIMAL64→float64 conversion) must reject with the §7 minPeriods
// family instead of silently encoding threshold 1, while integral
// decimal spellings stay ordinary thresholds.
func TestConstructEwmConfigMinPeriodsIntegrality(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// raw 10^18-1 at scale 18 is the exact 0.999999999999999999 the
	// review's CAST witness produced (float64 view exactly 1.0)
	adjacentBelowOne64 := ewmMinPeriodsDecimal64ConstExpr(
		t, 999999999999999999, 18, 18)
	adjacentBelowOne128 := ewmMinPeriodsDecimal128ConstExpr(
		t, new(big.Int).Sub(pow10BigOf(38), big.NewInt(1)), 38, 38)
	adjacentBelowOne256 := ewmMinPeriodsDecimal256CastConstExpr(
		t, proc, "0."+strings.Repeat("9", 64), 64, 64)

	for _, tc := range []struct {
		name string
		mp   *plan.Expr
	}{
		{
			"decimal64 fraction that float64 rounds to one",
			adjacentBelowOne64,
		},
		{
			"decimal128 fraction that float64 rounds to one",
			adjacentBelowOne128,
		},
		{
			"decimal256 fraction that float64 rounds to one",
			adjacentBelowOne256,
		},
		{
			"decimal64 negative fraction",
			ewmMinPeriodsDecimal64ConstExpr(t, -15, 18, 1),
		},
		{
			"float64 fractional control",
			plan2.MakePlan2Float64ConstExprWithType(0.5),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := constructEwmConfig(ewmMeanCallWithMinPeriods(tc.mp), proc)
			require.Error(t, err, "minPeriods must be rejected in its original domain")
			require.True(t, strings.Contains(err.Error(),
				"ewm statistics require minPeriods to be a constant integer >= 0"),
				"error %q missing the minPeriods family", err.Error())
		})
	}

	// integral decimal controls resolve at their exact whole-number
	// thresholds — the §4 integer the declared spelling carries
	args, config, err := constructEwmConfig(
		ewmMeanCallWithMinPeriods(ewmMinPeriodsDecimal64ConstExpr(t, 20, 18, 1)), proc)
	require.NoError(t, err)
	require.Equal(t, 1, len(args))
	cfg, derr := aggexec.DecodeEwmConfig(config)
	require.NoError(t, derr)
	require.Equal(t, int64(2), int64(cfg.MinPeriods))

	args, config, err = constructEwmConfig(
		ewmMeanCallWithMinPeriods(
			ewmMinPeriodsDecimal128ConstExpr(t, big.NewInt(1000), 38, 2)), proc)
	require.NoError(t, err)
	require.Equal(t, 1, len(args))
	cfg, derr = aggexec.DecodeEwmConfig(config)
	require.NoError(t, derr)
	require.Equal(t, int64(10), int64(cfg.MinPeriods))

	// the 256-bit carrier keeps the same rule: an integral spelling is
	// its exact whole-number threshold
	args, config, err = constructEwmConfig(
		ewmMeanCallWithMinPeriods(
			ewmMinPeriodsDecimal256CastConstExpr(t, proc, "2.0", 64, 1)), proc)
	require.NoError(t, err)
	require.Equal(t, 1, len(args))
	cfg, derr = aggexec.DecodeEwmConfig(config)
	require.NoError(t, derr)
	require.Equal(t, int64(2), int64(cfg.MinPeriods))
}

// TestConstructEwmConfigMinPeriodsUpperBound (eleventh review round):
// §4 declares minPeriods a constant integer >= 0 with no upper limit,
// so the int32 cap is gone — 2147483648 is an ordinary threshold that
// merely publishes NULL on small fixtures — and the exact integer
// travels from its original domain (4611686018427387905 is not
// float64-exact: float64(2^62+1) == 2^62, and the configured threshold
// must still decode exactly). Thresholds beyond the int64 axis saturate
// to math.MaxInt64, observationally exact because the C-gate counts rows
// of one partition and can never reach it.
func TestConstructEwmConfigMinPeriodsUpperBound(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	decode := func(mp *plan.Expr) aggexec.EwmConfig {
		args, config, err := constructEwmConfig(ewmMeanCallWithMinPeriods(mp), proc)
		require.NoError(t, err)
		require.Equal(t, 1, len(args))
		cfg, derr := aggexec.DecodeEwmConfig(config)
		require.NoError(t, derr)
		return cfg
	}

	require.Equal(t, int64(2147483647),
		int64(decode(plan2.MakePlan2Int64ConstExprWithType(2147483647)).MinPeriods),
		"the old int32 boundary stays an ordinary threshold")
	require.Equal(t, int64(2147483648),
		int64(decode(plan2.MakePlan2Int64ConstExprWithType(2147483648)).MinPeriods),
		"§4 has no int32 limit: 2147483648 is a valid threshold")
	require.Equal(t, int64(4611686018427387905),
		int64(decode(plan2.MakePlan2Int64ConstExprWithType(4611686018427387905)).MinPeriods),
		"the exact integer survives its original int64 domain (not float64(2^62+1)==2^62)")
	require.Equal(t, int64(math.MaxInt64),
		int64(decode(plan2.MakePlan2Int64ConstExprWithType(math.MaxInt64)).MinPeriods))
	require.Equal(t, int64(math.MaxInt64),
		int64(decode(plan2.MakePlan2Uint64ConstExprWithType(math.MaxUint64)).MinPeriods),
		"a uint64 threshold above int64 saturates to the unreachable maximum")
	require.Equal(t, int64(math.MaxInt64),
		int64(decode(plan2.MakePlan2Float64ConstExprWithType(1e19)).MinPeriods),
		"an integral float64 threshold above int64 saturates to the unreachable maximum")
	require.Equal(t, int64(math.MaxInt64),
		int64(decode(ewmMinPeriodsDecimal128ConstExpr(
			t, new(big.Int).Sub(pow10BigOf(38), big.NewInt(1)), 38, 0)).MinPeriods),
		"an integral decimal threshold above int64 saturates to the unreachable maximum")
	require.Equal(t, int64(math.MaxInt64),
		int64(decode(ewmMinPeriodsDecimal256CastConstExpr(
			t, proc, strings.Repeat("9", 64), 64, 0)).MinPeriods),
		"an integral decimal256 threshold above int64 saturates to the unreachable maximum")
}
