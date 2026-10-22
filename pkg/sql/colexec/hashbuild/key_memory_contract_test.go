// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package hashbuild

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func contractCall(name string, result types.T, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(result)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: name}, Args: args}}}
}
func TestSpillKeyMemoryContracts(t *testing.T) {
	fixed := newExpr(0, types.T_int32.ToType())
	variable := newExpr(1, types.T_varchar.ToType())
	for _, name := range []string{"json_valid", "json_length", "length", "crc32"} {
		require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{contractCall(name, types.T_bool, variable)}), "fixed result does not bound hidden parse/hash scratch: %s", name)
	}
	require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{contractCall("+", types.T_int32, contractCall("json_length", types.T_int32, variable), fixed)}), "all intermediate functions must have a contract")
	require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "literal"}}}}}))
	fixedExpr := contractCall("+", types.T_int32, fixed, fixed)
	require.True(t, SupportsBoundedSpillKeys([]*plan.Expr{fixedExpr}))
	plain, ok := SpillKeyMemoryBound([]*plan.Expr{fixed}, 1<<20, 64)
	require.True(t, ok)
	derived, ok := SpillKeyMemoryBound([]*plan.Expr{fixedExpr}, 1<<20, 64)
	require.True(t, ok)
	require.Greater(t, derived.ExpressionBytes, plain.ExpressionBytes)
	require.Equal(t, plain.EncodedUnit, derived.EncodedUnit)
	for _, name := range []string{"serial", "serial_full"} {
		one := contractCall(name, types.T_varchar, variable, fixed)
		require.True(t, SupportsBoundedSpillKeys([]*plan.Expr{one}))
		single, ok := SpillKeyMemoryBound([]*plan.Expr{one}, 1<<20, 64)
		require.True(t, ok)
		repeated, ok := SpillKeyMemoryBound([]*plan.Expr{contractCall(name, types.T_varchar, variable, variable, fixed)}, 1<<20, 64)
		require.True(t, ok)
		require.Greater(t, repeated.EncodedUnit, single.EncodedUnit)
		require.Greater(t, repeated.ExpressionBytes, single.ExpressionBytes)
		require.True(t, single.StringKeys)
		_, ok = SpillKeyMemoryBound([]*plan.Expr{one}, math.MaxInt64, 8192)
		require.False(t, ok, "overflow must retire eligibility before allocation")
	}
	null := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	require.True(t, SupportsBoundedSpillKeys([]*plan.Expr{contractCall("serial_full", types.T_varchar, null, fixed)}))
}
