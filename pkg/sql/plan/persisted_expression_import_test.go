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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestImportPersistedNumericProgramFreshensWholeWireGroup(t *testing.T) {
	ctx := context.Background()
	valueSource := MakePlan2StringConstExprWithType("31")
	valueSource.GetLit().IsBin = true
	value, kind, err := makeNumericValueProgramOutputs(ctx, valueSource)
	require.NoError(t, err)
	oldScope, oldSlot, ok := numericProgramRefIdentity(value)
	require.True(t, ok)
	require.Equal(t, int64(0), oldSlot)

	// Persist sibling raw and BOOL outputs together, then import that exact
	// catalog representation twice into independent execution owners.
	wire, err := proto.Marshal(&planpb.ExprList{List: []*planpb.Expr{value, kind}})
	require.NoError(t, err)
	decode := func() []*Expr {
		list := new(planpb.ExprList)
		require.NoError(t, proto.Unmarshal(wire, list))
		return list.List
	}
	first, err := importPersistedExprRoots(ctx, decode())
	require.NoError(t, err)
	second, err := importPersistedExprRoots(ctx, decode())
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Len(t, second, 2)

	firstScope, firstSlot, ok := numericProgramRefIdentity(first[0])
	require.True(t, ok)
	require.Equal(t, int64(0), firstSlot)
	firstKindScope, firstKindSlot, ok := numericProgramRefIdentity(first[1])
	require.True(t, ok)
	require.Equal(t, int64(1), firstKindSlot)
	require.Equal(t, firstScope, firstKindScope)
	require.NotEqual(t, oldScope, firstScope)

	secondScope, secondSlot, ok := numericProgramRefIdentity(second[0])
	require.True(t, ok)
	require.Equal(t, int64(0), secondSlot)
	secondKindScope, secondKindSlot, ok := numericProgramRefIdentity(second[1])
	require.True(t, ok)
	require.Equal(t, int64(1), secondKindSlot)
	require.Equal(t, secondScope, secondKindScope)
	require.NotEqual(t, firstScope, secondScope)
	require.Equal(t, oldScope, mustProgramScope(t, value), "import must not mutate catalog-owned protobufs")

	localValue, _, err := makeNumericValueProgramOutputs(ctx, valueSource)
	require.NoError(t, err)
	localScope, _, ok := numericProgramRefIdentity(localValue)
	require.True(t, ok)
	require.NotEqual(t, firstScope, localScope)
	require.NotEqual(t, secondScope, localScope)
	require.Equal(t, oldScope, mustProgramScope(t, DeepCopyExpr(value)), "ordinary semantic copies retain aliases")
}

func TestImportPersistedNumericProgramRejectsMalformedGroups(t *testing.T) {
	ctx := context.Background()
	source := MakePlan2StringConstExprWithType("1")
	source.GetLit().IsBin = true
	value, kind, err := makeNumericValueProgramOutputs(ctx, source)
	require.NoError(t, err)

	t.Run("orphan kind reference", func(t *testing.T) {
		_, err := importPersistedExprRoots(ctx, []*Expr{kind})
		require.Error(t, err)
	})
	t.Run("unknown overload", func(t *testing.T) {
		for _, expr := range []*Expr{value, kind} {
			copy := DeepCopyExpr(expr)
			copy.GetF().Func.Obj = function.EncodeOverloadID(function.SCALAR_PROGRAM_REF, 1)
			_, err := importPersistedExpr(ctx, copy)
			require.Error(t, err)
		}
	})
	t.Run("invalid slot", func(t *testing.T) {
		bad := scalarNumericProgramRef(mustProgramScope(t, value), 2, makeSimplePlan2Type(types.T_bool))
		_, err := importPersistedExpr(ctx, bad)
		require.Error(t, err)
	})
	t.Run("conflicting definitions in one group", func(t *testing.T) {
		conflict := DeepCopyExpr(value)
		definition := conflict.GetF().Args[0]
		definition.GetF().Args[1].GetLit().Value = &planpb.Literal_Sval{Sval: "different"}
		_, err := importPersistedExprRoots(ctx, []*Expr{value, kind, conflict})
		require.Error(t, err)
	})
	t.Run("literal source is rebased but not an executable owner", func(t *testing.T) {
		oldScope := mustProgramScope(t, value)
		valueWitness := MakePlan2StringConstExprWithType("value witness")
		valueWitness.GetLit().Src = DeepCopyExpr(value)
		kindWitness := MakePlan2StringConstExprWithType("kind witness")
		kindWitness.GetLit().Src = DeepCopyExpr(kind)

		imported, err := importPersistedExprRoots(ctx, []*Expr{valueWitness, kindWitness})
		require.NoError(t, err)
		importedValueScope, importedValueSlot, ok := numericProgramRefIdentity(imported[0].GetLit().Src)
		require.True(t, ok)
		require.Zero(t, importedValueSlot)
		importedKindScope, importedKindSlot, ok := numericProgramRefIdentity(imported[1].GetLit().Src)
		require.True(t, ok)
		require.Equal(t, int64(1), importedKindSlot)
		require.NotEqual(t, oldScope, importedValueScope)
		require.Equal(t, importedValueScope, importedKindScope, "one import must preserve the witness pair identity")

		liveKind := DeepCopyExpr(kind)
		witnessOnlyValue := MakePlan2StringConstExprWithType("not an executable owner")
		witnessOnlyValue.GetLit().Src = DeepCopyExpr(value)
		_, err = importPersistedExprRoots(ctx, []*Expr{liveKind, witnessOnlyValue})
		require.Error(t, err, "Literal.Src cannot satisfy a live REF1")
	})
	t.Run("literal source private identity is still validated", func(t *testing.T) {
		witness := MakePlan2StringConstExprWithType("invalid private source")
		witness.GetLit().Src = DeepCopyExpr(value)
		witness.GetLit().Src.GetF().Func.Obj = function.EncodeOverloadID(function.SCALAR_PROGRAM_REF, 1)
		_, err := importPersistedExpr(ctx, witness)
		require.Error(t, err)
	})
}

func TestImportPersistedNumericProgramRefreshesNativeIdentityAndType(t *testing.T) {
	ctx := context.Background()
	native, err := BindFuncExprImplByPlanExpr(ctx, "round", []*Expr{MakePlan2Float64ConstExprWithType(2.5)})
	require.NoError(t, err)
	originalObj := native.GetF().Func.Obj
	originalType := proto.Clone(&native.Typ)
	value, _, err := makeNumericValueProgramOutputsWithKind(ctx, native, MakePlan2BoolConstExprWithType(true))
	require.NoError(t, err)

	imported, err := importPersistedExpr(ctx, value)
	require.NoError(t, err)
	importedSource, ok := numericProgramRawValue(imported)
	require.True(t, ok)
	require.Equal(t, originalObj, importedSource.GetF().Func.Obj, "function refresh must preserve overload")
	require.True(t, proto.Equal(originalType, &importedSource.Typ), "function refresh must preserve bound result metadata")
}

func TestPersistedNumericProgramCycleValidationScansDirectDependencies(t *testing.T) {
	ctx := context.Background()
	const depth = 64
	scopes := make([]int64, depth)
	for i := range scopes {
		scope, err := allocateNumericValueProgramScope(ctx, nil)
		require.NoError(t, err)
		scopes[i] = scope
	}

	root := MakePlan2StringConstExprWithType("1")
	for i := depth - 1; i >= 0; i-- {
		definition := numericValueProgramDefinition(scopes[i], root, nil)
		root = scalarNumericProgramRef(scopes[i], 0, root.Typ)
		root.GetF().Args = append([]*Expr{definition}, root.GetF().Args...)
	}
	group, err := validatePersistedNumericProgramRoots(ctx, []*Expr{root})
	require.NoError(t, err)
	edges, err := collectPersistedNumericProgramDependencies(ctx, group)
	require.NoError(t, err)
	edgeCount := 0
	for _, dependencies := range edges {
		edgeCount += len(dependencies)
	}
	require.Equal(t, depth-1, edgeCount, "each nested program should contribute only its direct REF dependency")

	outerScope, err := allocateNumericValueProgramScope(ctx, nil)
	require.NoError(t, err)
	innerScope, err := allocateNumericValueProgramScope(ctx, nil)
	require.NoError(t, err)
	innerRaw := MakePlan2StringConstExprWithType("1")
	innerDefinition := numericValueProgramDefinition(
		innerScope,
		innerRaw,
		scalarNumericProgramRef(outerScope, 1, makeSimplePlan2Type(types.T_bool)),
	)
	innerValue := scalarNumericProgramRef(innerScope, 0, innerRaw.Typ)
	innerValue.GetF().Args = append([]*Expr{innerDefinition}, innerValue.GetF().Args...)
	outerDefinition := numericValueProgramDefinition(
		outerScope,
		innerValue,
		scalarNumericProgramRef(innerScope, 1, makeSimplePlan2Type(types.T_bool)),
	)
	outerValue := scalarNumericProgramRef(outerScope, 0, innerValue.Typ)
	outerValue.GetF().Args = append([]*Expr{outerDefinition}, outerValue.GetF().Args...)
	_, err = validatePersistedNumericProgramRoots(ctx, []*Expr{outerValue})
	require.ErrorContains(t, err, "program scope graph contains a cycle")
}

func mustProgramScope(t *testing.T, expr *Expr) int64 {
	t.Helper()
	scope, _, ok := numericProgramRefIdentity(expr)
	require.True(t, ok)
	return scope
}
