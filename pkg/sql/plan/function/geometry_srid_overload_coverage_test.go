// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// The SRID-aware overloads are selected through the function registry.  The
// ordinary function test harness receives an evaluator directly and therefore
// cannot prove that each registry entry's retType/newOp closures are wired to
// the same implementation.  Keep this small test on the public resolver path.
func TestGeometrySRIDOverloadRegistry(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	mp := proc.Mp()
	// Use the canonical point payload produced by the geo package rather than
	// hand-rolled bytes; this also keeps the execute-side oracle meaningful.
	wkb := geo.WriteWKB(geo.Point{X: 1, Y: 2})

	for _, inputType := range []types.T{types.T_varchar, types.T_blob, types.T_varbinary} {
		t.Run("st_geomfromwkb/"+inputType.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), "st_geomfromwkb", []types.Type{inputType.ToType(), types.T_int64.ToType()})
			require.NoError(t, err)
			require.Equal(t, types.T_geometry, resolved.GetReturnType().Oid)

			source := testutil.MakeVarlenaVector([][]byte{wkb}, nil, inputType.ToType(), mp)
			srid := testutil.MakeInt64Vector([]int64{4326}, nil, mp)
			defer source.Free(mp)
			defer srid.Free(mp)
			out, err := RunFunctionDirectly(proc, resolved.GetEncodedOverloadID(), []*vector.Vector{source, srid}, 1)
			require.NoError(t, err)
			defer out.Free(mp)
			require.Equal(t, wkb, out.GetBytesAt(0))
		})
	}

	for _, inputType := range []types.T{types.T_geometry, types.T_geometry32} {
		t.Run("st_srid/"+inputType.String(), func(t *testing.T) {
			argType := inputType.ToType()
			if inputType == types.T_geometry32 {
				argType.Scale = 1
			}
			resolved, err := GetFunctionByName(context.Background(), "st_srid", []types.Type{argType, types.T_int64.ToType()})
			require.NoError(t, err)
			require.Equal(t, inputType, resolved.GetReturnType().Oid)

			payload := geo.WriteWKB(geo.Point{X: 1, Y: 2})
			if inputType == types.T_geometry32 {
				payload, err = geo.WriteWKBFloat32(geo.Point{X: 1, Y: 2})
				require.NoError(t, err)
			}
			source := testutil.MakeVarlenaVector([][]byte{payload}, nil, argType, mp)
			srid := testutil.MakeInt64Vector([]int64{0}, nil, mp)
			defer source.Free(mp)
			defer srid.Free(mp)
			out, err := RunFunctionDirectly(proc, resolved.GetEncodedOverloadID(), []*vector.Vector{source, srid}, 1)
			require.NoError(t, err)
			defer out.Free(mp)
			require.Equal(t, payload, out.GetBytesAt(0))
		})
	}
}

func TestGeometrySRIDAwareConstructorsHonorSelectionAndNullContracts(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	wkb := string(geo.WriteWKB(geo.Point{X: 1, Y: 2}))

	// A masked row must not decode its payload or validate its SRID.  This is
	// the short-circuit contract used by WHERE/CASE evaluation.
	wkbCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-wkb", wkb}, []bool{false, false}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, 4326}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"", wkb}, []bool{true, false}),
		StGeomFromWKBWithSRID).WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
	ok, info := wkbCase.Run()
	require.True(t, ok, info)

	// All rows masked still produce a cardinality-preserving NULL result and do
	// not surface an invalid SRID from an unselected branch.
	allMasked := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-wkb"}, []bool{false}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1}, []bool{false}),
		},
		NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{true}),
		StGeomFromWKBWithSRID).WithSelectList(&FunctionSelectList{AllNull: true})
	ok, info = allMasked.Run()
	require.True(t, ok, info)

	// NULL propagation precedes SRID validation, while a non-NULL source with
	// an out-of-range SRID remains an error.
	nullSource := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1}, []bool{false}),
		},
		NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{true}), StGeomFromWKBWithSRID)
	ok, info = nullSource.Run()
	require.True(t, ok, info)

	for _, srid := range []int64{-1, int64(geo.MaxSRID) + 1} {
		badSRID := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{wkb}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{srid}, []bool{false}),
			}, NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil), StGeomFromWKBWithSRID)
		ok, info = badSRID.Run()
		require.True(t, ok, info)
	}

	badPayload := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-wkb"}, []bool{false}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{4326}, []bool{false}),
		}, NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil), StGeomFromWKBWithSRID)
	ok, info = badPayload.Run()
	require.True(t, ok, info)

	// The same selection/null behavior applies to the ST_SRID setter, including
	// the float32 reader used by GEOMETRY32 values.
	float32Type := types.T_geometry32.ToType()
	float32Payload, err := geo.WriteWKBFloat32(geo.Point{X: 1, Y: 2})
	require.NoError(t, err)
	setter := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(float32Type, []string{"not-wkb", string(float32Payload)}, []bool{false, false}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, 4326}, []bool{false, false}),
		}, NewFunctionTestResult(float32Type, false, []string{"", string(float32Payload)}, []bool{true, false}),
		StSRIDWithSRID).WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
	ok, info = setter.Run()
	require.True(t, ok, info)

	allSetterMasked := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(float32Type, []string{"not-wkb"}, []bool{false}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1}, []bool{false}),
		}, NewFunctionTestResult(float32Type, false, []string{""}, []bool{true}), StSRIDWithSRID).
		WithSelectList(&FunctionSelectList{AllNull: true})
	ok, info = allSetterMasked.Run()
	require.True(t, ok, info)
}
