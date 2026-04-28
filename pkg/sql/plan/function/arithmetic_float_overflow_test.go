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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestFloatArithmeticOverflowReturnsError(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "float64 addition overflow returns out-of-range error",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(), []float64{math.MaxFloat64}, nil),
			NewFunctionTestInput(types.T_float64.ToType(), []float64{math.MaxFloat64}, nil),
		},
		expect: NewFunctionTestResult(types.T_float64.ToType(), true, []float64{0}, nil),
	}

	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func TestFloatArithmeticHelpers(t *testing.T) {
	ctx := context.Background()

	// addFloat64 / addFloat32
	if v, err := addFloat64WithOverflowCheck(ctx, 1.0, 2.0); err != nil || v != 3.0 {
		t.Fatalf("addFloat64 normal: v=%v err=%v", v, err)
	}
	if _, err := addFloat64WithOverflowCheck(ctx, math.MaxFloat64, math.MaxFloat64); err == nil {
		t.Fatal("addFloat64 overflow expected error")
	}
	if v, err := addFloat32WithOverflowCheck(ctx, 1.0, 2.0); err != nil || v != 3.0 {
		t.Fatalf("addFloat32 normal: v=%v err=%v", v, err)
	}
	if _, err := addFloat32WithOverflowCheck(ctx, math.MaxFloat32, math.MaxFloat32); err == nil {
		t.Fatal("addFloat32 overflow expected error")
	}

	// subFloat64 / subFloat32
	if v, err := subFloat64WithOverflowCheck(ctx, 3.0, 1.0); err != nil || v != 2.0 {
		t.Fatalf("subFloat64 normal: v=%v err=%v", v, err)
	}
	if _, err := subFloat64WithOverflowCheck(ctx, -math.MaxFloat64, math.MaxFloat64); err == nil {
		t.Fatal("subFloat64 overflow expected error")
	}
	if v, err := subFloat32WithOverflowCheck(ctx, 3.0, 1.0); err != nil || v != 2.0 {
		t.Fatalf("subFloat32 normal: v=%v err=%v", v, err)
	}
	if _, err := subFloat32WithOverflowCheck(ctx, -math.MaxFloat32, math.MaxFloat32); err == nil {
		t.Fatal("subFloat32 overflow expected error")
	}

	// mulFloat64 / mulFloat32
	if v, err := mulFloat64WithOverflowCheck(ctx, 2.0, 3.0); err != nil || v != 6.0 {
		t.Fatalf("mulFloat64 normal: v=%v err=%v", v, err)
	}
	if _, err := mulFloat64WithOverflowCheck(ctx, math.MaxFloat64, 2.0); err == nil {
		t.Fatal("mulFloat64 overflow expected error")
	}
	if v, err := mulFloat32WithOverflowCheck(ctx, 2.0, 3.0); err != nil || v != 6.0 {
		t.Fatalf("mulFloat32 normal: v=%v err=%v", v, err)
	}
	if _, err := mulFloat32WithOverflowCheck(ctx, math.MaxFloat32, 2.0); err == nil {
		t.Fatal("mulFloat32 overflow expected error")
	}

	// divFloat64 / divFloat32 — overflow when dividing by a tiny number.
	if v, err := divFloat64WithOverflowCheck(ctx, 6.0, 2.0); err != nil || v != 3.0 {
		t.Fatalf("divFloat64 normal: v=%v err=%v", v, err)
	}
	if _, err := divFloat64WithOverflowCheck(ctx, math.MaxFloat64, math.SmallestNonzeroFloat64); err == nil {
		t.Fatal("divFloat64 overflow expected error")
	}
	if v, err := divFloat32WithOverflowCheck(ctx, 6.0, 2.0); err != nil || v != 3.0 {
		t.Fatalf("divFloat32 normal: v=%v err=%v", v, err)
	}
	if _, err := divFloat32WithOverflowCheck(ctx, math.MaxFloat32, math.SmallestNonzeroFloat32); err == nil {
		t.Fatal("divFloat32 overflow expected error")
	}
}

func TestExpOverflowReturnsError(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "exp overflow returns out-of-range error",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(), []float64{1000}, nil),
		},
		expect: NewFunctionTestResult(types.T_float64.ToType(), true, []float64{0}, nil),
	}

	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInExp)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}
