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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestNullabilityRequiresPositiveProof(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []types.Type
		want bool
	}{
		{"regexp_substr", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}, false},
		{"sqrt", []types.Type{types.T_float64.ToType()}, false},
		{"ln", []types.Type{types.T_float64.ToType()}, false},
		{"unhex", []types.Type{types.T_varchar.ToType()}, false},
		{"=", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, true},
		{"+", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, true},
		{"count", []types.Type{types.T_int32.ToType()}, true},
		{"case", []types.Type{types.T_bool.ToType(), types.T_int32.ToType()}, false},
		{"case", []types.Type{types.T_bool.ToType(), types.T_int32.ToType(), types.T_int32.ToType()}, true},
		{"cast", []types.Type{types.T_varchar.ToType(), types.T_date.ToType()}, false},
		{"cast", []types.Type{types.T_int32.ToType(), types.T_int64.ToType()}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn, err := GetFunctionByName(context.Background(), tc.name, tc.args)
			require.NoError(t, err)
			args := make([]*plan.Expr, len(tc.args))
			for i, tp := range tc.args {
				args[i] = &plan.Expr{Typ: plan.Type{Id: int32(tp.Oid), NotNullable: true}}
			}
			require.Equal(t, tc.want, DeduceNotNullable(fn.GetEncodedOverloadID(), args))
		})
	}
}

func TestRandomBytesVolatileMetadata(t *testing.T) {
	for _, tp := range []types.T{types.T_int64, types.T_uint64} {
		fr, err := GetFunctionByName(context.Background(), "random_bytes", []types.Type{tp.ToType()})
		require.NoError(t, err)
		ov, ok := GetFunctionByIdWithoutError(fr.GetEncodedOverloadID())
		require.True(t, ok)
		require.True(t, ov.CannotFold(), "random bytes must not be folded or hoisted out of pairwise evaluation")
	}
}
