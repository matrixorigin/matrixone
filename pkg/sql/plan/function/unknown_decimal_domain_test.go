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

package function_test

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDecimalUnknownInputKeepsReleasedCarrier(t *testing.T) {
	for _, tc := range []struct {
		op    string
		width int32
	}{{"*", 20}, {"+", 38}, {"-", 38}} {
		for _, reverse := range []bool{false, true} {
			args := []types.Type{types.New(types.T_decimal128, tc.width, 0), types.T_any.ToType()}
			if reverse {
				args[0], args[1] = args[1], args[0]
			}
			t.Run(tc.op+args[0].Oid.String(), func(t *testing.T) {
				resolved, err := function.GetFunctionByName(context.Background(), tc.op, args)
				require.NoError(t, err)
				require.Equal(t, types.New(types.T_decimal128, 38, 0), resolved.GetReturnType())
				casts, _ := resolved.ShouldDoImplicitTypeCast()
				for _, cast := range casts {
					require.Equal(t, types.T_decimal128, cast.Oid)
				}
			})
		}
	}
	for _, reverse := range []bool{false, true} {
		args := []types.Type{types.New(types.T_decimal256, 20, 0), types.T_any.ToType()}
		if reverse {
			args[0], args[1] = args[1], args[0]
		}
		resolved, err := function.GetFunctionByName(context.Background(), "*", args)
		require.NoError(t, err)
		require.Equal(t, types.New(types.T_decimal256, 40, 0), resolved.GetReturnType())
	}
}
