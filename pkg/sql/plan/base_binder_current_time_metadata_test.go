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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestBindCurrentTimeFamilyPreservesLiteralFSP(t *testing.T) {
	for _, tc := range []struct {
		name       string
		defaultTyp types.Type
		fsp        int64
	}{
		{"now", types.T_timestamp.ToTypeWithScale(6), 0},
		{"current_timestamp", types.T_timestamp.ToTypeWithScale(6), 3},
		{"localtime", types.T_timestamp.ToTypeWithScale(6), 6},
		{"localtimestamp", types.T_timestamp.ToTypeWithScale(6), 5},
		{"sysdate", types.T_timestamp.ToTypeWithScale(6), 2},
		{"current_time", types.T_time.ToType(), 4},
		{"curtime", types.T_time.ToType(), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), tc.name, []*Expr{
				makePlan2Int64ConstExprWithType(tc.fsp),
			})
			require.NoError(t, err)
			require.Equal(t, tc.fsp, int64(expr.Typ.Scale))
			require.Equal(t, tc.defaultTyp.Oid, types.T(expr.Typ.Id))
		})
	}
}

func TestBindCurrentTimeFamilyKeepsDefaultForRuntimeFSP(t *testing.T) {
	for _, tc := range []struct {
		name  string
		want  int32
		input types.Type
	}{
		{"now", 6, types.T_int64.ToType()},
		{"curtime", 0, types.T_int64.ToType()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arg := &Expr{Typ: makePlan2Type(&tc.input)}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), tc.name, []*Expr{arg})
			require.NoError(t, err)
			require.Equal(t, tc.want, expr.Typ.Scale)
		})
	}
}

func TestBindCurrentTimeFamilyDefaultsToZeroFSP(t *testing.T) {
	for _, name := range []string{
		"now", "current_timestamp", "localtime", "localtimestamp", "sysdate",
		"current_time", "curtime",
	} {
		t.Run(name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, nil)
			require.NoError(t, err)
			require.Equal(t, int32(0), expr.Typ.Scale)
		})
	}
}
