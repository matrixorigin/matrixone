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

package readutil

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestPrefixInRejectsIncompatibleVector(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	ints := vector.NewVec(types.T_int32.ToType())
	t.Cleanup(func() { ints.Free(mp) })
	require.NoError(t, vector.AppendFixed(ints, int32(7), false, mp))
	fixed, err := ints.MarshalBinary()
	require.NoError(t, err)
	strings := vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { strings.Free(mp) })
	require.NoError(t, vector.AppendBytes(strings, []byte("abc"), false, mp))
	valid, err := strings.MarshalBinary()
	require.NoError(t, err)
	values, _ := vector.MustVarlenaRawData(strings)
	values[0].SetOffsetLen(1<<31, 4096)
	badArea, err := strings.MarshalBinary()
	require.NoError(t, err)
	empty := vector.NewVec(types.T_varchar.ToType())
	emptyData, err := empty.MarshalBinary()
	require.NoError(t, err)
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "k", ColPos: 0}}}
	table := &plan.TableDef{Name2ColIndex: map[string]int32{"k": 0}, Cols: []*plan.ColDef{{Name: "k", Typ: plan.Type{Id: int32(types.T_varchar)}}}}
	for _, tc := range []struct {
		name string
		data []byte
		card int32
		want bool
	}{
		{"fixed-width", fixed, 1, false},
		{"missing", nil, 0, false},
		{"truncated", valid[:len(valid)/2], 1, false},
		{"invalid-varlena-area", badArea, 1, false},
		{"valid-varlena", valid, 1, true},
		{"empty-varlena", emptyData, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := plan2.MakeInExpr(context.Background(), col, tc.card, tc.data, true)
			require.NotPanics(t, func() {
				fast, load, obj, block, seek, can, _ := CompileFilterExpr(expr, table, nil)
				require.Equal(t, tc.want, can)
				if !tc.want {
					require.Nil(t, fast)
					require.Nil(t, load)
					require.Nil(t, obj)
					require.Nil(t, block)
					require.Nil(t, seek)
				} else {
					require.NotNil(t, block)
				}
			}, "unsupported filter representations must decline the optimization without unsafe vector interpretation")
		})
	}
}
