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

package external

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMediumIntCSVLoadBounds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		oid    types.T
		value  string
		valid  bool
		signed int32
		unsig  uint32
	}{
		{name: "signed lower boundary", oid: types.T_int32, value: "-8388608", valid: true, signed: -1 << 23},
		{name: "signed upper boundary", oid: types.T_int32, value: "8388607", valid: true, signed: (1 << 23) - 1},
		{name: "signed upper overflow", oid: types.T_int32, value: "8388608"},
		{name: "signed lower overflow", oid: types.T_int32, value: "-8388609"},
		{name: "unsigned lower boundary", oid: types.T_uint32, value: "0", valid: true, unsig: 0},
		{name: "unsigned upper boundary", oid: types.T_uint32, value: "16777215", valid: true, unsig: (1 << 24) - 1},
		{name: "unsigned upper overflow", oid: types.T_uint32, value: "16777216"},
		{name: "unsigned negative", oid: types.T_uint32, value: "-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			extern := &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV}}
			cols := []*plan.ColDef{{Name: "m", Typ: plan.Type{Id: int32(tc.oid), Width: 24}}}
			field := csvparser.Field{Val: tc.value}
			require.Equal(t, tc.valid, isLegalLine(extern, cols, []csvparser.Field{field}))

			proc := testutil.NewProc(t)
			mp := proc.GetMPool()
			bat := batch.New([]string{"m"})
			bat.Vecs[0] = vector.NewVec(types.New(tc.oid, 24, -1))
			t.Cleanup(func() { bat.Clean(mp) })
			param := &ExternalParam{
				ExParamConst: ExParamConst{Ctx: context.Background(), Extern: extern, Cols: cols},
			}
			attr := plan.ExternAttr{ColName: "m", ColIndex: 0, ColFieldIndex: 0}
			err := getColData(bat, []csvparser.Field{field}, 0, param, mp, attr, proc)
			if !tc.valid {
				require.Error(t, err)
				require.Zero(t, bat.Vecs[0].Length())
				return
			}
			require.NoError(t, err)
			if tc.oid == types.T_int32 {
				require.Equal(t, []int32{tc.signed}, vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0]))
			} else {
				require.Equal(t, []uint32{tc.unsig}, vector.MustFixedColWithTypeCheck[uint32](bat.Vecs[0]))
			}
		})
	}
}
