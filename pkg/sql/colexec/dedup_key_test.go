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

package colexec

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestFormatDedupKeyDecodesFloatIdentity(t *testing.T) {
	pool := mpool.MustNewZero()
	packer := types.NewPacker()
	defer packer.Close()

	packer.EncodeFloat32(1)
	float32Key := append([]byte(nil), packer.Bytes()...)
	packer.Reset()
	packer.EncodeFloat64(math.Copysign(0, -1))
	float64Key := append([]byte(nil), packer.Bytes()...)

	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(pool)
	require.NoError(t, vector.AppendBytes(vec, float32Key, false, pool))
	require.NoError(t, vector.AppendBytes(vec, float64Key, false, pool))

	got, err := FormatDedupKey(vec, 0, []plan.Type{{Id: int32(types.T_float32)}})
	require.NoError(t, err)
	require.Equal(t, "1", got)

	got, err = FormatDedupKey(vec, 1, []plan.Type{{Id: int32(types.T_float64)}})
	require.NoError(t, err)
	require.Equal(t, "-0", got)
}

func TestFormatDedupKeyRejectsMalformedEncodedIdentity(t *testing.T) {
	pool := mpool.MustNewZero()
	_, err := FormatDedupKey(nil, 0, []plan.Type{{Id: int32(types.T_int64)}})
	require.Error(t, err)
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(pool)
	require.NoError(t, vector.AppendBytes(vec, []byte{0xff}, false, pool))

	_, err = FormatDedupKey(vec, 0, []plan.Type{{Id: int32(types.T_float64)}})
	require.Error(t, err)
	_, err = FormatDedupKey(vec, 1, []plan.Type{{Id: int32(types.T_float64)}})
	require.Error(t, err)

	// A truncated integer tuple used to panic inside StringifyTuple while
	// rendering an already-rejected IGNORE row. Diagnostic formatting must stay
	// best-effort and return an error instead.
	truncated := vector.NewVec(types.T_varchar.ToType())
	defer truncated.Free(pool)
	require.NoError(t, vector.AppendBytes(truncated, []byte{0x3b}, false, pool))
	_, err = FormatDedupKey(truncated, 0, []plan.Type{
		{Id: int32(types.T_int64)},
		{Id: int32(types.T_int64)},
	})
	require.Error(t, err)

	_, err = FormatDedupKey(truncated, 0, []plan.Type{
		{Id: int32(types.T_float64)},
		{Id: int32(types.T_int64)},
	})
	require.Error(t, err)
}

func TestFormatDedupKeyFromLogicalRow(t *testing.T) {
	proc := testutil.NewProcess(t)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	defer bat.Clean(proc.Mp())
	require.NoError(t, vector.AppendBytesList(bat.Vecs[0], [][]byte{[]byte("Alpha"), []byte("éclair"), []byte("a\x00\n\xffb")}, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[1], []int32{1, 7, 9}, nil, proc.Mp()))
	bat.SetRowCount(3)
	col := func(rel, pos int32, typ types.Type) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: pos}}}
	}
	integer := func(v int64) *plan.Expr {
		return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: v}}}}
	}
	before := proc.Mp().CurrNB()
	for _, rel := range []int32{0, 1} {
		text := col(rel, 0, types.T_varchar.ToType())
		prefix := bindTestFunction(t, proc, "substring", text, integer(1), integer(2))
		for _, tc := range []struct {
			name  string
			exprs []*plan.Expr
			row   int
			want  string
		}{
			{"source spelling", []*plan.Expr{text}, 0, "Alpha"},
			{"unicode prefix", []*plan.Expr{prefix}, 1, "éc"},
			{"composite", []*plan.Expr{text, col(rel, 1, types.T_int32.ToType())}, 1, "(éclair,7)"},
			{"escaped controls", []*plan.Expr{text}, 2, `a\x00\n\xffb`},
		} {
			t.Run(fmt.Sprintf("rel%d/%s", rel, tc.name), func(t *testing.T) {
				got, err := FormatDedupKeyFromRow(proc, tc.exprs, bat, tc.row)
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
				require.Equal(t, before, proc.Mp().CurrNB(), "diagnostic windows and executors must release borrowed/owned state")
			})
		}
		_, err := FormatDedupKeyFromRow(proc, []*plan.Expr{text}, bat, 3)
		require.Error(t, err)
	}
	bad := col(0, 7, types.T_varchar.ToType())
	_, err := FormatDedupKeyFromRow(proc, []*plan.Expr{bad}, bat, 0)
	require.Error(t, err)
	require.Equal(t, before, proc.Mp().CurrNB())
	require.Equal(t, "Alpha", bat.Vecs[0].RowToString(0), "diagnostic cleanup must preserve the input")
}
