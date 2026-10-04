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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimal256HighScaleMultiplicationPublicPath(t *testing.T) {
	stmt, err := runOneExprStmt(NewMockOptimizer(false), t,
		"select cast('1e-65' as decimal(65,65)) * cast('1e-65' as decimal(65,65))")
	require.NoError(t, err)
	expr := stmt.GetQuery().Nodes[1].ProjectList[0]
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
	require.Equal(t, int32(65), expr.Typ.Width)
	require.Equal(t, int32(65), expr.Typ.Scale)

	proc := testutil.NewProc(t)
	defer proc.Free()
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	require.NoError(t, err)
	require.False(t, result.IsNull(0))
	require.Equal(t, types.Decimal256{}, vector.GetFixedAtWithTypeCheck[types.Decimal256](result, 0))
}

// Scan vectors prevent constant folding from bypassing the batch arithmetic owner.
func TestDecimal256ScaleAlignmentPublicPath(t *testing.T) {
	for _, tc := range []struct {
		name, sql                    string
		scale, leftScale, rightScale int32
		want                         any
	}{
		{"intdiv_minimum_divisor", "select (cast('170141183460469231731687303715884105727' as decimal(65,0)) - cast(n_nationkey - 1 as decimal(65,0))) div cast('-170141183460469231731687303715884105728' as decimal(65,0)) from nation", 0, 0, 0, []int64{0, 0}},
		{"div_minimum_divisor", "select cast(n_nationkey as decimal(65,0)) / cast('-170141183460469231731687303715884105728' as decimal(65,0)) from nation", 4, 0, 0, []types.Decimal256{{}, {}}},

		{"mod_minimum128", "select (cast('-170141183460469231731687303715884105728' as decimal(65,0)) + cast(n_nationkey - 1 as decimal(65,0))) % cast('18446744073709551616' as decimal(65,0)) from nation", 0, 0, 0, []types.Decimal256{{}, {B0_63: 1, B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}}},
		{"mod_minimum128_correction", "select (cast('-170141183460469231731687303715884105728' as decimal(65,0)) + cast(n_nationkey - 1 as decimal(65,0))) % cast('85070591730234615865843651857942052865' as decimal(65,0)) from nation", 0, 0, 0, []types.Decimal256{{B0_63: 1, B64_127: 0xc000000000000000, B128_191: ^uint64(0), B192_255: ^uint64(0)}, {B0_63: 2, B64_127: 0xc000000000000000, B128_191: ^uint64(0), B192_255: ^uint64(0)}}},
		{"mod_negative_power_divisor", "select cast(n_nationkey as decimal(65,0)) % cast('-18446744073709551616' as decimal(65,0)) from nation", 0, 0, 0, []types.Decimal256{{B0_63: 1}, {B0_63: 2}}},

		{"add_38_left", "select cast(n_nationkey as decimal(20,0)) + cast('3e-38' as decimal(65,38)) from nation", 38, 0, 38, []types.Decimal256{{B0_63: 0x98a224000000003, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0x1314448000000003, B64_127: 0x96769950b50d88f4}}},
		{"add_38_right", "select cast('3e-38' as decimal(65,38)) + cast(n_nationkey as decimal(20,0)) from nation", 38, 38, 0, []types.Decimal256{{B0_63: 0x98a224000000003, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0x1314448000000003, B64_127: 0x96769950b50d88f4}}},
		{"sub_38_left", "select cast(n_nationkey as decimal(20,0)) - cast('3e-38' as decimal(65,38)) from nation", 38, 0, 38, []types.Decimal256{{B0_63: 0x98a223ffffffffd, B64_127: 0x4b3b4ca85a86c47a}, {B0_63: 0x1314447ffffffffd, B64_127: 0x96769950b50d88f4}}},
		{"sub_38_right", "select cast('3e-38' as decimal(65,38)) - cast(n_nationkey as decimal(20,0)) from nation", 38, 38, 0, []types.Decimal256{{B0_63: 0xf675ddc000000003, B64_127: 0xb4c4b357a5793b85, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}, {B0_63: 0xecebbb8000000003, B64_127: 0x698966af4af2770b, B128_191: 0xffffffffffffffff, B192_255: 0xffffffffffffffff}}},
		{"mod_38_left", "select cast(n_nationkey as decimal(20,0)) % cast('3e-38' as decimal(65,38)) from nation", 38, 0, 38, []types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}}},
		{"mod_38_right", "select cast('3e-38' as decimal(65,38)) % cast(n_nationkey as decimal(20,0)) from nation", 38, 38, 0, []types.Decimal256{{B0_63: 0x3}, {B0_63: 0x3}}},
		{"add_39_left", "select cast(n_nationkey as decimal(20,0)) + cast('3e-39' as decimal(65,39)) from nation", 39, 0, 39, []types.Decimal256{{B0_63: 0x5f65568000000003, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0xbecaad0000000003, B64_127: 0xe0a1fd2712875988, B128_191: 0x5}}},
		{"add_39_right", "select cast('3e-39' as decimal(65,39)) + cast(n_nationkey as decimal(20,0)) from nation", 39, 39, 0, []types.Decimal256{{B0_63: 0x5f65568000000003, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0xbecaad0000000003, B64_127: 0xe0a1fd2712875988, B128_191: 0x5}}},
		{"sub_39_left", "select cast(n_nationkey as decimal(20,0)) - cast('3e-39' as decimal(65,39)) from nation", 39, 0, 39, []types.Decimal256{{B0_63: 0x5f65567ffffffffd, B64_127: 0xf050fe938943acc4, B128_191: 0x2}, {B0_63: 0xbecaacfffffffffd, B64_127: 0xe0a1fd2712875988, B128_191: 0x5}}},
		{"sub_39_right", "select cast('3e-39' as decimal(65,39)) - cast(n_nationkey as decimal(20,0)) from nation", 39, 39, 0, []types.Decimal256{{B0_63: 0xa09aa98000000003, B64_127: 0xfaf016c76bc533b, B128_191: 0xfffffffffffffffd, B192_255: 0xffffffffffffffff}, {B0_63: 0x4135530000000003, B64_127: 0x1f5e02d8ed78a677, B128_191: 0xfffffffffffffffa, B192_255: 0xffffffffffffffff}}},
		{"mod_39_left", "select cast(n_nationkey as decimal(20,0)) % cast('3e-39' as decimal(65,39)) from nation", 39, 0, 39, []types.Decimal256{{B0_63: 0x1}, {B0_63: 0x2}}},
		{"mod_39_right", "select cast('3e-39' as decimal(65,39)) % cast(n_nationkey as decimal(20,0)) from nation", 39, 39, 0, []types.Decimal256{{B0_63: 0x3}, {B0_63: 0x3}}},
		{"mod_high_bits", "select cast((cast(n_nationkey as decimal(20,0)) * cast('2000000000000000000' as decimal(20,0))) as decimal(65,0)) % cast('85070591730234615865843651857942052864e-58' as decimal(65,58)) from nation", 58, 0, 58, []types.Decimal256{{B64_127: 0x2eeb4be2e32a2000}, {B64_127: 0x1dd697c5c6544000}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := runOneExprStmt(NewMockOptimizer(false), t, tc.sql)
			require.NoError(t, err)
			var expr *pb.Expr
			for _, node := range stmt.GetQuery().Nodes {
				if node.NodeType == pb.Node_PROJECT {
					require.Len(t, node.ProjectList, 1)
					expr = node.ProjectList[0]
				}
			}
			require.NotNil(t, expr)
			wantType := types.T_decimal256
			if _, integer := tc.want.([]int64); integer {
				wantType = types.T_int64
			}
			require.Equal(t, int32(wantType), expr.Typ.Id)
			require.Equal(t, tc.scale, expr.Typ.Scale)
			args := expr.GetF().Args
			require.Len(t, args, 2)
			require.Equal(t, tc.leftScale, args[0].Typ.Scale)
			require.Equal(t, tc.rightScale, args[1].Typ.Scale)
			proc := testutil.NewProc(t)
			t.Cleanup(proc.Free)
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int32{1, 2, 0}, []bool{false, false, true}, proc.Mp()))
			input.SetRowCount(3)
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			if executor != nil {
				t.Cleanup(executor.Free)
			}
			require.NoError(t, err)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, 3, result.Length())
			require.Equal(t, wantType, result.GetType().Oid)
			require.Equal(t, expr.Typ.Width, result.GetType().Width)
			require.Equal(t, tc.scale, result.GetType().Scale)
			switch want := tc.want.(type) {
			case []types.Decimal256:
				require.Len(t, want, 2)
				for i, value := range want {
					require.False(t, result.IsNull(uint64(i)))
					require.Equal(t, value, vector.GetFixedAtWithTypeCheck[types.Decimal256](result, i))
				}
			case []int64:
				require.Len(t, want, 2)
				for i, value := range want {
					require.False(t, result.IsNull(uint64(i)))
					require.Equal(t, value, vector.GetFixedAtWithTypeCheck[int64](result, i))
				}
			default:
				t.Fatalf("unsupported expected result type %T", tc.want)
			}

			require.True(t, result.IsNull(2))
		})
	}
}
