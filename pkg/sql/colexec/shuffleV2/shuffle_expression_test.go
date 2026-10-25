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

package shuffleV2

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestShuffleExpressionRangeRouting(t *testing.T) {
	for _, tc := range []struct {
		name     string
		oid      types.T
		values   []any
		signed   []int64
		unsigned []uint64
	}{
		{"signed_min_max", types.T_int64, []any{int64(0), int64(2), int64(10), int64(11), int64(20), int64(21), int64(30), int64(41)}, nil, nil},
		{"signed_nonuniform", types.T_int64, []any{int64(-11), int64(-10), int64(-1), int64(0), int64(1), int64(20), int64(21), int64(100)}, []int64{-10, 0, 20}, nil},
		{"unsigned_min_max", types.T_uint64, []any{uint64(0), uint64(2), uint64(10), uint64(11), uint64(20), uint64(21), uint64(1) << 63, ^uint64(0)}, nil, nil},
		{"unsigned_nonuniform", types.T_uint64, []any{uint64(0), uint64(2), uint64(3), uint64(8), uint64(9), uint64(32), uint64(33), uint64(100)}, nil, []uint64{2, 8, 32}},
		{"varchar_inline", types.T_varchar, []any{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("y"), []byte("z"), []byte("zz")}, nil, []uint64{uint64('b') << 56, uint64('d') << 56, uint64('y') << 56}},
		{"varchar_area", types.T_varchar, []any{[]byte("a0000000" + strings.Repeat("x", 40)), []byte("b0000000" + strings.Repeat("x", 40)), []byte("c0000000" + strings.Repeat("x", 40)), []byte("d0000000" + strings.Repeat("x", 40)), []byte("e0000000" + strings.Repeat("x", 40)), []byte("y0000000" + strings.Repeat("x", 40)), []byte("z0000000" + strings.Repeat("x", 40)), []byte("zz000000" + strings.Repeat("x", 40))}, nil, []uint64{binary.BigEndian.Uint64([]byte("b0000000")), binary.BigEndian.Uint64([]byte("d0000000")), binary.BigEndian.Uint64([]byte("y0000000"))}},
	} {
		for _, withNull := range []bool{false, true} {
			for _, route := range []string{"column", "expression", "serial_extract"} {
				t.Run(fmt.Sprintf("%s/null=%t/%s", tc.name, withNull, route), func(t *testing.T) {
					proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
					defer proc.Free()
					defer func() { require.Zero(t, proc.Mp().CurrNB()) }()
					bat := batch.NewWithSize(3)
					bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
					bat.Vecs[1] = vector.NewVec(tc.oid.ToType())
					bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
					defer bat.Clean(proc.Mp())
					packer := types.NewPacker()
					defer packer.Close()
					for i, value := range tc.values {
						isNull := withNull && i == 7
						require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), false, proc.Mp()))
						require.NoError(t, vector.AppendAny(bat.Vecs[1], value, isNull, proc.Mp()))
						packer.Reset()
						packer.EncodeStringType([]byte("6608010000"))
						if isNull {
							packer.EncodeNull()
						} else {
							switch v := value.(type) {
							case int64:
								packer.EncodeInt64(v)
							case uint64:
								packer.EncodeUint64(v)
							case []byte:
								packer.EncodeStringType(v)
							}
						}
						require.NoError(t, vector.AppendBytes(bat.Vecs[2], packer.GetBuf(), false, proc.Mp()))
					}
					bat.SetRowCount(len(tc.values))
					arg := &ShuffleV2{BucketNum: 4, CurrentShuffleIdx: 3, ShuffleColIdx: 1,
						ShuffleType: int32(plan.ShuffleType_Range), ShuffleColMin: 0, ShuffleColMax: 40,
						ShuffleRangeInt64: tc.signed, ShuffleRangeUint64: tc.unsigned}
					if route == "expression" {
						arg.ShuffleExpr = plan2.GetColExpr(plan.Type{Id: int32(tc.oid)}, 0, 1)
					} else if route == "serial_extract" {
						var err error
						arg.ShuffleExpr, err = plan2.MakeSerialExtractExpr(proc.Ctx,
							plan2.GetColExpr(plan.Type{Id: int32(types.T_varchar)}, 0, 2),
							plan.Type{Id: int32(tc.oid)}, 1)
						require.NoError(t, err)
					}
					arg.SetShufflePool(NewShufflePool(4, 1))
					defer arg.Free(proc, false, nil)
					defer arg.Reset(proc, false, nil)
					require.NoError(t, arg.Prepare(proc))
					inputVectors := append([]*vector.Vector(nil), bat.Vecs...)
					var out *batch.Batch
					var err error
					if route == "column" {
						out, err = rangeShuffle(arg, bat, proc)
					} else {
						out, err = arg.evalAndShuffle(bat, proc)
					}
					require.NoError(t, err)
					for i := range inputVectors {
						require.Same(t, inputVectors[i], bat.Vecs[i])
					}
					expected := []int32{0, 0, 1, 1, 2, 2, 3, 3}
					if withNull {
						expected[7] = 0
					}
					assertShuffleRows(t, arg, proc, out, expected)
				})
			}
		}
	}
}

// 验证每一行恰好进入一个正确桶，而非只断言“不 panic”。
func assertShuffleRows(t *testing.T, arg *ShuffleV2, proc *process.Process, direct *batch.Batch, expected []int32) {
	t.Helper()
	actual := make([]int32, len(expected))
	for i := range actual {
		actual[i] = -1
	}
	collect := func(b *batch.Batch, bucket int32) {
		for _, id := range vector.MustFixedColNoTypeCheck[int64](b.Vecs[0]) {
			require.GreaterOrEqual(t, id, int64(0))
			require.Less(t, id, int64(len(actual)))
			require.Equal(t, int32(-1), actual[id], "重复输出行 %d", id)
			actual[id] = bucket
		}
	}
	if direct != nil {
		require.Equal(t, arg.CurrentShuffleIdx, direct.ShuffleIDX, "不能把其他桶的 batch 直接送给当前 worker")
		collect(direct, direct.ShuffleIDX)
	}
	for bucket := int32(0); bucket < arg.BucketNum; bucket++ {
		b := arg.GetShufflePool().getLastBatch(bucket)
		if b != nil {
			func() {
				defer b.Clean(proc.Mp())
				require.Equal(t, bucket, b.ShuffleIDX)
				collect(b, bucket)
			}()
		}
	}
	require.Equal(t, expected, actual)
}

func TestShuffleExpressionConstantRouting(t *testing.T) {
	nullExpr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	for _, tc := range []struct {
		name string
		expr *plan.Expr
		kind plan.ShuffleType
		want int32
	}{
		{"range_value", plan2.MakePlan2Int64ConstExprWithType(25), plan.ShuffleType_Range, 2},
		{"range_null", nullExpr, plan.ShuffleType_Range, 0},
		{"hash_value", plan2.MakePlan2Int64ConstExprWithType(25), plan.ShuffleType_Hash, int32(plan2.SimpleInt64HashToRange(25, 4))},
		{"hash_null", nullExpr, plan.ShuffleType_Hash, 0},
	} {
		for _, current := range []int32{tc.want, (tc.want + 1) % 4} {
			for _, ordinary := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/current=%d/ordinary=%t", tc.name, current, ordinary), func(t *testing.T) {
					proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
					defer proc.Free()
					defer func() { require.Zero(t, proc.Mp().CurrNB()) }()
					bat := batch.NewWithSize(2)
					bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
					bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
					defer bat.Clean(proc.Mp())
					require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{0, 1}, nil, proc.Mp()))
					for range 2 {
						require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(25), tc.expr.GetLit().Isnull, proc.Mp()))
					}
					bat.SetRowCount(2)
					expr := tc.expr
					if ordinary {
						expr = plan2.GetColExpr(plan.Type{Id: int32(types.T_int64)}, 0, 1)
					}
					arg := &ShuffleV2{BucketNum: 4, CurrentShuffleIdx: current, ShuffleExpr: expr,
						ShuffleType: int32(tc.kind), ShuffleColMin: 0, ShuffleColMax: 40}
					arg.SetShufflePool(NewShufflePool(4, 1))
					defer arg.Free(proc, false, nil)
					defer arg.Reset(proc, false, nil)
					require.NoError(t, arg.Prepare(proc))
					out, err := arg.evalAndShuffle(bat, proc)
					require.NoError(t, err)
					if current == tc.want {
						require.Same(t, bat, out)
					} else {
						require.Nil(t, out)
					}
					assertShuffleRows(t, arg, proc, out, []int32{tc.want, tc.want})
				})
			}
		}
	}
}
