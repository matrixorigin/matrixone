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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestShuffleExpressionArguments(t *testing.T) {
	type arguments struct {
		column, kind, buckets int32
		min, max              int64
		expr                  *plan.Expr
		signed                []int64
		unsigned              []uint64
	}
	constructors := []struct {
		name string
		make func(*plan.Node) arguments
	}{
		{"group_v1", func(n *plan.Node) arguments {
			a := constructShuffleArgForGroup(make([]*Scope, 4), n)
			defer a.Release()
			return arguments{a.ShuffleColIdx, a.ShuffleType, a.BucketNum, a.ShuffleColMin, a.ShuffleColMax, a.ShuffleExpr, a.ShuffleRangeInt64, a.ShuffleRangeUint64}
		}},
		{"group_v2", func(n *plan.Node) arguments {
			a := constructShuffleArgForGroupV2(n, 4)
			defer a.Release()
			return arguments{a.ShuffleColIdx, a.ShuffleType, a.BucketNum, a.ShuffleColMin, a.ShuffleColMax, a.ShuffleExpr, a.ShuffleRangeInt64, a.ShuffleRangeUint64}
		}},
		{"join_left_v1", func(n *plan.Node) arguments {
			a := constructShuffleOperatorForJoin(4, n, true)
			defer a.Release()
			return arguments{a.ShuffleColIdx, a.ShuffleType, a.BucketNum, a.ShuffleColMin, a.ShuffleColMax, a.ShuffleExpr, a.ShuffleRangeInt64, a.ShuffleRangeUint64}
		}},
		{"join_right_v1", func(n *plan.Node) arguments {
			a := constructShuffleOperatorForJoin(4, n, false)
			defer a.Release()
			return arguments{a.ShuffleColIdx, a.ShuffleType, a.BucketNum, a.ShuffleColMin, a.ShuffleColMax, a.ShuffleExpr, a.ShuffleRangeInt64, a.ShuffleRangeUint64}
		}},
		{"join_left_v2", func(n *plan.Node) arguments {
			a := constructShuffleOperatorForJoinV2(4, n, true)
			defer a.Release()
			return arguments{a.ShuffleColIdx, a.ShuffleType, a.BucketNum, a.ShuffleColMin, a.ShuffleColMax, a.ShuffleExpr, a.ShuffleRangeInt64, a.ShuffleRangeUint64}
		}},
		{"join_right_v2", func(n *plan.Node) arguments {
			a := constructShuffleOperatorForJoinV2(4, n, false)
			defer a.Release()
			return arguments{a.ShuffleColIdx, a.ShuffleType, a.BucketNum, a.ShuffleColMin, a.ShuffleColMax, a.ShuffleExpr, a.ShuffleRangeInt64, a.ShuffleRangeUint64}
		}},
	}
	for _, constructor := range constructors {
		t.Run(constructor.name, func(t *testing.T) {
			for _, oid := range []types.T{types.T_int64, types.T_uint64, types.T_varchar} {
				t.Run(oid.String(), func(t *testing.T) {
					typ := plan.Type{Id: int32(oid), Width: 255}
					column := plan2.GetColExpr(typ, 0, 3)
					encoded := plan2.GetColExpr(plan.Type{Id: int32(types.T_varchar)}, 0, 2)
					expr, err := plan2.MakeSerialExtractExpr(context.Background(), encoded, typ, 1)
					require.NoError(t, err)
					for _, boundary := range []struct {
						name     string
						ranges   []float64
						nulls    int64
						expected []int64
					}{
						{"min_max", nil, 0, nil},
						{"nonuniform", []float64{1, 2, 4, 8, 16, 32, 64, 128}, 0, []int64{2, 8, 32}},
						{"null_bucket", []float64{1, 2, 4, 8, 16, 32, 64, 128}, 30, []int64{1, 4, 16}},
					} {
						t.Run(boundary.name, func(t *testing.T) {
							for _, key := range []struct {
								name string
								expr *plan.Expr
							}{{"column", column}, {"serial_extract", expr}} {
								t.Run(key.name, func(t *testing.T) {
									node := &plan.Node{
										GroupBy: []*plan.Expr{key.expr},
										OnList: []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{
											Args: []*plan.Expr{key.expr, key.expr},
										}}}},
										Stats: &plan.Stats{TableCnt: 100, HashmapStats: &plan.HashMapStats{
											ShuffleType: plan.ShuffleType_Range, ShuffleColIdx: 0,
											ShuffleColMin: 0, ShuffleColMax: 256,
											Ranges: boundary.ranges, Nullcnt: boundary.nulls,
										}},
									}
									a := constructor.make(node)
									require.Equal(t, int32(plan.ShuffleType_Range), a.kind)
									require.Equal(t, int32(4), a.buckets)
									require.Equal(t, int64(0), a.min)
									require.Equal(t, int64(256), a.max)
									if key.name == "column" {
										require.Equal(t, int32(3), a.column)
										require.Nil(t, a.expr)
									} else {
										require.Equal(t, key.expr, a.expr)
										require.NotSame(t, key.expr, a.expr)
									}
									if oid == types.T_int64 {
										require.Equal(t, boundary.expected, a.signed)
										require.Nil(t, a.unsigned)
									} else {
										var expected []uint64
										for _, v := range boundary.expected {
											expected = append(expected, uint64(v))
										}
										require.Equal(t, expected, a.unsigned)
										require.Nil(t, a.signed)
									}
								})
							}
						})
					}
				})
			}
		})
	}
}
