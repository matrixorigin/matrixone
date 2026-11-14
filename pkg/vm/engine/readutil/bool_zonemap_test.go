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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestCompileFilterExprLegacyBoolZoneMap(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewLocalFS(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	writer := ioutil.ConstructWriter(0, []uint16{0}, 0, false, false, fs)
	func() {
		input := batch.NewWithSize(1)
		defer input.Clean(mp)
		input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
		require.NoError(t, vector.AppendFixedList(input.Vecs[0], []bool{false, true}, nil, mp))
		input.SetRowCount(2)
		_, err := writer.WriteBatch(input)
		require.NoError(t, err)
	}()
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	location := stats.ObjectLocation()
	original, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	require.NoError(t, err)
	for _, sorted := range []bool{false, true} {
		for _, stored := range []bool{false, true} {
			for _, bound := range []bool{false, true} {
				t.Run(fmt.Sprintf("sorted=%v/stored=%v/bound=%v", sorted, stored, bound), func(t *testing.T) {
					meta := append(objectio.ObjectMeta(nil), original...)
					data := meta.MustDataMeta()
					zm := index.BuildZM(types.T_bool, types.EncodeBool(&stored))
					data.MustGetColumn(0).SetZoneMap(zm)
					data.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(zm)
					var objectStats objectio.ObjectStats
					require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&objectStats, zm))
					table := &plan.TableDef{Name2ColIndex: map[string]int32{"v": 0}, Cols: []*plan.ColDef{{Name: "v", Seqnum: 0, ClusterBy: sorted, Typ: plan.Type{Id: int32(types.T_bool)}}}}
					expr := MakeFunctionExprForTest("=", []*plan.Expr{MakeColExprForTest(0, types.T_bool, "v"), {Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeBool(&bound)}}}})
					fast, _, object, block, seek, can, _ := CompileFilterExpr(expr, table, nil)
					require.True(t, can)
					want := !stored || bound
					keep, err := object(meta, nil)
					require.NoError(t, err)
					require.Equal(t, sorted || want, keep)
					stop, keep, err := block(0, data.GetBlockMeta(0), nil)
					require.NoError(t, err)
					require.Equal(t, want, keep)
					if want {
						require.False(t, stop)
					}
					if fast != nil {
						keep, err := fast(&objectStats)
						require.NoError(t, err)
						require.Equal(t, want, keep)
					}
					if seek != nil && want {
						require.Zero(t, seek(data))
					}
				})
			}
		}
	}
}
