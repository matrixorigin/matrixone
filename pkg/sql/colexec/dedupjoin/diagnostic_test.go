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

package dedupjoin

import (
	"fmt"
	"math"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestDedupLogicalDiagnostic(t *testing.T) {
	for _, mode := range []txn.TxnMode{txn.TxnMode_Pessimistic, txn.TxnMode_Optimistic} {
		for _, threshold := range []int64{0, 2} {
			for _, shape := range []string{"integer", "signed_zero", "unicode", "second_chunk", "render_error"} {
				t.Run(fmt.Sprintf("%s/threshold=%d/%s", mode, threshold, shape), func(t *testing.T) {
					ctrl := gomock.NewController(t)
					defer ctrl.Finish()
					proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
					proc.Base.Lim.SpillSize = 64 << 20
					op := mock_frontend.NewMockTxnOperator(ctrl)
					op.EXPECT().Txn().Return(txn.TxnMeta{Mode: mode}).AnyTimes()
					proc.Base.TxnOperator = op
					proc.SetMessageBoard(message.NewMessageBoard())
					var buildBat, probeBat *batch.Batch
					var join *DedupJoin
					var build *hashbuild.HashBuild
					var allocation *mpool.AllocationAccount
					defer func() {
						if join != nil {
							join.Free(proc, false, nil)
						}
						if build != nil {
							build.Free(proc, false, nil)
						}
						if buildBat != nil {
							buildBat.Clean(proc.Mp())
						}
						if probeBat != nil {
							probeBat.Clean(proc.Mp())
						}
						proc.Free()
						require.Zero(t, proc.Mp().CurrNB())
						if allocation != nil {
							require.Zero(t, allocation.Snapshot().Used)
						}
					}()
					keyType := types.T_int32.ToType()
					if shape == "signed_zero" {
						keyType = types.T_float64.ToType()
					} else if shape == "unicode" || shape == "render_error" {
						keyType = types.T_varbinary.ToType()
					}
					buildBat = batch.NewWithSize(1)
					buildBat.Vecs[0] = vector.NewVec(keyType)
					probeBat = batch.NewWithSize(1)
					probeBat.Vecs[0] = vector.NewVec(keyType)
					rows, want := 3, "7"
					switch shape {
					case "signed_zero":
						require.NoError(t, vector.AppendFixed(buildBat.Vecs[0], float64(0), true, proc.Mp()))
						require.NoError(t, vector.AppendFixed(buildBat.Vecs[0], float64(13), false, proc.Mp()))
						require.NoError(t, vector.AppendFixed(buildBat.Vecs[0], math.Copysign(0, -1), false, proc.Mp()))
						require.NoError(t, vector.AppendFixed(probeBat.Vecs[0], float64(0), false, proc.Mp()))
						want = "-0"
					case "unicode", "render_error":
						buildBat.Vecs = append(buildBat.Vecs, vector.NewVec(types.T_varchar.ToType()))
						for i, value := range []string{"", "Other", "alpha"} {
							key, err := collation.UTF8MB4UnicodeCI.Key(nil, []byte(value))
							require.NoError(t, err)
							require.NoError(t, vector.AppendBytes(buildBat.Vecs[0], key, i == 0, proc.Mp()))
							require.NoError(t, vector.AppendBytes(buildBat.Vecs[1], []byte(value), i == 0, proc.Mp()))
						}
						key, err := collation.UTF8MB4UnicodeCI.Key(nil, []byte("Alpha"))
						require.NoError(t, err)
						require.NoError(t, vector.AppendBytes(probeBat.Vecs[0], key, false, proc.Mp()))
						want = "alpha"
					default:
						if shape == "second_chunk" {
							rows, want = hashmap.UnitLimit+1, fmt.Sprint(hashmap.UnitLimit)
						}
						for i := 0; i < rows; i++ {
							value := int32(i)
							if shape == "integer" && i == rows-1 {
								value = 7
							}
							require.NoError(t, vector.AppendFixed(buildBat.Vecs[0], value, i == 0, proc.Mp()))
						}
						value := int32(7)
						if shape == "second_chunk" {
							value = hashmap.UnitLimit
						}
						require.NoError(t, vector.AppendFixed(probeBat.Vecs[0], value, false, proc.Mp()))
					}
					buildBat.SetRowCount(rows)
					probeBat.SetRowCount(1)
					condition := newExpr(0, keyType)
					condition.GetCol().RelPos = 1
					diagnostic := newExpr(0, keyType)
					if shape == "unicode" || shape == "render_error" {
						diagnostic = newExpr(1, types.T_varchar.ToType())
					}
					if shape == "render_error" {
						diagnostic.GetCol().ColPos = int32(len(buildBat.Vecs))
					}
					diagnostic.GetCol().RelPos = 1
					rightTypes := make([]types.Type, len(buildBat.Vecs))
					for i, vec := range buildBat.Vecs {
						rightTypes[i] = *vec.GetType()
					}
					tag++
					join = &DedupJoin{
						LeftTypes: []types.Type{keyType}, RightTypes: rightTypes,
						Conditions:        [][]*plan.Expr{{newExpr(0, keyType)}, {condition}},
						Result:            []colexec.ResultPos{colexec.NewResultPos(1, 0)},
						OnDuplicateAction: plan.Node_FAIL, DedupColName: "uk_named",
						DedupColTypes: []plan.Type{diagnostic.Typ}, DedupKeyExprs: []*plan.Expr{diagnostic},
						JoinMapTag: tag, NumCPU: 1, IsMerger: true, IsShuffle: threshold > 0,
						DelColIdx: -1, SpillThreshold: threshold,
					}
					build = &hashbuild.HashBuild{
						NeedHashMap: true, NeedBatches: true, Conditions: join.Conditions[1],
						IsDedup: true, OnDuplicateAction: plan.Node_FAIL, DelColIdx: -1,
						DedupColName: join.DedupColName, DedupColTypes: join.DedupColTypes, DedupKeyExprs: join.DedupKeyExprs,
						JoinMapTag: tag, JoinMapRefCnt: 1, IsShuffle: threshold > 0, SpillThreshold: threshold,
					}
					if threshold > 0 {
						build.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tag + 9000}
					}
					join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat}))
					build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat}))
					allocation = installTestAllocation(t, join, build)
					require.NoError(t, build.Prepare(proc))
					require.NoError(t, join.Prepare(proc))
					_, err := vm.Exec(build, proc)
					require.NoError(t, err)
					for {
						res, err := vm.Exec(join, proc)
						if err != nil {
							if shape == "render_error" {
								require.ErrorContains(t, err, "cannot format logical duplicate key")
							} else {
								require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry), "%v", err)
								require.Equal(t, fmt.Sprintf("Duplicate entry '%s' for key 'uk_named'", want), err.Error())
							}
							if threshold > 0 {
								require.NotNil(t, join.ctr.spillEngine, "exercise recovered spill buckets")
							}
							break
						}
						require.NotEqual(t, vm.ExecStop, res.Status, "expected duplicate diagnostic")
					}
				})
			}
		}
	}
}
