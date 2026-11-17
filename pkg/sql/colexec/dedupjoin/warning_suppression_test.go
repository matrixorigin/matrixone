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

package dedupjoin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestAuxiliaryWarningGroupRows(t *testing.T) {
	for _, prefixNull := range []bool{false, true} {
		name := "trailing_NULL_compact_identity"
		if prefixNull {
			name = "prefix_NULL_explicit_selections"
		}
		t.Run(name, func(t *testing.T) {
			proc, ctrl := newCaptureTestProc(t)
			defer ctrl.Finish()
			session := &dedupJoinWarningSession{}
			proc.Session = session
			proc.SetStmtProfile(&process.StmtProfile{})
			proc.GetStmtProfile().SetStatementRuntimeProfile("Update", "DML", true)
			buildBat, probeBat := batch.NewWithSize(4), batch.NewWithSize(1)
			var join *DedupJoin
			var build *hashbuild.HashBuild
			var account *mpool.AllocationAccount
			defer func() {
				if join != nil {
					join.Free(proc, false, nil)
				}
				if build != nil {
					build.Free(proc, false, nil)
				}
				buildBat.Clean(proc.Mp())
				probeBat.Clean(proc.Mp())
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
				if account != nil {
					require.Zero(t, account.Snapshot().Used)
				}
			}()
			intType, boolType := types.T_int32.ToType(), types.T_bool.ToType()
			rightTypes := []types.Type{intType, intType, boolType, intType}
			for i, typ := range rightTypes {
				buildBat.Vecs[i] = vector.NewVec(typ)
			}
			probeBat.Vecs[0] = vector.NewVec(intType)
			appendRow := func(key, id int32, keyNull, auxiliary bool) {
				require.NoError(t, vector.AppendFixed(buildBat.Vecs[0], key, keyNull, proc.Mp()))
				require.NoError(t, vector.AppendFixed(buildBat.Vecs[1], int32(0), true, proc.Mp()))
				require.NoError(t, vector.AppendFixed(buildBat.Vecs[2], false, false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(buildBat.Vecs[3], id, auxiliary, proc.Mp()))
			}
			if prefixNull {
				appendRow(0, 0, true, true)
			}
			appendRow(10, 1, false, false) // Conflicts with an untargeted snapshot row.
			appendRow(20, 2, false, false) // The only accepted real candidate.
			for i := 0; i < colexec.DefaultBatchSize; i++ {
				appendRow(int32(1000+i), 0, false, true)
			}
			if !prefixNull {
				appendRow(0, 0, true, true)
			}
			buildBat.SetRowCount(buildBat.Vecs[0].Length())
			for _, key := range []int32{10, 1000 + colexec.DefaultBatchSize - 1} {
				require.NoError(t, vector.AppendFixed(probeBat.Vecs[0], key, false, proc.Mp()))
			}
			probeBat.SetRowCount(2)
			conditions := [][]*plan.Expr{{newExpr(0, intType)}, {newExpr(0, intType)}}
			const mapTag = 918273
			join = &DedupJoin{LeftTypes: []types.Type{intType}, RightTypes: rightTypes, Conditions: conditions,
				Result:            []colexec.ResultPos{colexec.NewResultPos(1, 3), colexec.NewResultPos(1, 0)},
				OnDuplicateAction: plan.Node_IGNORE, DelColIdx: 1, DedupDeleteMarkerColIdx: 2, AuxiliaryRowIdColIdx: 4,
				DedupColName: "uk_named", DedupColTypes: []plan.Type{conditions[0][0].Typ},
				DedupKeyExprs: []*plan.Expr{newExpr(0, intType)}, JoinMapTag: mapTag}
			build = &hashbuild.HashBuild{NeedHashMap: true, NeedBatches: true, Conditions: conditions[1],
				// The marked producer owns this requirement, including direct callers.
				NeedAllocateSels: false, IsDedup: true, OnDuplicateAction: plan.Node_IGNORE,
				DelColIdx: 1, DedupDeleteMarkerColIdx: 2, AuxiliaryRowIdColIdx: 4,
				DedupColName: join.DedupColName, DedupColTypes: join.DedupColTypes,
				JoinMapTag: mapTag, JoinMapRefCnt: 1}
			join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat}))
			build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat}))
			account = installTestAllocation(t, join, build)
			require.NoError(t, build.Prepare(proc))
			require.NoError(t, join.Prepare(proc))
			_, err := vm.Exec(build, proc)
			require.NoError(t, err)
			result, err := vm.Exec(join, proc)
			require.NoError(t, err)
			require.NotNil(t, join.ctr.mp)
			require.Greater(t, len(join.ctr.batches), 1)
			require.EqualValues(t, colexec.DefaultBatchSize+2, join.ctr.mp.GetGroupCount())
			require.EqualValues(t, colexec.DefaultBatchSize+3, join.ctr.batchRowCount)
			group := uint64(colexec.DefaultBatchSize + 2)
			require.Equal(t, prefixNull, len(join.ctr.mp.GetSels(group)) > 0)
			suppressed, err := join.ctr.duplicateWarningSuppressed(join, proc, group)
			require.NoError(t, err)
			require.True(t, suppressed, "reservation is in the second retained batch")
			var actual [][]int32
			for {
				if result.Batch != nil {
					for row := 0; row < result.Batch.RowCount(); row++ {
						if !result.Batch.Vecs[0].IsNull(uint64(row)) {
							actual = append(actual, []int32{vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[0], row), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[1], row)})
						}
					}
				}
				if result.Status == vm.ExecStop {
					break
				}
				result, err = vm.Exec(join, proc)
				require.NoError(t, err)
			}
			require.Equal(t, [][]int32{{2, 20}}, actual)
			require.Equal(t, uint64(1), session.total, "real conflict counts; auxiliary snapshot match does not")
			require.Equal(t, []dedupJoinWarning{{code: 1062, msg: "Duplicate entry '10' for key 'uk_named'"}}, session.warnings)
		})
	}
}
