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

package hashjoin

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestHashJoinRetainedVarlenAcrossBatches(t *testing.T) {
	previousLimit := mpool.CapLimit
	mpool.CapLimit = 1 << 20
	t.Cleanup(func() { mpool.CapLimit = previousLimit })

	const (
		defaultRows  = 13
		rowsPerInput = 6
		valueBytes   = 92 << 10
		queryBudget  = 16 << 20
		inlineValue  = "inline"
	)
	for _, tc := range []struct {
		name             string
		joinType         plan.Node_JoinType
		probeKeys        []int64
		buildRows        int
		usePK            bool
		useResidual      bool
		unmatchedOutlier bool
		wantBatchRows    []int
	}{
		{
			name: "left-duplicate-continuation", joinType: plan.Node_LEFT, probeKeys: []int64{1, 2},
			wantBatchRows: []int{11, 3},
		},
		{
			name: "right-unique-gather-and-finalize", joinType: plan.Node_RIGHT,
			probeKeys: []int64{11, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12}, buildRows: 26, usePK: true,
			wantBatchRows: []int{12, 1, 12, 1},
		},
		{
			name: "left-residual-continuation", joinType: plan.Node_LEFT, probeKeys: []int64{1, 2, 3, 4},
			useResidual: true, wantBatchRows: []int{11, 5},
		},
		{
			name: "left-unmatched-build-outlier", joinType: plan.Node_LEFT, probeKeys: []int64{1, 2},
			unmatchedOutlier: true, wantBatchRows: []int{13},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			proc.SetMessageBoard(message.NewMessageBoard())
			ledger := process.MustNewExecutionResourceBudget(queryBudget, queryBudget)
			generation, err := ledger.OpenGeneration(1)
			require.NoError(t, err)
			t.Cleanup(generation.Close)
			require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
			t.Cleanup(func() {
				require.Zero(t, generation.Used())
				require.Zero(t, mp.CurrNB())
			})
			t.Cleanup(func() { proc.GetMessageBoard().Reset() })

			keyType, textType := types.T_int64.ToType(), types.T_text.ToType()
			key := newExpr(0, keyType)
			buildChild := colexec.NewMockOperator()
			t.Cleanup(func() { buildChild.Free(proc, false, nil) })
			build := &hashbuild.HashBuild{
				JoinMapTag: 1, JoinMapRefCnt: 1, Conditions: []*plan.Expr{key},
				NeedHashMap: true, NeedBatches: true, NeedAllocateSels: !tc.usePK,
				HashOnPK: tc.usePK, DirectInput: tc.usePK,
			}
			build.AppendChild(buildChild)
			t.Cleanup(func() { build.Free(proc, false, nil) })

			probeChild := colexec.NewMockOperator()
			t.Cleanup(func() { probeChild.Free(proc, false, nil) })
			arg := &HashJoin{
				JoinType: tc.joinType, JoinMapTag: 1, NumCPU: 1, IsMerger: true,
				HashOnPK: tc.usePK, IsRightJoin: tc.usePK,
				LeftTypes: []types.Type{keyType}, RightTypes: []types.Type{keyType, keyType, textType},
				ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 1}, {Rel: 1, Pos: 2}},
				EqConds:    [][]*plan.Expr{{key}, {key}},
			}
			buildRows := max(defaultRows, tc.buildRows)
			wideRows := buildRows
			if tc.useResidual {
				// ON p.k = b.k AND p.k <= b.bound
				bound := newExpr(3, keyType)
				bound.GetCol().RelPos = 1
				cmp, err := function.GetFunctionByName(proc.Ctx, "<=", []types.Type{keyType, keyType})
				require.NoError(t, err)
				arg.RightTypes = append(arg.RightTypes, keyType)
				arg.NonEqCond = &plan.Expr{
					Typ: types.T_bool.ToType().PlanType(),
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: &plan.ObjectRef{Obj: cmp.GetEncodedOverloadID(), ObjName: "<="},
						Args: []*plan.Expr{key, bound},
					}},
				}
				wideRows++
				buildRows = wideRows + 3
			}
			arg.AppendChild(probeChild)
			t.Cleanup(func() { arg.Free(proc, false, nil) })

			registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
			require.NoError(t, err)
			account, err := registry.OpenWithController(queryBudget, generation)
			require.NoError(t, err)
			require.NoError(t, build.SetAllocationAccount(account))
			require.NoError(t, arg.SetAllocationAccount(account))

			for first := 0; first < buildRows; first += rowsPerInput {
				last := min(first+rowsPerInput, buildRows)
				input := batch.NewWithSize(len(arg.RightTypes))
				buildChild.WithBatchs([]*batch.Batch{input})
				for col, typ := range arg.RightTypes {
					input.Vecs[col] = vector.NewVec(typ)
				}
				for row := first; row < last; row++ {
					buildKey := int64(1)
					var payload []byte
					var payloadNull bool
					if tc.usePK {
						buildKey = int64(row)
					}
					switch row {
					case wideRows:
						buildKey, payload = 2, []byte(inlineValue)
					case wideRows + 1:
						buildKey, payloadNull = 2, true
					case wideRows + 2:
						buildKey, payload = 3, []byte("rejected")
					default:
						payload = bytes.Repeat([]byte{byte(row + 1)}, valueBytes)
					}
					// Put a short value near the start of both the probe and
					// finalize output. Each batch can fit one extra row.
					if tc.usePK && (row == 0 || row == 13) {
						payload = []byte(inlineValue)
					}
					if tc.unmatchedOutlier {
						if row == 0 {
							buildKey = 9
						} else {
							payload = []byte(inlineValue)
						}
					}
					require.NoError(t, vector.AppendFixed(input.Vecs[0], buildKey, false, mp))
					require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(row), false, mp))
					require.NoError(t, vector.AppendBytes(input.Vecs[2], payload, payloadNull, mp))
					if tc.useResidual {
						bound := buildKey
						if row == wideRows-2 || row == wideRows+2 {
							bound = 0
						}
						require.NoError(t, vector.AppendFixed(input.Vecs[3], bound, row == wideRows-1, mp))
					}
				}
				input.SetRowCount(last - first)
			}

			require.NoError(t, buildChild.Prepare(proc))
			require.NoError(t, build.Prepare(proc))
			built, err := vm.Exec(build, proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, built.Status)
			buildChild.Free(proc, false, nil)

			probe := batch.NewWithSize(1)
			probeChild.WithBatchs([]*batch.Batch{probe})
			probe.Vecs[0] = vector.NewVec(keyType)
			for _, key := range tc.probeKeys {
				require.NoError(t, vector.AppendFixed(probe.Vecs[0], key, false, mp))
			}
			probe.SetRowCount(len(tc.probeKeys))
			require.NoError(t, probeChild.Prepare(proc))
			require.NoError(t, arg.Prepare(proc))

			// -1 represents the NULL side of an outer join row.
			type joinedRow struct{ key, id int64 }
			inlineID, nullID := int64(wideRows), int64(wideRows+1)
			want := make(map[joinedRow]bool)
			for row := range buildRows {
				key := int64(1)
				if tc.usePK {
					key = -1
					for _, probeKey := range tc.probeKeys {
						if probeKey == int64(row) {
							key = probeKey
							break
						}
					}
				}
				want[joinedRow{key, int64(row)}] = true
			}
			if tc.unmatchedOutlier {
				delete(want, joinedRow{1, 0})
			}
			if tc.useResidual {
				// The first probe has more qualifying wide rows than one result
				// batch can hold. Its final candidates are false and NULL.
				for id := wideRows - 2; id < buildRows; id++ {
					delete(want, joinedRow{1, int64(id)})
				}
				want[joinedRow{2, inlineID}] = true
				want[joinedRow{2, nullID}] = true
				want[joinedRow{3, -1}] = true
				want[joinedRow{4, -1}] = true
			} else if !tc.usePK {
				want[joinedRow{2, -1}] = true
			}

			var batchRows []int
			for calls := 0; ; calls++ {
				require.LessOrEqual(t, calls, buildRows+2)
				result, err := vm.Exec(arg, proc)
				require.NoError(t, err)
				if result.Status == vm.ExecStop {
					break
				}
				if result.Batch == nil || result.Batch.IsEmpty() {
					continue
				}
				out := result.Batch
				batchRows = append(batchRows, out.RowCount())
				require.LessOrEqual(t, int64(len(out.Vecs[2].GetArea())), mpool.MaxAllocationSize())
				keys := vector.MustFixedColNoTypeCheck[int64](out.Vecs[0])
				ids := vector.MustFixedColNoTypeCheck[int64](out.Vecs[1])
				for row := range out.RowCount() {
					key, id := int64(-1), int64(-1)
					if !out.Vecs[0].IsNull(uint64(row)) {
						key = keys[row]
					}
					if !out.Vecs[1].IsNull(uint64(row)) {
						id = ids[row]
					}
					got := joinedRow{key, id}
					require.True(t, want[got], "unexpected or repeated output row %+v", got)
					delete(want, got)
					isNull := id == -1 || id == nullID
					require.Equal(t, isNull, out.Vecs[2].IsNull(uint64(row)))
					if id == inlineID || (tc.unmatchedOutlier && !isNull) || (tc.usePK && (id == 0 || id == 13)) {
						require.Equal(t, inlineValue, string(out.Vecs[2].GetBytesAt(row)))
					} else if !isNull {
						payload := out.Vecs[2].GetBytesAt(row)
						require.Len(t, payload, valueBytes)
						require.Equal(t, valueBytes, bytes.Count(payload, []byte{byte(id + 1)}))
					}
				}
			}
			require.Empty(t, want)
			require.Equal(t, tc.wantBatchRows, batchRows)
		})
	}
}
