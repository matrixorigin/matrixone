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

package hashjoin

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestNonuniqueInnerLongProbePayloadCapacity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		matches    int
		probeRows  int
		dictionary bool
	}{
		{name: "duplicate group", matches: 128, probeRows: 1},
		{name: "across gather chunks", matches: 2*hashmap.UnitLimit + 1, probeRows: 1},
		{name: "different probe values", matches: 128, probeRows: 2},
		{name: "dictionary bulk control", matches: 128, probeRows: 1, dictionary: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keyType, textType := types.T_int64.ToType(), types.T_varchar.ToType()
			join := newTestCase(t, []types.Type{keyType, keyType}, []colexec.ResultPos{{Rel: 0, Pos: 1}},
				[][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}})
			join.arg.JoinType, join.arg.NonEqCond = plan.Node_INNER, nil
			join.arg.LeftTypes, join.arg.RightTypes = []types.Type{keyType, textType}, []types.Type{keyType}
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			limit := uint64(64 << 10)
			if tc.dictionary {
				// The existing dictionary repeat copier stores each payload.
				// This control preserves its bulk-copy route and footprint.
				limit = 4 << 20
			}
			account, err := registry.Open(limit)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.Zero(t, account.Snapshot().Used)
				_, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
			})
			// Bound result copies independently of the retained build hash table.
			join.arg.resultAllocation, err = vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerHashBuild,
				hashJoinAllocationSiteResultData, hashJoinAllocationSiteResultArea,
				hashJoinAllocationSiteResultNulls, hashJoinAllocationSiteResultGrouping)
			require.NoError(t, err)
			probe := batch.NewWithSchema(false, nil, join.arg.LeftTypes)
			build := batch.NewWithSchema(false, nil, join.arg.RightTypes)
			t.Cleanup(func() {
				join.arg.Reset(join.proc, false, nil)
				join.barg.Reset(join.proc, false, nil)
				join.arg.Free(join.proc, false, nil)
				join.barg.Free(join.proc, false, nil)
				probe.Clean(join.proc.Mp())
				build.Clean(join.proc.Mp())
				join.proc.GetMessageBoard().Reset()
				join.cancel()
				join.proc.Free()
				require.Zero(t, join.proc.Mp().CurrNB())
			})
			for row := range tc.probeRows {
				require.NoError(t, vector.AppendFixed(probe.Vecs[0], int64(1), false, join.proc.Mp()))
				require.NoError(t, vector.AppendBytes(probe.Vecs[1], []byte(strings.Repeat(string(rune('a'+row)), 4096)), false, join.proc.Mp()))
			}
			probe.SetRowCount(tc.probeRows)
			if tc.dictionary {
				selected := make([]*vector.Vector, len(probe.Vecs))
				admitted, err := vector.TrySelectDictionaryVectors(probe.Vecs, selected, []int64{0}, 1, join.proc.Mp(), nil)
				require.NoError(t, err)
				require.True(t, admitted)
				for _, v := range probe.Vecs {
					v.Free(join.proc.Mp())
				}
				probe.Vecs = selected
			}
			for range tc.matches {
				require.NoError(t, vector.AppendFixed(build.Vecs[0], int64(1), false, join.proc.Mp()))
			}
			build.SetRowCount(tc.matches)
			resetHashBuildChildrenWithBatch(join.barg, build)
			resetChildrenWithBatch(join.arg, probe)
			require.NoError(t, join.barg.Prepare(join.proc))
			require.NoError(t, join.arg.Prepare(join.proc))
			_, err = vm.Exec(join.barg, join.proc)
			require.NoError(t, err)
			result, err := vm.Exec(join.arg, join.proc)
			require.NoError(t, err)
			require.Equal(t, tc.probeRows*tc.matches, result.Batch.RowCount())
			area := tc.probeRows * 4096
			if tc.dictionary {
				area *= tc.matches
			}
			require.Equal(t, area, len(result.Batch.Vecs[0].GetArea()))
			for row := range result.Batch.RowCount() {
				require.Equal(t, strings.Repeat(string(rune('a'+row/tc.matches)), 4096), string(result.Batch.Vecs[0].GetBytesAt(row)))
			}
		})
	}
}

func BenchmarkHashJoinSmallDuplicatePayload(b *testing.B) {
	for _, duplicates := range []int{2, 4, 7} {
		b.Run(fmt.Sprintf("matches=%d", duplicates), func(b *testing.B) {
			typ := types.T_int64.ToType()
			tc := newTestCase(b, []types.Type{typ, typ}, nil, [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
			tc.arg.JoinType, tc.arg.NonEqCond = plan.Node_INNER, nil
			tc.arg.LeftTypes = []types.Type{typ, typ}
			tc.arg.RightTypes = make([]types.Type, 8)
			for col := range tc.arg.RightTypes {
				tc.arg.RightTypes[col] = typ
			}
			tc.arg.ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 1}}
			for col := 1; col < 8; col++ {
				tc.arg.ResultCols = append(tc.arg.ResultCols, colexec.ResultPos{Rel: 1, Pos: int32(col)})
			}
			probe, build := batch.NewWithSize(2), batch.NewWithSize(8)
			b.Cleanup(func() {
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				tc.arg.Free(tc.proc, false, nil)
				tc.barg.Free(tc.proc, false, nil)
				probe.Clean(tc.proc.Mp())
				build.Clean(tc.proc.Mp())
				tc.proc.GetMessageBoard().Reset()
				tc.cancel()
				tc.proc.Free()
				require.Zero(b, tc.proc.Mp().CurrNB())
			})
			for col := range probe.Vecs {
				probe.Vecs[col] = vector.NewVec(typ)
			}
			for col := range build.Vecs {
				build.Vecs[col] = vector.NewVec(typ)
			}
			const groups = 512
			for row := range groups * duplicates {
				for col, v := range build.Vecs {
					value := int64(row + col)
					if col == 0 {
						value = int64(row / duplicates)
					}
					require.NoError(b, vector.AppendFixed(v, value, false, tc.proc.Mp()))
				}
			}
			build.SetRowCount(groups * duplicates)
			for row := range 1024 {
				require.NoError(b, vector.AppendFixed(probe.Vecs[0], int64(row%groups), false, tc.proc.Mp()))
				require.NoError(b, vector.AppendFixed(probe.Vecs[1], int64(row), false, tc.proc.Mp()))
			}
			probe.SetRowCount(1024)
			resetHashBuildChildrenWithBatch(tc.barg, build)
			resetChildrenWithBatch(tc.arg, probe)
			require.NoError(b, tc.barg.Prepare(tc.proc))
			require.NoError(b, tc.arg.Prepare(tc.proc))
			_, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(b, err)
			require.NoError(b, tc.arg.build(tc.arg.OpAnalyzer, tc.proc))
			require.False(b, tc.arg.ctr.probeHashOnPK)
			run := func() {
				require.NoError(b, tc.arg.resetResultBat())
				ctr := &tc.arg.ctr
				ctr.leftBat = probe
				ctr.lastIdx = 0
				ctr.vsIdx = 0
				ctr.sels = nil
				ctr.probeState = psNextBatch
				result := vm.NewCallResult()
				if err := ctr.probe(tc.arg, tc.proc, &result); err != nil {
					b.Fatal(err)
				}
			}
			run()
			require.Equal(b, 1024*duplicates, tc.arg.ctr.resBat.RowCount())
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				run()
			}
		})
	}
}
