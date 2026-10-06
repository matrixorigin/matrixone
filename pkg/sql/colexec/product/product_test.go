// Copyright 2021 Matrix Origin
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

package product

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestProductAllocationSiteLedger(t *testing.T) {
	require.Equal(t, uint8(94), uint8(productAllocationSiteResultData))
	require.Equal(t, uint8(97), uint8(productAllocationSiteResultGrouping))
}

// add unit tests for cases
type productTestCase struct {
	arg  *Product
	proc *process.Process
	barg *hashbuild.HashBuild
}

var (
	tag int32
)

func makeTestCases(t *testing.T) []productTestCase {
	return []productTestCase{
		newTestCase(t, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)}),
	}
}

func TestString(t *testing.T) {
	var buf bytes.Buffer
	arg := &Product{}
	arg.String(&buf)
	require.Equal(t, "product: cross join ", buf.String())
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		t.Cleanup(func() {
			tc.arg.Free(tc.proc, false, nil)
			tc.barg.Free(tc.proc, false, nil)
			require.Zero(t, tc.proc.Mp().OnHeapCurrNB())
		})
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestPrepareRequiresAllocationAccount(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	require.ErrorIs(t, (&Product{}).Prepare(proc), mpool.ErrAllocationAccountInvalid)
}

func TestProduct(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		t.Cleanup(func() {
			tc.arg.Free(tc.proc, false, nil)
			tc.barg.Free(tc.proc, false, nil)
			require.Zero(t, tc.proc.Mp().CurrNB())
			require.Zero(t, tc.proc.Mp().OnHeapCurrNB())
		})
		for generation := 0; generation < 2; generation++ {
			func() {
				resetChildren(tc.arg, tc.proc.Mp())
				resetHashBuildChildren(tc.barg, tc.proc.Mp())
				probe := tc.arg.GetChildren(0)
				build := tc.barg.GetChildren(0)
				defer func() {
					tc.arg.Reset(tc.proc, false, nil)
					tc.barg.Reset(tc.proc, false, nil)
					usedAfterReset := tc.arg.allocationAccount.Snapshot().Used
					probe.Free(tc.proc, false, nil)
					build.Free(tc.proc, false, nil)
					tc.proc.GetMessageBoard().Reset()
					require.Zero(t, usedAfterReset)
					require.Zero(t, tc.proc.Mp().CurrNB())
					require.Zero(t, tc.proc.Mp().OnHeapCurrNB())
				}()
				require.NoError(t, tc.arg.Prepare(tc.proc))
				require.NoError(t, tc.barg.Prepare(tc.proc))
				res, err := vm.Exec(tc.barg, tc.proc)
				require.NoError(t, err)
				require.Nil(t, res.Batch)
				res, err = vm.Exec(tc.arg, tc.proc)
				require.NoError(t, err)
				require.NotNil(t, res.Batch)
				require.Equal(t, 4, res.Batch.RowCount())
				require.Len(t, res.Batch.Vecs, 2)
				for _, vec := range res.Batch.Vecs {
					require.Equal(t, types.T_int32, vec.GetType().Oid)
					require.Zero(t, vec.GetNulls().Count())
				}
				left := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])
				right := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[1])
				require.Len(t, left, 4)
				require.Len(t, right, 4)
				pairs := make([][2]int32, 4)
				for row := range pairs {
					pairs[row] = [2]int32{left[row], right[row]}
				}
				require.ElementsMatch(t, [][2]int32{{1, 1}, {1, 1000}, {1000, 1}, {1000, 1000}}, pairs)
			}()
		}
	}
}

func TestProductPassesRecursiveMarker(t *testing.T) {
	for _, test := range []struct {
		name       string
		probeData  bool
		emptyBuild bool
	}{
		{name: "marker before build"},
		{name: "marker after empty build", probeData: true, emptyBuild: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			tc := newTestCase(t, []colexec.ResultPos{
				colexec.NewResultPos(0, 0),
				colexec.NewResultPos(1, 0),
			})
			marker := colexec.MakeMockBatchs(tc.proc.Mp())
			marker.SetLast()
			probeBatches := []*batch.Batch{marker}
			if test.probeData {
				probeBatches = append([]*batch.Batch{colexec.MakeMockBatchs(tc.proc.Mp())}, marker)
			}
			probe := colexec.NewMockOperator().WithBatchs(probeBatches)
			tc.arg.Children = nil
			tc.arg.AppendChild(probe)
			if test.emptyBuild {
				resetHashBuildChildrenWithBatch(tc.barg, batch.EmptyBatch)
			} else {
				resetHashBuildChildren(tc.barg, tc.proc.Mp())
			}
			defer func() {
				tc.arg.Free(tc.proc, false, nil)
				tc.barg.Free(tc.proc, false, nil)
				probe.Free(tc.proc, false, nil)
				tc.proc.Free()
			}()

			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))
			if test.probeData {
				res, err := vm.Exec(tc.barg, tc.proc)
				require.NoError(t, err)
				require.Nil(t, res.Batch)
			}
			// The first marker must pass even before the build publishes: a
			// recursive round can depend on this progress signal.
			res, err := vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			require.Same(t, marker, res.Batch)
		})
	}
}

func TestProductConsumesMultipleBuildBatchesWithoutCopy(t *testing.T) {
	tc := newTestCase(
		t,
		[]colexec.ResultPos{
			colexec.NewResultPos(0, 0),
			colexec.NewResultPos(1, 0),
		},
	)
	probe := colexec.MakeMockBatchs(tc.proc.Mp())
	build1 := colexec.MakeMockBatchs(tc.proc.Mp())
	build2 := colexec.MakeMockBatchs(tc.proc.Mp())
	tc.arg.Children = nil
	tc.arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probe}))
	tc.barg.Children = nil
	tc.barg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{build1, build2}))

	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	_, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	wantRows := probe.RowCount() * (build1.RowCount() + build2.RowCount())
	rows := 0
	for {
		result, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		if result.Batch != nil {
			rows += result.Batch.RowCount()
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	require.Equal(t, wantRows, rows)

	tc.arg.Reset(tc.proc, false, nil)
	tc.barg.Reset(tc.proc, false, nil)
	require.Zero(t, tc.arg.allocationAccount.Snapshot().Used)
	tc.arg.Free(tc.proc, false, nil)
	tc.barg.Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func newTestCase(t *testing.T, rp []colexec.ResultPos) productTestCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	tag++
	tc := productTestCase{
		proc: proc,
		arg: &Product{
			Result:     rp,
			JoinMapTag: tag,
		},
		barg: &hashbuild.HashBuild{
			NeedBatches:   true,
			JoinMapTag:    tag,
			JoinMapRefCnt: 1,
		},
	}
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<20)
	require.NoError(t, err)
	account, err := registry.Open(1 << 60)
	require.NoError(t, err)
	require.NoError(t, tc.arg.SetAllocationAccount(account))
	require.NoError(t, tc.barg.SetAllocationAccount(account))
	return tc
}
func resetChildren(arg *Product, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func resetHashBuildChildren(arg *hashbuild.HashBuild, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	resetHashBuildChildrenWithBatch(arg, bat)
}

func resetHashBuildChildrenWithBatch(arg *hashbuild.HashBuild, bat *batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
