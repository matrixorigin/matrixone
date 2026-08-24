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
	"context"
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

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

func TestProductAllocationSiteLedger(t *testing.T) {
	require.Equal(t, uint8(94), uint8(productAllocationSiteResultData))
	require.Equal(t, uint8(97), uint8(productAllocationSiteResultGrouping))
}

// add unit tests for cases
type productTestCase struct {
	arg         *Product
	flgs        []bool // flgs[i] == true: nullable
	types       []types.Type
	proc        *process.Process
	cancel      context.CancelFunc
	barg        *hashbuild.HashBuild
	resultBatch *batch.Batch
}

var (
	tag int32
)

func makeTestCases(t *testing.T) []productTestCase {
	return []productTestCase{
		newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)}),
		newTestCase(t, []bool{true}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
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

		resetChildren(tc.arg, tc.proc.Mp())
		resetHashBuildChildren(tc.barg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err := vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch.RowCount(), tc.resultBatch.RowCount())
		require.Equal(t, len(res.Batch.Vecs), len(tc.resultBatch.Vecs))
		for i := range res.Batch.Vecs {
			vec1 := res.Batch.Vecs[i]
			vec2 := tc.resultBatch.Vecs[i]
			require.Equal(t, vec1.GetType().Oid, vec2.GetType().Oid)
		}

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)
		require.Zero(t, tc.arg.allocationAccount.Snapshot().Used)

		resetChildren(tc.arg, tc.proc.Mp())
		resetHashBuildChildren(tc.barg, tc.proc.Mp())
		tc.proc.GetMessageBoard().Reset()
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch.RowCount(), tc.resultBatch.RowCount())
		require.Equal(t, len(res.Batch.Vecs), len(tc.resultBatch.Vecs))
		for i := range res.Batch.Vecs {
			vec1 := res.Batch.Vecs[i]
			vec2 := tc.resultBatch.Vecs[i]
			require.Equal(t, vec1.GetType().Oid, vec2.GetType().Oid)
		}

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)
		require.Zero(t, tc.arg.allocationAccount.Snapshot().Used)

		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
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
			tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{
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
				tc.cancel()
			}()

			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))
			res, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(t, err)
			require.Nil(t, res.Batch)
			res, err = vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			require.Same(t, marker, res.Batch)
		})
	}
}

func TestProductConsumesMultipleBuildBatchesWithoutCopy(t *testing.T) {
	tc := newTestCase(
		t,
		[]bool{false},
		[]types.Type{types.T_int32.ToType()},
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

/*
	func BenchmarkProduct(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tcs = []productTestCase{
				newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)}),
				newTestCase([]bool{true}, []types.Type{types.T_int8.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)}),
			}
			t := new(testing.T)
			for _, tc := range tcs {
				bats := hashBuild(t, tc)
				err := tc.arg.Prepare(tc.proc)
				require.NoError(t, err)
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(batch.EmptyBatch)
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- nil
				tc.proc.Reg.MergeReceivers[1].Ch <- testutil.NewRegMsg(bats[1])
				for {
					ok, err := tc.arg.Call(tc.proc)
					if ok.Status == vm.ExecStop || err != nil {
						break
					}
				}
			}
		}
	}
*/
func newTestCase(t *testing.T, flgs []bool, ts []types.Type, rp []colexec.ResultPos) productTestCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	_, cancel := context.WithCancel(context.Background())
	resultBatch := batch.NewWithSize(len(rp))
	resultBatch.SetRowCount(4)
	bat := colexec.MakeMockBatchs(proc.Mp())
	for i := range rp {
		resultBatch.Vecs[i] = vector.NewVec(*bat.Vecs[rp[i].Pos].GetType())
	}
	tag++
	tc := productTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &Product{
			Result: rp,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			JoinMapTag: tag,
		},
		barg: &hashbuild.HashBuild{
			NeedBatches: true,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			JoinMapTag:    tag,
			JoinMapRefCnt: 1,
		},
		resultBatch: resultBatch,
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
