// Copyright 2024 Matrix Origin
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

package colexec

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(MockOperator)

type MockOperator struct {
	vm.OperatorBase

	batchs  []*batch.Batch
	current int
}

func (op *MockOperator) GetOperatorBase() *vm.OperatorBase {
	return &op.OperatorBase
}

func NewMockOperator() *MockOperator {
	return &MockOperator{}
}

func (op *MockOperator) WithBatchs(batchs []*batch.Batch) *MockOperator {
	op.batchs = append(op.batchs, batchs...)
	return op
}

func (op *MockOperator) Release() {
}

func (op *MockOperator) Reset(proc *process.Process, pipelineFailed bool, err error) {
	op.Free(proc, pipelineFailed, err)
}

func (op *MockOperator) Free(proc *process.Process, pipelineFailed bool, err error) {
	for i := range op.batchs {
		op.batchs[i].Clean(proc.Mp())
	}
	op.batchs = nil
	op.current = 0
}

func (op *MockOperator) String(buf *bytes.Buffer) {
	buf.WriteString(": mock")
}

func (op *MockOperator) OpType() vm.OpType {
	return vm.Mock
}

func (op *MockOperator) Prepare(proc *process.Process) (err error) {
	return nil
}

func (op *MockOperator) Call(proc *process.Process) (vm.CallResult, error) {
	result := vm.NewCallResult()
	if op.current >= len(op.batchs) {
		result.Status = vm.ExecStop
		return result, nil
	}
	result.Batch = op.batchs[op.current]
	op.current = op.current + 1
	return result, nil
}

func makeMockVecs() []*vector.Vector {
	vecs := make([]*vector.Vector, 5)
	vecs[0] = testutil.MakeInt32Vector([]int32{1, 1000}, nil)
	uuid1, _ := types.BuildUuid()
	uuid2, _ := types.BuildUuid()
	vecs[1] = testutil.MakeUUIDVector([]types.Uuid{uuid1, uuid2}, nil)
	vecs[2] = testutil.MakeVarcharVector([]string{"abcd", "中文..............................................."}, nil)
	vecs[3] = testutil.MakeJsonVector([]string{"{\"a\":1, \"b\":\"dd\"}", "{\"a\":2, \"b\":\"dd\"}"}, nil)
	vecs[4] = testutil.MakeDatetimeVector([]string{"2021-01-01 00:00:00", "2025-01-01 00:00:00"}, nil)
	return vecs
}

// new batchs with schema : (a int, b uuid, c varchar, d json, e datetime)
func MakeMockBatchs() *batch.Batch {
	bat := batch.New(true, []string{"a", "b", "c", "d", "e"})
	vecs := makeMockVecs()
	bat.Vecs = vecs
	bat.SetRowCount(vecs[0].Length())
	return bat
}

func makeMockTimeWinVecs() []*vector.Vector {
	vecs := make([]*vector.Vector, 2)
	vecs[0] = testutil.MakeDatetimeVector([]string{
		"2021-01-01 00:00:01", "2025-01-01 00:00:02",
		"2021-01-01 00:00:03", "2025-01-01 00:00:04",
	}, nil)
	vecs[1] = testutil.MakeInt32Vector([]int32{1, 4, 5, 1000}, nil)
	return vecs
}

func MakeMockTimeWinBatchs() *batch.Batch {
	bat := batch.New(true, []string{"ts", "b"})
	vecs := makeMockTimeWinVecs()
	bat.Vecs = vecs
	bat.SetRowCount(vecs[0].Length())
	return bat
}

// new batchs with schema : (a int, b uuid, c varchar, d json, e date)
func MakeMockBatchsWithRowID() *batch.Batch {
	bat := batch.New(true, []string{catalog.Row_ID, "a", "b", "c", "d", "e"})
	vecs := makeMockVecs()

	uuid1 := objectio.NewSegmentid()
	blkId1 := objectio.NewBlockid(uuid1, 0, 0)
	rowid1 := *objectio.NewRowid(blkId1, 0)
	rowid2 := *objectio.NewRowid(blkId1, 0)
	bat.Vecs[0] = testutil.MakeRowIdVector([]types.Rowid{rowid1, rowid2}, nil)
	for i := range vecs {
		bat.Vecs[i+1] = vecs[i]
	}
	bat.SetRowCount(vecs[0].Length())
	return bat
}

// new batchs with schema : (a int auto_increment, b uuid, c varchar, d json, e date)
// vecs[0] is null,  use for test preinsert...
func MakeMockBatchsWithNullVec() *batch.Batch {
	bat := batch.New(true, []string{"a", "b", "c", "d", "e"})
	vecs := makeMockVecs()
	vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, []uint64{0, 1})
	bat.Vecs = vecs
	bat.SetRowCount(vecs[0].Length())
	return bat
}

//@todo make more test batchs
