// Copyright 2025 Matrix Origin
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

package table_clone

import (
	"bytes"
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	reuse.CreatePool[TableClone](
		func() *TableClone {
			return &TableClone{}
		},
		func(a *TableClone) {
			*a = TableClone{}
		},
		reuse.DefaultOptions[TableClone]().
			WithEnableChecker(),
	)
}

func (tc *TableClone) Free(proc *process.Process, pipelineFailed bool, err error) {

}

func (tc *TableClone) Reset(proc *process.Process, pipelineFailed bool, err error) {
	return
}

func (tc *TableClone) String(buf *bytes.Buffer) {
	//TODO implement me
	panic("implement me")
}

func (tc *TableClone) OpType() vm.OpType {
	return 0
}

func (tc *TableClone) Prepare(proc *process.Process) error {
	if tc.OpAnalyzer == nil {
		tc.OpAnalyzer = process.NewAnalyzer(
			tc.GetIdx(),
			false,
			true,
			"table_clone")
	} else {
		tc.OpAnalyzer.Reset()
	}

	var (
		err error

		dstDB engine.Database
	)

	if tc.SrcRel, err = colexec.GetRelAndPartitionRelsByObjRef(
		proc.Ctx, proc, tc.Ctx.Eng, tc.Ctx.SrcObjDef,
	); err != nil {
		return err
	}

	if dstDB, err = tc.Ctx.Eng.Database(
		proc.Ctx, tc.Ctx.DstDatabaseName, proc.GetTxnOperator(),
	); err != nil {
		return err
	}

	if tc.DstRel, err = dstDB.Relation(proc.Ctx, tc.Ctx.DstTblName, nil); err != nil {
		return err
	}

	tc.DataObjBat = colexec.AllocCNS3ResultBat(false, false)
	tc.TombstoneObjBat = colexec.AllocCNS3ResultBat(true, false)

	return nil
}

func (tc *TableClone) reflagMeta(
	ctx context.Context,
	mp *mpool.MPool,
	bat *batch.Batch,
) {

	var (
		idx int
	)

	if bat.VectorCount() > 1 {
		idx = 1
	}

	col, area := vector.MustVarlenaRawData(bat.Vecs[idx])
	for i := range col {
		stats := objectio.ObjectStats(col[i].GetByteSlice(area))
		objectio.WithCNCreated()(&stats)
		vector.SetBytesAt(bat.Vecs[idx], i, stats.Marshal(), mp)
	}
}

func (tc *TableClone) Call(proc *process.Process) (vm.CallResult, error) {

	var (
		err error
	)

	if _, err = tc.R.Read(proc.Ctx, nil, nil, proc.Mp(), tc.DataObjBat); err != nil {
		return vm.CallResult{}, err
	}

	// write data to the workspace
	for i := range tc.DataObjBat.Vecs[1].Length() {
		stats := objectio.ObjectStats(tc.DataObjBat.Vecs[1].GetBytesAt(i))
		fmt.Println("copy data", stats.Rows())
	}

	if tc.DataObjBat.RowCount() != 0 {
		tc.reflagMeta(proc.Ctx, proc.Mp(), tc.DataObjBat)
		if err = tc.DstRel.Write(proc.Ctx, tc.DataObjBat); err != nil {
			return vm.CallResult{}, err
		}
	}

	if _, err = tc.R.Read(proc.Ctx, nil, nil, proc.Mp(), tc.TombstoneObjBat); err != nil {
		return vm.CallResult{}, err
	}

	// write tombstone to the workspace
	for i := range tc.TombstoneObjBat.Vecs[0].Length() {
		stats := objectio.ObjectStats(tc.TombstoneObjBat.Vecs[0].GetBytesAt(i))
		fmt.Println("copy tombstone", stats.Rows())
	}

	if tc.TombstoneObjBat.RowCount() != 0 {
		tc.reflagMeta(proc.Ctx, proc.Mp(), tc.TombstoneObjBat)
		if err = tc.DstRel.Delete(proc.Ctx, tc.TombstoneObjBat, ""); err != nil {
			return vm.CallResult{}, err
		}
	}

	return vm.CallResult{}, nil
}

func (tc *TableClone) Release() {
	if tc != nil {
		reuse.Free[TableClone](tc, nil)
	}
}

func (tc *TableClone) GetOperatorBase() *vm.OperatorBase {
	return &tc.OperatorBase
}

func (tc *TableClone) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (tc *TableClone) TypeName() string {
	return "table_clone"
}
