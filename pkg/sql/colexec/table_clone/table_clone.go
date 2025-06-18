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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
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
	tc.dataObjBat.Clean(proc.Mp())
	tc.tombstoneObjBat.Clean(proc.Mp())
	tc.idxNameToReader = nil
	tc.dstIdxNameToRel = nil
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
		err   error
		srcDB engine.Database
		dstDB engine.Database

		srcIdxRel engine.Relation
	)

	if len(tc.Ctx.SrcTblDef.Indexes) > 0 {
		tc.idxNameToReader = make(map[string]engine.Reader)
		tc.dstIdxNameToRel = make(map[string]engine.Relation)
	}

	tc.dataObjBat = colexec.AllocCNS3ResultBat(false, false)
	tc.tombstoneObjBat = colexec.AllocCNS3ResultBat(true, false)

	{
		if srcDB, err = tc.Ctx.Eng.Database(
			proc.Ctx, tc.Ctx.SrcTblDef.DbName, proc.GetTxnOperator(),
		); err != nil {
			return err
		}

		if tc.srcRel, err = srcDB.Relation(
			proc.Ctx, tc.Ctx.SrcTblDef.Name, nil,
		); err != nil {
			return err
		}

		tc.srcRelReader, err = disttae.NewTableMetaReader(proc.Ctx, tc.srcRel)
	}

	{
		if dstDB, err = tc.Ctx.Eng.Database(
			proc.Ctx, tc.Ctx.DstDatabaseName, proc.GetTxnOperator(),
		); err != nil {
			return err
		}

		if tc.dstRel, err = dstDB.Relation(proc.Ctx, tc.Ctx.DstTblName, nil); err != nil {
			return err
		}
	}

	for _, idx := range tc.Ctx.SrcTblDef.Indexes {
		if srcIdxRel, err =
			srcDB.Relation(proc.Ctx, idx.IndexTableName, nil); err != nil {
			return err
		}

		if tc.idxNameToReader[idx.IndexName], err =
			disttae.NewTableMetaReader(proc.Ctx, srcIdxRel); err != nil {
			return err
		}
	}

	dstIndexes := tc.dstRel.GetTableDef(proc.Ctx).Indexes
	for _, idx := range dstIndexes {
		if tc.dstIdxNameToRel[idx.IndexName], err =
			srcDB.Relation(proc.Ctx, idx.IndexTableName, nil); err != nil {
			return err
		}
	}

	return nil
}

func clone(
	ctx context.Context,
	mp *mpool.MPool,
	reader engine.Reader,
	dstRel engine.Relation,
	dataObjBat *batch.Batch,
	tombstoneObjBat *batch.Batch,
) error {

	var (
		err error
	)

	if _, err = reader.Read(ctx, nil, nil, mp, dataObjBat); err != nil {
		return err
	}

	// write data to the workspace
	//for i := range dataObjBat.Vecs[1].Length() {
	//	stats := objectio.ObjectStats(dataObjBat.Vecs[1].GetBytesAt(i))
	//	fmt.Println("copy data", dstRel.GetTableName(), stats.Rows())
	//}

	if dataObjBat.RowCount() != 0 {
		if err = dstRel.Write(ctx, dataObjBat); err != nil {
			return err
		}
	}

	if _, err = reader.Read(ctx, nil, nil, mp, tombstoneObjBat); err != nil {
		return err
	}

	// write tombstone to the workspace
	//for i := range tombstoneObjBat.Vecs[0].Length() {
	//	stats := objectio.ObjectStats(tombstoneObjBat.Vecs[0].GetBytesAt(i))
	//	fmt.Println("copy tombstone", dstRel.GetTableName(), stats.Rows())
	//}

	if tombstoneObjBat.RowCount() != 0 {
		if err = dstRel.Delete(ctx, tombstoneObjBat, ""); err != nil {
			return err
		}
	}

	return nil
}

func (tc *TableClone) Call(proc *process.Process) (vm.CallResult, error) {

	var (
		err error
	)

	if err = clone(
		proc.Ctx, proc.Mp(), tc.srcRelReader, tc.dstRel,
		tc.dataObjBat, tc.tombstoneObjBat,
	); err != nil {
		return vm.CallResult{}, err
	}

	for idxName, reader := range tc.idxNameToReader {
		tc.dataObjBat.CleanOnlyData()
		tc.tombstoneObjBat.CleanOnlyData()

		if err = clone(
			proc.Ctx, proc.Mp(), reader, tc.dstIdxNameToRel[idxName],
			tc.dataObjBat, tc.tombstoneObjBat,
		); err != nil {
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
