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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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
	if tc.dataObjBat != nil {
		tc.dataObjBat.CleanOnlyData()
	}

	if tc.tombstoneObjBat != nil {
		tc.tombstoneObjBat.CleanOnlyData()
	}

	if tc.idxNameToReader != nil {
		for _, r := range tc.idxNameToReader {
			r.Close()
		}
		clear(tc.idxNameToReader)
	}

	if tc.dstIdxNameToRel != nil {
		clear(tc.dstIdxNameToRel)
	}

	if tc.srcRelReader != nil {
		tc.srcRelReader.Close()
	}

	if tc.Ctx.SrcAutoIncrOffsets != nil {
		clear(tc.Ctx.SrcAutoIncrOffsets)
	}
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
		txnOp client.TxnOperator

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
		tc.Ctx.SrcCtx = proc.Ctx
		if tc.Ctx.ScanSnapshot != nil && tc.Ctx.ScanSnapshot.Tenant != nil {
			// the source data may be coming from a different account.
			tc.Ctx.SrcCtx = defines.AttachAccountId(tc.Ctx.SrcCtx, tc.Ctx.ScanSnapshot.Tenant.TenantID)
		}

		txnOp = proc.GetCloneTxnOperator()
		if txnOp == nil {
			txnOp = proc.GetTxnOperator()
		}

		if srcDB, err = tc.Ctx.Eng.Database(
			tc.Ctx.SrcCtx, tc.Ctx.SrcTblDef.DbName, txnOp,
		); err != nil {
			return err
		}

		if tc.srcRel, err = srcDB.Relation(
			tc.Ctx.SrcCtx, tc.Ctx.SrcTblDef.Name, nil,
		); err != nil {
			return err
		}

		if tc.srcRelReader, err = disttae.NewTableMetaReader(tc.Ctx.SrcCtx, tc.srcRel); err != nil {
			return err
		}
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
			srcDB.Relation(tc.Ctx.SrcCtx, idx.IndexTableName, nil); err != nil {
			return err
		}

		if tc.idxNameToReader[idx.IndexName], err =
			disttae.NewTableMetaReader(tc.Ctx.SrcCtx, srcIdxRel); err != nil {
			return err
		}
	}

	dstIndexes := tc.dstRel.GetTableDef(proc.Ctx).Indexes
	for _, idx := range dstIndexes {
		if tc.dstIdxNameToRel[idx.IndexName], err =
			dstDB.Relation(proc.Ctx, idx.IndexTableName, nil); err != nil {
			return err
		}
	}

	return nil
}

func clone(
	dstCtx context.Context,
	srcCtx context.Context,
	mp *mpool.MPool,
	reader engine.Reader,
	dstRel engine.Relation,
	dataObjBat *batch.Batch,
	tombstoneObjBat *batch.Batch,
) error {

	var (
		err error
	)

	checkObjStatsFmt := func(bat *batch.Batch, isTombstone bool) error {
		idx := 0
		if !isTombstone {
			idx = 1
		}

		col, area := vector.MustVarlenaRawData(bat.Vecs[idx])
		for i := range col {
			stats := objectio.ObjectStats(col[i].GetByteSlice(area))
			if stats.GetAppendable() || !stats.GetCNCreated() {
				return moerr.NewInternalErrorNoCtxf("object fmt wrong: %s", stats.FlagString())
			}

			//if isTombstone {
			//	fmt.Println("copy tombstone", dstRel.GetTableName(), stats.Rows(), stats.GetAppendable())
			//} else {
			//	fmt.Println("copy data", dstRel.GetTableName(), stats.Rows(), stats.GetAppendable())
			//}
		}

		return nil
	}

	// copy data
	{
		if _, err = reader.Read(srcCtx, nil, nil, mp, dataObjBat); err != nil {
			return err
		}

		if err = checkObjStatsFmt(dataObjBat, false); err != nil {
			return err
		}

		if dataObjBat.RowCount() != 0 {
			if err = dstRel.Write(dstCtx, dataObjBat); err != nil {
				return err
			}
		}
	}

	// copy tombstone
	{
		if _, err = reader.Read(srcCtx, nil, nil, mp, tombstoneObjBat); err != nil {
			return err
		}

		if err = checkObjStatsFmt(tombstoneObjBat, true); err != nil {
			return err
		}

		if tombstoneObjBat.RowCount() != 0 {
			if err = dstRel.Delete(dstCtx, tombstoneObjBat, ""); err != nil {
				return err
			}
		}
	}

	return nil
}

func (tc *TableClone) Call(proc *process.Process) (vm.CallResult, error) {

	var (
		err error
	)

	if err = clone(
		proc.Ctx, tc.Ctx.SrcCtx,
		proc.Mp(), tc.srcRelReader, tc.dstRel,
		tc.dataObjBat, tc.tombstoneObjBat,
	); err != nil {
		return vm.CallResult{}, err
	}

	for idxName, reader := range tc.idxNameToReader {
		tc.dataObjBat.CleanOnlyData()
		tc.tombstoneObjBat.CleanOnlyData()

		if err = clone(
			proc.Ctx, tc.Ctx.SrcCtx,
			proc.Mp(), reader, tc.dstIdxNameToRel[idxName],
			tc.dataObjBat, tc.tombstoneObjBat,
		); err != nil {
			return vm.CallResult{}, err
		}
	}

	if err = tc.updateDstAutoIncrColumns(proc.Ctx, proc); err != nil {
		return vm.CallResult{}, err
	}

	return vm.CallResult{}, nil
}

func (tc *TableClone) updateDstAutoIncrColumns(
	dstCtx context.Context,
	proc *process.Process,
) error {

	if tc.Ctx.SrcAutoIncrOffsets == nil {
		return nil
	}

	var (
		err      error
		typs     []types.Type
		incrCols []incrservice.AutoColumn

		maxVal int64

		dstTblDef *plan.TableDef
	)

	dstTblDef = tc.dstRel.GetTableDef(dstCtx)
	_, typs, _, _, _ = colexec.GetSequmsAttrsSortKeyIdxFromTableDef(dstTblDef)
	incrCols = incrservice.GetAutoColumnFromDef(dstTblDef)

	vecs := make([]*vector.Vector, len(typs))
	for i, typ := range typs {
		vecs[i] = vector.NewVec(typ)
	}

	defer func() {
		for i := range vecs {
			vecs[i].Free(proc.Mp())
		}
	}()

	maxVal = int64(0)
	for _, col := range incrCols {
		maxVal = int64(tc.Ctx.SrcAutoIncrOffsets[int32(col.ColIndex)])

		var val any
		switch typs[col.ColIndex].Oid {
		case types.T_uint8:
			val = uint8(maxVal)
		case types.T_uint16:
			val = uint16(maxVal)
		case types.T_uint32:
			val = uint32(maxVal)
		case types.T_uint64:
			val = uint64(maxVal)
		case types.T_int8:
			val = int8(maxVal)
		case types.T_int16:
			val = int16(maxVal)
		case types.T_int32:
			val = int32(maxVal)
		case types.T_int64:
			val = int64(maxVal)
		}

		if err = vector.AppendAny(
			vecs[col.ColIndex], val, false, proc.Mp(),
		); err != nil {
			return err
		}
	}

	if _, err = proc.GetIncrService().InsertValues(
		dstCtx, tc.dstRel.GetTableID(dstCtx), vecs, vecs[0].Length(), maxVal,
	); err != nil {
		return err
	}

	return nil
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
