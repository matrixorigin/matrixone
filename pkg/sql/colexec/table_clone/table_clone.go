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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
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
	if tc.dataObjBat != nil {
		tc.dataObjBat.Clean(proc.Mp())
	}

	if tc.tombstoneObjBat != nil {
		tc.tombstoneObjBat.Clean(proc.Mp())
	}

	for _, r := range tc.srcReader {
		r.Close()
	}

	for _, r := range tc.srcIdxReader {
		r.Close()
	}

	tc.srcRel = nil
	tc.srcReader = nil
	tc.srcIdxReader = nil
	tc.dstRel = nil
	tc.dstIdxRel = nil
}

func (tc *TableClone) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if tc.dataObjBat != nil {
		tc.dataObjBat.Clean(proc.Mp())
	}

	if tc.tombstoneObjBat != nil {
		tc.tombstoneObjBat.Clean(proc.Mp())
	}

	for _, r := range tc.srcReader {
		r.Close()
	}

	for _, r := range tc.srcIdxReader {
		r.Close()
	}

	tc.srcRel = nil
	tc.srcReader = nil
	tc.srcIdxReader = nil
	tc.dstRel = nil
	tc.dstIdxRel = nil

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

func initRelAndReader(
	ctx context.Context,
	db engine.Database,
	masterTblName string,
	relMap map[string]engine.Relation,
	readerMap map[string]engine.Reader,
	idxRelMap map[string]engine.Relation,
	idxReaderMap map[string]engine.Reader,
	metadata partition.PartitionMetadata,
) (err error) {

	var (
		tmpRel    engine.Relation
		tmpReader engine.Reader
	)

	// if it is a partitioned table
	if metadata.Partitions != nil {
		// master tables
		for _, p := range metadata.Partitions {
			if tmpRel, err = db.Relation(ctx, p.PartitionTableName, nil); err != nil {
				return err
			}

			relMap[p.Name] = tmpRel

			if readerMap != nil {
				if tmpReader, err = disttae.NewTableMetaReader(ctx, tmpRel); err != nil {
					return err
				}
				readerMap[p.Name] = tmpReader
			}
		}

		// index tables
		for pName, rel := range relMap {
			for _, idx := range rel.GetTableDef(ctx).Indexes {
				if tmpRel, err = db.Relation(ctx, idx.IndexTableName, nil); err != nil {
					return err
				}

				if tmpReader, err = disttae.NewTableMetaReader(ctx, tmpRel); err != nil {
					return err
				}

				if idxReaderMap != nil {
					idxReaderMap[pName+"."+idx.IndexName] = tmpReader
				}

				if idxRelMap != nil {
					idxRelMap[pName+"."+idx.IndexName] = tmpRel
				}
			}
		}
	} else {
		if tmpRel, err = db.Relation(ctx, masterTblName, nil); err != nil {
			return err
		}
		relMap["p0"] = tmpRel

		if readerMap != nil {
			if tmpReader, err = disttae.NewTableMetaReader(ctx, tmpRel); err != nil {
				return err
			}
			readerMap["p0"] = tmpReader
		}

		for _, idx := range relMap["p0"].GetTableDef(ctx).Indexes {
			if tmpRel, err = db.Relation(ctx, idx.IndexTableName, nil); err != nil {
				return err
			}

			if idxReaderMap != nil {
				if idxReaderMap[idx.IndexName], err = disttae.NewTableMetaReader(ctx, tmpRel); err != nil {
					return err
				}
			}

			if idxRelMap != nil {
				idxRelMap[idx.IndexName] = tmpRel
			}
		}
	}

	return nil
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

	tc.dataObjBat = colexec.AllocCNS3ResultBat(false, false)
	tc.tombstoneObjBat = colexec.AllocCNS3ResultBat(true, false)

	tc.srcRel = make(map[string]engine.Relation)
	tc.srcReader = make(map[string]engine.Reader)
	tc.srcIdxReader = make(map[string]engine.Reader)

	tc.dstRel = make(map[string]engine.Relation)
	tc.dstIdxRel = make(map[string]engine.Relation)

	var (
		err   error
		txnOp client.TxnOperator

		srcDB engine.Database
		dstDB engine.Database
	)

	tc.Ctx.SrcCtx = proc.Ctx
	if tc.Ctx.SrcObjDef.PubInfo != nil {
		// the src table is a publication
		tc.Ctx.SrcCtx = defines.AttachAccountId(tc.Ctx.SrcCtx, uint32(tc.Ctx.SrcObjDef.PubInfo.TenantId))

	} else if tc.Ctx.ScanSnapshot != nil {
		if tc.Ctx.ScanSnapshot.Tenant != nil {
			// the source data may be coming from a different account.
			tc.Ctx.SrcCtx = defines.AttachAccountId(tc.Ctx.SrcCtx, tc.Ctx.ScanSnapshot.Tenant.TenantID)
		}

		// without setting this scan ts, we could read the newly created table !!!
		if tc.Ctx.ScanSnapshot.TS != nil {
			txnOp = proc.GetTxnOperator().CloneSnapshotOp(*tc.Ctx.ScanSnapshot.TS)
			proc.SetCloneTxnOperator(txnOp)
		}
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

	if dstDB, err = tc.Ctx.Eng.Database(
		proc.Ctx, tc.Ctx.DstDatabaseName, proc.GetTxnOperator(),
	); err != nil {
		return err
	}

	var (
		pSrv = proc.GetPartitionService()

		partitioned bool
		metadata    partition.PartitionMetadata
	)

	partitioned =
		pSrv != nil &&
			pSrv.Enabled() &&
			features.IsPartitioned(tc.Ctx.SrcTblDef.FeatureFlag)

	// src tables
	{
		if partitioned {
			if metadata, err = pSrv.GetPartitionMetadata(
				tc.Ctx.SrcCtx, tc.Ctx.SrcTblDef.TblId, txnOp,
			); err != nil {
				return err
			}
		}

		if err = initRelAndReader(
			tc.Ctx.SrcCtx, srcDB, tc.Ctx.SrcTblDef.Name, tc.srcRel,
			tc.srcReader, nil, tc.srcIdxReader, metadata,
		); err != nil {
			return err
		}
	}

	// dst tables
	{
		if tc.dstMasterRel, err = dstDB.Relation(proc.Ctx, tc.Ctx.DstTblName, nil); err != nil {
			return err
		}

		if partitioned {
			if metadata, err = pSrv.GetPartitionMetadata(
				proc.Ctx, tc.dstMasterRel.GetTableID(proc.Ctx), proc.GetTxnOperator(),
			); err != nil {
				return err
			}
		}

		if err = initRelAndReader(
			proc.Ctx, dstDB, tc.dstMasterRel.GetTableName(),
			tc.dstRel, nil, tc.dstIdxRel, nil, metadata,
		); err != nil {
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

		var (
			objCnt int
			blkCnt int
			rowCnt int
		)

		col, area := vector.MustVarlenaRawData(bat.Vecs[idx])
		for i := range col {
			stats := objectio.ObjectStats(col[i].GetByteSlice(area))
			if stats.GetAppendable() || !stats.GetCNCreated() {
				return moerr.NewInternalErrorNoCtxf("object fmt wrong: %s", stats.FlagString())
			}

			objCnt++
			blkCnt += int(stats.BlkCnt())
			rowCnt += int(stats.Rows())
		}

		dstDef := dstRel.GetTableDef(dstCtx)
		innerReader := reader.(*disttae.TableMetaReader)
		srcDef := innerReader.GetTableDef()

		logutil.Info("TABLE-CLONE",
			zap.Bool("isTombstone", isTombstone),
			zap.String("src", fmt.Sprintf("%s(%d)-%s(%d)", srcDef.DbName, srcDef.DbId, srcDef.Name, srcDef.TblId)),
			zap.String("dst", fmt.Sprintf("%s(%d)-%s(%d)", dstDef.DbName, dstDef.DbId, dstDef.Name, dstDef.TblId)),
			zap.Int("objCnt", objCnt),
			zap.Int("blkCnt", blkCnt),
			zap.Int("rowCnt", rowCnt),
			zap.String("read txn", innerReader.GetTxnInfo()))

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

	for name, srcReader := range tc.srcReader {
		if err = clone(
			proc.Ctx, tc.Ctx.SrcCtx,
			proc.Mp(), srcReader, tc.dstRel[name],
			tc.dataObjBat, tc.tombstoneObjBat,
		); err != nil {
			return vm.CallResult{}, err
		}
	}

	for idxName, reader := range tc.srcIdxReader {
		tc.dataObjBat.CleanOnlyData()
		tc.tombstoneObjBat.CleanOnlyData()

		if err = clone(
			proc.Ctx, tc.Ctx.SrcCtx,
			proc.Mp(), reader, tc.dstIdxRel[idxName],
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

	dstTblDef = tc.dstMasterRel.GetTableDef(dstCtx)
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
		dstCtx, tc.dstMasterRel.GetTableID(dstCtx), vecs, vecs[0].Length(), maxVal,
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
