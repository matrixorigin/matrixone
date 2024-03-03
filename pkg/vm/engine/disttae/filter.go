// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type FastFilterOp func(objectio.ObjectStats) (bool, error)
type LoadOp = func(
	context.Context, objectio.ObjectStats, objectio.ObjectMeta, objectio.BloomFilter,
) (objectio.ObjectMeta, objectio.BloomFilter, error)
type ObjectFilterOp func(objectio.ObjectMeta, objectio.BloomFilter) (bool, error)
type BlockFilterOp func(objectio.BlockInfo, objectio.BlockObject, objectio.BloomFilter) (bool, error)
type LoadOpFactory func(fileservice.FileService) LoadOp

var loadMetadataOnlyOpFactory LoadOpFactory
var loadMetadataAndBFOpFactory LoadOpFactory

func init() {
	loadMetadataAndBFOpFactory = func(fs fileservice.FileService) LoadOp {
		return func(
			ctx context.Context,
			obj objectio.ObjectStats,
			inMeta objectio.ObjectMeta,
			inBF objectio.BloomFilter,
		) (outMeta objectio.ObjectMeta, outBF objectio.BloomFilter, err error) {
			location := obj.ObjectLocation()
			outMeta = inMeta
			if outMeta == nil {
				if outMeta, err = objectio.FastLoadObjectMeta(
					ctx, &location, false, fs,
				); err != nil {
					return nil, nil, err
				}
			}
			outBF = inBF
			if outBF == nil {
				meta := outMeta.MustDataMeta()
				if outBF, err = objectio.LoadBFWithMeta(
					ctx, meta, location, fs,
				); err != nil {
					return nil, nil, err
				}
			}
			return outMeta, outBF, nil
		}
	}
	loadMetadataOnlyOpFactory = func(fs fileservice.FileService) LoadOp {
		return func(
			ctx context.Context,
			obj objectio.ObjectStats,
			inMeta objectio.ObjectMeta,
			inBF objectio.BloomFilter,
		) (outMeta objectio.ObjectMeta, outBF objectio.BloomFilter, err error) {
			outMeta = inMeta
			outBF = inBF
			if outMeta != nil {
				return
			}
			location := obj.ObjectLocation()
			if outMeta, err = objectio.FastLoadObjectMeta(
				ctx, &location, false, fs,
			); err != nil {
				return nil, nil, err
			}
			return outMeta, outBF, nil
		}
	}
}

func isClusterOrPKFromColExpr(expr *plan.Expr_Col, tableDef *plan.TableDef) (isPK, isCluster bool) {
	if tableDef.Pkey != nil && tableDef.Pkey.PkeyColId == uint64(expr.Col.ColPos) {
		isPK = true
	}
	if !isPK && tableDef.Cols[expr.Col.ColPos].ClusterBy {
		isCluster = true
	}
	return
}

func getConstBytesFromExpr(expr *plan.Expr, colDef *plan.ColDef, proc *process.Process) ([]byte, bool) {
	constVal := getConstValueByExpr(expr, proc)
	if constVal == nil {
		return nil, false
	}
	colType := types.T(colDef.Typ.Id)
	val, ok := evalLiteralExpr2(constVal, colType)
	return val, ok
}

func mustColVecValueFromBinaryFuncExpr(
	expr *plan.Expr_F, tableDef *plan.TableDef, proc *process.Process,
) (*plan.Expr_Col, []byte, bool) {
	var (
		colExpr *plan.Expr_Col
		valExpr *plan.Expr
		ok      bool
	)
	if colExpr, ok = expr.F.Args[0].Expr.(*plan.Expr_Col); ok {
		valExpr = expr.F.Args[1]
	} else if colExpr, ok = expr.F.Args[1].Expr.(*plan.Expr_Col); ok {
		valExpr = expr.F.Args[0]
	} else {
		return nil, nil, false
	}

	switch exprImpl := valExpr.Expr.(type) {
	case *plan.Expr_Vec:
		return colExpr, exprImpl.Vec.Data, true
	}
	return nil, nil, false
}

func mustColConstValueFromBinaryFuncExpr(
	expr *plan.Expr_F, tableDef *plan.TableDef, proc *process.Process,
) (*plan.Expr_Col, []byte, bool) {
	var (
		colExpr *plan.Expr_Col
		valExpr *plan.Expr
		ok      bool
	)
	if colExpr, ok = expr.F.Args[0].Expr.(*plan.Expr_Col); ok {
		valExpr = expr.F.Args[1]
	} else if colExpr, ok = expr.F.Args[1].Expr.(*plan.Expr_Col); ok {
		valExpr = expr.F.Args[0]
	} else {
		return nil, nil, false
	}
	val, ok := getConstBytesFromExpr(
		valExpr,
		tableDef.Cols[colExpr.Col.ColPos],
		proc,
	)
	if !ok {
		return nil, nil, false
	}
	return colExpr, val, true
}

func CompileFilterExprs(
	exprs []*plan.Expr,
	proc *process.Process,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	canCompile bool,
) {
	canCompile = true
	if len(exprs) == 0 {
		return
	}
	if len(exprs) == 1 {
		return CompileFilterExpr(exprs[0], proc, tableDef, fs)
	}
	ops1 := make([]FastFilterOp, len(exprs))
	ops2 := make([]LoadOp, len(exprs))
	ops3 := make([]ObjectFilterOp, len(exprs))
	ops4 := make([]BlockFilterOp, len(exprs))

	for _, expr := range exprs {
		expr_op1, expr_op2, expr_op3, expr_op4, can := CompileFilterExpr(expr, proc, tableDef, fs)
		if !can {
			return nil, nil, nil, nil, false
		}
		ops1 = append(ops1, expr_op1)
		ops2 = append(ops2, expr_op2)
		ops3 = append(ops3, expr_op3)
		ops4 = append(ops4, expr_op4)
	}
	fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
		for _, op := range ops1 {
			if op == nil {
				continue
			}
			ok, err := op(obj)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
	loadOp = func(
		ctx context.Context,
		obj objectio.ObjectStats,
		inMeta objectio.ObjectMeta,
		inBF objectio.BloomFilter,
	) (meta objectio.ObjectMeta, bf objectio.BloomFilter, err error) {
		for _, op := range ops2 {
			if op == nil {
				continue
			}
			if meta != nil && bf != nil {
				continue
			}
			if meta, bf, err = op(ctx, obj, meta, bf); err != nil {
				return
			}
		}
		return
	}
	objectFilterOp = func(meta objectio.ObjectMeta, bf objectio.BloomFilter) (bool, error) {
		for _, op := range ops3 {
			if op == nil {
				continue
			}
			ok, err := op(meta, bf)
			if !ok || err != nil {
				return ok, err
			}
		}
		return true, nil
	}
	blockFilterOp = func(
		obj objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
	) (bool, error) {
		for _, op := range ops4 {
			if op == nil {
				continue
			}
			ok, err := op(obj, blkMeta, bf)
			if !ok || err != nil {
				return ok, err
			}
		}
		return true, nil
	}
	return
}

func CompileFilterExpr(
	expr *plan.Expr,
	proc *process.Process,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	canCompile bool,
) {
	canCompile = true
	if expr == nil {
		return
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "<=":
			colExpr, val, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyLEByValue(val), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLEByValue(val), nil
			}
			blockFilterOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if blkMeta.IsEmpty() {
					return true, nil
				}
				return blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLEByValue(val), nil
			}
		case ">=":
			colExpr, val, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyGEByValue(val), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(val), nil
			}
			blockFilterOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if blkMeta.IsEmpty() {
					return true, nil
				}
				return blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(val), nil
			}
		case ">":
			colExpr, val, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyGTByValue(val), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGTByValue(val), nil
			}
			blockFilterOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if blkMeta.IsEmpty() {
					return true, nil
				}
				return blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGTByValue(val), nil
			}
		case "<":
			colExpr, val, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyLTByValue(val), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLTByValue(val), nil
			}
			blockFilterOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if blkMeta.IsEmpty() {
					return true, nil
				}
				return blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLTByValue(val), nil
			}
		case "prefix_eq":
			colExpr, val, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().PrefixEq(val), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixEq(val), nil
			}
			blockFilterOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if blkMeta.IsEmpty() {
					return true, nil
				}
				return blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixEq(val), nil
			}

		// case "prefix_between":
		// case "between"
		// case "prefix_in":
		// case "isnull", "is_null"
		// case "isnotnull", "is_not_null"
		case "in":
			colExpr, val, ok := mustColVecValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			vec := vector.NewVec(types.T_any.ToType())
			_ = vec.UnmarshalBinary(val)
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyIn(vec), nil
				}
			}
			if isPK {
				loadOp = loadMetadataAndBFOpFactory(fs)
			} else {
				loadOp = loadMetadataOnlyOpFactory(fs)
			}

			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyIn(vec), nil
			}
			blockFilterOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if !blkMeta.IsEmpty() && !blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyIn(vec) {
					return false, nil
				}
				if isPK {
					blkBf := bf.GetBloomFilter(uint32(blk.BlockID.Sequence()))
					blkBfIdx := index.NewEmptyBinaryFuseFilter()
					if err := index.DecodeBloomFilter(blkBfIdx, blkBf); err != nil {
						return false, err
					}
					if exist := blkBfIdx.MayContainsAny(vec); !exist {
						return false, nil
					}
				}
				return true, nil
			}
		case "=":
			colExpr, val, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().ContainsKey(val), nil
				}
			}
			if isPK {
				loadOp = loadMetadataAndBFOpFactory(fs)
			} else {
				loadOp = loadMetadataOnlyOpFactory(fs)
			}

			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().ContainsKey(val), nil
			}
			blockFilterOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if !blkMeta.IsEmpty() && !blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().ContainsKey(val) {
					return false, nil
				}
				if isPK {
					blkBf := bf.GetBloomFilter(uint32(blk.BlockID.Sequence()))
					blkBfIdx := index.NewEmptyBinaryFuseFilter()
					if err := index.DecodeBloomFilter(blkBfIdx, blkBf); err != nil {
						return false, err
					}
					exist, err := blkBfIdx.MayContainsKey(val)
					if err != nil || !exist {
						return false, err
					}
				}
				return true, nil
			}
		default:
			canCompile = false
		}
	default:
		canCompile = false
	}
	return
}

func TryFastFilterBlocks(
	snapshotTS timestamp.Timestamp,
	tableDef *plan.TableDef,
	exprs []*plan.Expr,
	snapshot *logtailreplay.PartitionState,
	uncommittedObjects []objectio.ObjectStats,
	blocks *objectio.BlockInfoSlice,
	fs fileservice.FileService,
	proc *process.Process,
) (ok bool, err error) {
	fastFilterOp, loadOp, objectFilterOp, blockFilterOp, ok := CompileFilterExprs(exprs, proc, tableDef, fs)
	if !ok {
		return false, nil
	}
	err = ExecuteBlockFilter(
		snapshotTS,
		fastFilterOp,
		loadOp,
		objectFilterOp,
		blockFilterOp,
		snapshot,
		uncommittedObjects,
		blocks,
		fs,
		proc,
	)
	return true, err
}

func ExecuteBlockFilter(
	snapshotTS timestamp.Timestamp,
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	snapshot *logtailreplay.PartitionState,
	uncommittedObjects []objectio.ObjectStats,
	blocks *objectio.BlockInfoSlice,
	fs fileservice.FileService,
	proc *process.Process,
) (err error) {
	err = ForeachSnapshotObjects(
		snapshotTS,
		func(obj logtailreplay.ObjectInfo, isCommitted bool) (err2 error) {
			var ok bool
			objStats := obj.ObjectStats
			if fastFilterOp != nil {
				if ok, err2 = fastFilterOp(objStats); err2 != nil || !ok {
					return
				}
			}
			var (
				meta objectio.ObjectMeta
				bf   objectio.BloomFilter
			)
			if loadOp != nil {
				if meta, bf, err2 = loadOp(
					proc.Ctx, objStats, meta, bf,
				); err2 != nil {
					return
				}
			}
			if objectFilterOp != nil {
				if ok, err2 = objectFilterOp(meta, bf); err2 != nil || !ok {
					return
				}
			}
			var dataMeta objectio.ObjectDataMeta
			if meta != nil {
				dataMeta = meta.MustDataMeta()
			}
			ForeachBlkInObjStatsList(false, dataMeta, func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				var ok2 bool
				if blockFilterOp != nil {
					ok2, err2 = blockFilterOp(blk, blkMeta, bf)
					if err2 != nil {
						return false
					}
					if !ok2 {
						return true
					}
				}

				blk.Sorted = obj.Sorted
				blk.EntryState = obj.EntryState
				blk.CommitTs = obj.CommitTS
				if obj.HasDeltaLoc {
					deltaLoc, commitTs, ok := snapshot.GetBockDeltaLoc(blk.BlockID)
					if ok {
						blk.DeltaLoc = deltaLoc
						blk.CommitTs = commitTs
					}
				}
				blk.PartitionNum = -1
				blocks.AppendBlockInfo(blk)
				return true
			},
				objStats,
			)
			return
		},
		snapshot,
		uncommittedObjects...,
	)
	return
}
