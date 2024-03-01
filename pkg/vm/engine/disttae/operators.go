package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type FastCheckOp func(objectio.ObjectStats) (bool, error)
type LoadOp = func(
	context.Context, objectio.ObjectStats, objectio.ObjectMeta, objectio.BloomFilter,
) (objectio.ObjectMeta, objectio.BloomFilter, error)
type ObjectCheckOp func(objectio.ObjectMeta, objectio.BloomFilter) (bool, error)
type BlockCheckOp func(objectio.BlockInfo, objectio.BlockObject, objectio.BloomFilter) (bool, error)
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
	if !isPK && tableDef.ClusterBy != nil && tableDef.ClusterBy.CompCbkeyCol.ColId == uint64(expr.Col.ColPos) {
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

func mustEvalColValueBinaryFunctionExpr(
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
	fastCheckOp FastCheckOp,
	loadOp LoadOp,
	objectCheckOp ObjectCheckOp,
	blockCheckOp BlockCheckOp,
	canCompile bool,
) {
	canCompile = true
	if len(exprs) == 0 {
		return
	}
	if len(exprs) == 1 {
		return CompileFilterExpr(exprs[0], proc, tableDef, fs)
	}
	ops1 := make([]FastCheckOp, len(exprs))
	ops2 := make([]LoadOp, len(exprs))
	ops3 := make([]ObjectCheckOp, len(exprs))
	ops4 := make([]BlockCheckOp, len(exprs))

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
	fastCheckOp = func(obj objectio.ObjectStats) (bool, error) {
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
	objectCheckOp = func(meta objectio.ObjectMeta, bf objectio.BloomFilter) (bool, error) {
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
	blockCheckOp = func(
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
	fastCheckOp FastCheckOp,
	loadOp LoadOp,
	objectCheckOp ObjectCheckOp,
	blockCheckOp BlockCheckOp,
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
			colExpr, val, ok := mustEvalColValueBinaryFunctionExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastCheckOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyLE2(val), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectCheckOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLE2(val), nil
			}
			blockCheckOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if blkMeta.IsEmpty() {
					return true, nil
				}
				return blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLE2(val), nil
			}
		case ">=":
			colExpr, val, ok := mustEvalColValueBinaryFunctionExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastCheckOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyGE2(val), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectCheckOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGE2(val), nil
			}
			blockCheckOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if blkMeta.IsEmpty() {
					return true, nil
				}
				return blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGE2(val), nil
			}
		case ">":
			colExpr, val, ok := mustEvalColValueBinaryFunctionExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastCheckOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyGT2(val), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectCheckOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGT2(val), nil
			}
			blockCheckOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if blkMeta.IsEmpty() {
					return true, nil
				}
				return blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGT2(val), nil
			}
		case "<":
			colExpr, val, ok := mustEvalColValueBinaryFunctionExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastCheckOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyLT2(val), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectCheckOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLT2(val), nil
			}
			blockCheckOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if blkMeta.IsEmpty() {
					return true, nil
				}
				return blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLT2(val), nil
			}
		case "prefix_eq":
			colExpr, val, ok := mustEvalColValueBinaryFunctionExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastCheckOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().PrefixEq(val), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			colDef := tableDef.Cols[colExpr.Col.ColPos]
			seqNum := colDef.Seqnum
			objectCheckOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixEq(val), nil
			}
			blockCheckOp = func(
				blk objectio.BlockInfo, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, error) {
				if blkMeta.IsEmpty() {
					return true, nil
				}
				return blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixEq(val), nil
			}
		// case "prefix_between":
		// case "between"
		// case "in":
		// case "prefix_in":
		// case "isnull", "is_null"
		// case "isnotnull", "is_not_null"
		case "=":
			colExpr, val, ok := mustEvalColValueBinaryFunctionExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			isPK, isCluster := isClusterOrPKFromColExpr(colExpr, tableDef)
			if isPK || isCluster {
				fastCheckOp = func(obj objectio.ObjectStats) (bool, error) {
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
			objectCheckOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isCluster || isPK {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().ContainsKey(val), nil
			}
			blockCheckOp = func(
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
	fastCheckOp, loadOp, objectCheckOp, blockCheckOp, ok := CompileFilterExprs(exprs, proc, tableDef, fs)
	if !ok {
		return false, nil
	}
	err = ExecuteBlockFilter(
		snapshotTS,
		fastCheckOp,
		loadOp,
		objectCheckOp,
		blockCheckOp,
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
	fastCheckOp FastCheckOp,
	loadOp LoadOp,
	objectCheckOp ObjectCheckOp,
	blockCheckOp BlockCheckOp,
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
			if fastCheckOp != nil {
				if ok, err2 = fastCheckOp(objStats); err2 != nil || !ok {
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
			if objectCheckOp != nil {
				if ok, err2 = objectCheckOp(meta, bf); err2 != nil || !ok {
					return
				}
			}
			ForeachBlkInObjStatsList(false, meta.MustDataMeta(), func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				var ok2 bool
				if blockCheckOp != nil {
					ok2, err2 = blockCheckOp(blk, blkMeta, bf)
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
