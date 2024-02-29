package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type LoadOp = func(obj objectio.ObjectStats) (objectio.ObjectMeta, objectio.BloomFilter, error)
type LoadOpFactory func(context.Context, fileservice.FileService) LoadOp

var loadMetadataOnlyOpFactory LoadOpFactory
var loadMetadataAndBFOpFactory LoadOpFactory

func init() {
	loadMetadataAndBFOpFactory = func(ctx context.Context, fs fileservice.FileService) LoadOp {
		return func(obj objectio.ObjectStats) (objectio.ObjectMeta, objectio.BloomFilter, error) {
			location := obj.ObjectLocation()
			objMeta, err := objectio.FastLoadObjectMeta(
				ctx, &location, false, fs,
			)
			if err != nil {
				return nil, nil, err
			}
			meta := objMeta.MustDataMeta()
			bf, err := objectio.LoadBFWithMeta(
				ctx, meta, location, fs,
			)
			if err != nil {
				return nil, nil, err
			}
			return objMeta, bf, nil
		}
	}
	loadMetadataOnlyOpFactory = func(ctx context.Context, fs fileservice.FileService) LoadOp {
		return func(obj objectio.ObjectStats) (objectio.ObjectMeta, objectio.BloomFilter, error) {
			location := obj.ObjectLocation()
			objMeta, err := objectio.FastLoadObjectMeta(
				ctx, &location, false, fs,
			)
			if err != nil {
				return nil, nil, err
			}
			return objMeta, nil, nil
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

func GenerateFilterExprOperators(
	ctx context.Context,
	expr *plan.Expr,
	proc *process.Process,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
) (
	fastCheckOp func(objectio.ObjectStats) (bool, error),
	loadOp func(objectio.ObjectStats) (objectio.ObjectMeta, objectio.BloomFilter, error),
	objectCheckOp func(objectio.ObjectMeta, objectio.BloomFilter) (bool, error),
	blockCheckOp func(objectio.BlockInfo, objectio.BlockObject, objectio.BloomFilter) (bool, error),
) {
	if expr == nil {
		return
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "<=":
			colExpr, val, ok := mustEvalColValueBinaryFunctionExpr(exprImpl, tableDef, proc)
			if !ok {
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
			loadOp = loadMetadataOnlyOpFactory(ctx, fs)
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
			loadOp = loadMetadataOnlyOpFactory(ctx, fs)
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
			loadOp = loadMetadataOnlyOpFactory(ctx, fs)
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
			loadOp = loadMetadataOnlyOpFactory(ctx, fs)
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
			loadOp = loadMetadataOnlyOpFactory(ctx, fs)
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
				loadOp = loadMetadataAndBFOpFactory(ctx, fs)
			} else {
				loadOp = loadMetadataOnlyOpFactory(ctx, fs)
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
		}
	}
	return
}
