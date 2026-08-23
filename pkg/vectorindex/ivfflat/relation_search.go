// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ivfflat

import (
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

func (idx *IvfflatSearchIndex[T]) entryQueryBytes(idxcfg vectorindex.IndexConfig, query []T) ([]byte, plan.Type, error) {
	vectorType := types.T(idxcfg.Ivfflat.VectorType)
	if vectorType == 0 {
		switch any(query).(type) {
		case []float32:
			vectorType = types.T_array_float32
		case []float64:
			vectorType = types.T_array_float64
		}
	}
	dimension := int32(len(query))
	typ := plan.Type{Id: int32(vectorType), Width: dimension}
	switch vectorType {
	case types.T_array_float32:
		if q, ok := any(query).([]float32); ok {
			return types.ArrayToBytes(q), typ, nil
		}
	case types.T_array_float64:
		if q, ok := any(query).([]float64); ok {
			return types.ArrayToBytes(q), typ, nil
		}
	case types.T_array_bf16:
		if q, ok := any(query).([]float32); ok {
			return types.ArrayToBytes(types.Float32ToBF16Slice(q)), typ, nil
		}
	case types.T_array_float16:
		if q, ok := any(query).([]float32); ok {
			return types.ArrayToBytes(types.Float32ToFloat16Slice(q)), typ, nil
		}
	case types.T_array_int8:
		if q, ok := any(query).([]float32); ok {
			return types.ArrayToBytes(quantizer.ApplyInt8(q, idx.QuantMul, idx.QuantAdd)), typ, nil
		}
	case types.T_array_uint8:
		if q, ok := any(query).([]float32); ok {
			return types.ArrayToBytes(quantizer.ApplyUint8(q, idx.QuantMul, idx.QuantAdd)), typ, nil
		}
	}
	return nil, plan.Type{}, moerr.NewInternalErrorNoCtxf(
		"ivfflat: cannot encode %T query for entries vector type %s", query, vectorType.String())
}

func (idx *IvfflatSearchIndex[T]) scanEntries(
	sqlproc *sqlexec.SqlProcess,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	query []T,
	version int64,
	centroidIDs []int64,
	includeCols []string,
	filters []*plan.Expr,
	limit uint,
) (executor.Result, error) {
	queryBytes, queryType, err := idx.entryQueryBytes(idxcfg, query)
	if err != nil {
		return executor.Result{}, err
	}
	columns := entryScanColumns(tblcfg.IncludeColumns)
	orderFlag := ivfOrderFlag(sqlproc.IndexReaderParam)
	storageTopK := canUseStorageTopK(sqlproc, centroidIDs, filters, limit)
	var filter *plan.Expr
	indexParam := &plan.IndexReaderParam{
		Limit:   ivfUint64Expr(uint64(limit)),
		OrderBy: []*plan.OrderBySpec{{Flag: orderFlag}},
	}
	if storageTopK {
		// The entries table is ordered by (version, centroid, source PK).
		// Express the complete candidate set as composite-key prefixes so both
		// range pruning and block reads enforce it before their distance heap.
		cpkeyPos := int32(len(columns))
		columns = append(columns, catalog.CPrimaryKeyColName)
		filter, err = ivfCentroidPrefixFilter(
			sqlproc.GetContext(), sqlproc.Proc.Mp(), version, centroidIDs, cpkeyPos)
		if err != nil {
			return executor.Result{}, err
		}
		entryExpr := ivfColExpr(3, queryType)
		queryExpr := &plan.Expr{
			Typ: queryType,
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_VecVal{VecVal: string(queryBytes)},
			}},
		}
		indexParam.OrderBy[0].Expr, err = ivfFuncExpr(
			sqlproc.GetContext(),
			metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)],
			entryExpr,
			queryExpr,
		)
		if err != nil {
			return executor.Result{}, err
		}
		indexParam.OrigFuncName = tblcfg.OrigFuncName
	} else {
		versionFilter, filterErr := ivfFuncExpr(sqlproc.GetContext(), "=",
			ivfColExpr(0, plan.Type{Id: int32(types.T_int64)}), ivfInt64Expr(version))
		if filterErr != nil {
			return executor.Result{}, filterErr
		}
		allFilters := make([]*plan.Expr, 0, 2+len(filters))
		allFilters = append(allFilters, versionFilter)
		if len(centroidIDs) > 0 {
			centroidFilter, centroidErr := ivfInInt64Expr(sqlproc.GetContext(), sqlproc.Proc.Mp(),
				ivfColExpr(1, plan.Type{Id: int32(types.T_int64)}), centroidIDs)
			if centroidErr != nil {
				return executor.Result{}, centroidErr
			}
			allFilters = append(allFilters, centroidFilter)
		}
		if sqlproc.IvfHasMembershipFilter {
			membershipFilter, membershipErr := ivfRuntimeMembershipExpr(
				sqlproc.GetContext(), sqlproc.IvfRuntimeFilterData,
				ivfColExpr(2, plan.Type{Id: tblcfg.PKeyType}))
			if membershipErr != nil {
				return executor.Result{}, membershipErr
			}
			allFilters = append(allFilters, membershipFilter)
		}
		allFilters = append(allFilters, filters...)
		filter, err = ivfAndExpr(sqlproc.GetContext(), allFilters...)
		if err != nil {
			return executor.Result{}, err
		}
	}
	var blockFilters []*plan.Expr
	if storageTopK {
		blockFilters = []*plan.Expr{filter}
	}
	res, err := sqlproc.RelationScanner.ScanRelation(sqlexec.RelationScanRequest{
		Schema:            tblcfg.DbName,
		Table:             tblcfg.EntriesTable,
		Columns:           columns,
		Filter:            filter,
		BlockFilters:      blockFilters,
		IndexParam:        indexParam,
		PostFilterTopOnly: !storageTopK,
		BatchTransform: func(bat *batch.Batch) error {
			batchResult := executor.Result{Batches: []*batch.Batch{bat}, Mp: sqlproc.Proc.Mp()}
			if storageTopK {
				if len(bat.Vecs) != len(columns)+1 {
					return moerr.NewInternalErrorNoCtxf(
						"ivfflat storage Top-K returned %d vectors, expected %d", len(bat.Vecs), len(columns)+1)
				}
			} else {
				if transformErr := appendEntryDistances(sqlproc, &batchResult, queryBytes, queryType,
					metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)]); transformErr != nil {
					return transformErr
				}
			}
			if transformErr := idx.filterEntryDistanceRange(&batchResult, sqlproc.IndexReaderParam.GetDistRange(),
				tblcfg.OrigFuncName, metric.MetricType(idxcfg.Ivfflat.Metric)); transformErr != nil {
				return transformErr
			}
			// Distance and exact filters have consumed the high-width entry vector.
			// Keep a typed empty slot for batch alignment, but do not retain/copy
			// embeddings in the bounded Top-K accumulator.
			entryType := *bat.Vecs[3].GetType()
			bat.Vecs[3].Free(sqlproc.Proc.Mp())
			bat.Vecs[3] = vector.NewVec(entryType)
			return nil
		},
	})
	if err != nil {
		return res, err
	}
	for _, bat := range res.Batches {
		if len(bat.Vecs) != len(columns)+1 {
			res.Close()
			return executor.Result{}, moerr.NewInternalErrorNoCtxf(
				"ivfflat entries reader returned %d vectors, expected %d", len(bat.Vecs), len(columns)+1)
		}
		entryVec := bat.Vecs[3]
		distVec := bat.Vecs[len(columns)]
		out := make([]*vector.Vector, 2+len(includeCols))
		out[0], out[1] = bat.Vecs[2], distVec
		for i, include := range includeCols {
			for configuredPos, configured := range tblcfg.IncludeColumns {
				if include == configured {
					out[2+i] = bat.Vecs[4+configuredPos]
					break
				}
			}
			if out[2+i] == nil {
				res.Close()
				return executor.Result{}, moerr.NewInternalErrorNoCtxf("ivfflat include column %q not found in entries result", include)
			}
		}
		entryVec.Free(res.Mp)
		// Version, centroid id, and unrequested INCLUDE vectors are no longer
		// reachable from the returned candidate batch.
		for i, vec := range bat.Vecs[:len(columns)] {
			if i == 2 || i == 3 {
				continue
			}
			keep := false
			for _, kept := range out[2:] {
				if vec == kept {
					keep = true
					break
				}
			}
			if !keep {
				vec.Free(res.Mp)
			}
		}
		bat.Vecs = out
		bat.Attrs = append([]string{catalog.SystemSI_IVFFLAT_TblCol_Entries_pk, "vec_dist"}, includeCols...)
	}
	return res, nil
}

func canUseStorageTopK(
	sqlproc *sqlexec.SqlProcess,
	centroidIDs []int64,
	filters []*plan.Expr,
	limit uint,
) bool {
	// Membership, user predicates, and bounded ranges can remove a row after
	// storage ranking. They must stay on the filter-then-local-Top-K path.
	if sqlproc == nil || sqlproc.IvfHasMembershipFilter || len(centroidIDs) == 0 || len(filters) != 0 || limit == 0 {
		return false
	}
	distRange := sqlproc.IndexReaderParam.GetDistRange()
	return distRange == nil ||
		distRange.LowerBoundType == plan.BoundType_UNBOUNDED &&
			distRange.UpperBoundType == plan.BoundType_UNBOUNDED
}

func ivfOrderFlag(param *plan.IndexReaderParam) plan.OrderBySpec_OrderByFlag {
	if param != nil && len(param.OrderBy) > 0 && param.OrderBy[0] != nil {
		return param.OrderBy[0].Flag
	}
	return plan.OrderBySpec_ASC
}

func ivfCentroidPrefixFilter(
	ctx context.Context,
	mp *mpool.MPool,
	version int64,
	centroidIDs []int64,
	cpkeyPos int32,
) (*plan.Expr, error) {
	if len(centroidIDs) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("IVF storage Top-K requires at least one centroid")
	}
	if len(centroidIDs) > int(^uint32(0)>>1) {
		return nil, moerr.NewInvalidInputNoCtx("IVF centroid prefix set is too large")
	}

	versionVec, err := vector.NewConstFixed(types.T_int64.ToType(), version, len(centroidIDs), mp)
	if err != nil {
		return nil, err
	}
	defer versionVec.Free(mp)
	centroidVec := vector.NewVec(types.T_int64.ToType())
	defer centroidVec.Free(mp)
	if err = vector.AppendFixedList(centroidVec, centroidIDs, nil, mp); err != nil {
		return nil, err
	}
	versionEncoder, err := function.NewSerialValueEncoder(versionVec)
	if err != nil {
		return nil, err
	}
	centroidEncoder, err := function.NewSerialValueEncoder(centroidVec)
	if err != nil {
		return nil, err
	}

	cpkeyType := types.T_varchar.ToType()
	cpkeyType.Width = types.MaxVarcharLen
	cpkeyType.Charset = types.CharsetBinary
	prefixVec := vector.NewVec(cpkeyType)
	defer prefixVec.Free(mp)
	packer := types.NewPacker()
	defer packer.Close()
	for row := range centroidIDs {
		packer.Reset()
		versionEncoder(versionVec, row, packer)
		centroidEncoder(centroidVec, row, packer)
		if err = vector.AppendBytes(prefixVec, packer.Bytes(), false, mp); err != nil {
			return nil, err
		}
	}
	data, err := prefixVec.MarshalBinary()
	if err != nil {
		return nil, err
	}
	cpkeyPlanType := plan.Type{
		Id:      int32(types.T_varchar),
		Width:   types.MaxVarcharLen,
		Charset: uint32(types.CharsetBinary),
	}
	left := &plan.Expr{
		Typ: cpkeyPlanType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			Name:   catalog.CPrimaryKeyColName,
			ColPos: cpkeyPos,
		}},
	}
	right := &plan.Expr{
		Typ: cpkeyPlanType,
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
			Len:  int32(len(centroidIDs)),
			Data: data,
		}},
	}
	return ivfFuncExpr(ctx, function.PrefixInFunctionName, left, right)
}

func entryScanColumns(includeColumns []string) []string {
	columns := []string{
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
	}
	for _, col := range includeColumns {
		columns = append(columns, catalog.SystemSI_IVFFLAT_IncludeColPrefix+col)
	}
	return columns
}

func ivfRuntimeMembershipExpr(ctx context.Context, data []byte, left *plan.Expr) (*plan.Expr, error) {
	if len(data) == 0 || left == nil {
		return nil, moerr.NewInvalidInputNoCtx("IVF runtime membership filter is empty")
	}
	keyVec := new(vector.Vector)
	if err := keyVec.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	if keyVec.Length() == 0 {
		return nil, moerr.NewInvalidInputNoCtx("IVF runtime membership key set is empty")
	}
	if keyVec.Length() > int(^uint32(0)>>1) {
		return nil, moerr.NewInvalidInputNoCtx("IVF runtime membership key set is too large")
	}
	right := &plan.Expr{
		Typ: left.Typ,
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
			Len:  int32(keyVec.Length()),
			Data: append([]byte(nil), data...),
		}},
	}
	return ivfFuncExpr(ctx, function.InFunctionName, left, right)
}

func (idx *IvfflatSearchIndex[T]) filterEntryDistanceRange(
	res *executor.Result,
	distRange *plan.DistRange,
	origFuncName string,
	metricType metric.MetricType,
) error {
	if distRange == nil || res == nil {
		return nil
	}
	lower, hasLower, err := vectorDistanceBound(distRange.LowerBoundType, distRange.LowerBound)
	if err != nil {
		return err
	}
	upper, hasUpper, err := vectorDistanceBound(distRange.UpperBoundType, distRange.UpperBound)
	if err != nil {
		return err
	}
	if !hasLower && !hasUpper {
		return nil
	}

	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 || len(bat.Vecs) == 0 {
			continue
		}
		distVec := bat.Vecs[len(bat.Vecs)-1]
		sels := make([]int64, 0, bat.RowCount())
		for row := 0; row < bat.RowCount(); row++ {
			if distVec.IsNull(uint64(row)) {
				continue
			}
			distance := vector.GetFixedAtNoTypeCheck[float64](distVec, row)
			distance = idx.scoreFromQuantized(distance, origFuncName, metricType)
			if vectorDistanceInRange(distance, distRange, lower, hasLower, upper, hasUpper) {
				sels = append(sels, int64(row))
			}
		}
		switch {
		case len(sels) == bat.RowCount():
		case len(sels) == 0:
			bat.CleanOnlyData()
		default:
			bat.Shrink(sels, false)
		}
	}
	return nil
}

func vectorDistanceBound(boundType plan.BoundType, expr *plan.Expr) (float64, bool, error) {
	switch boundType {
	case plan.BoundType_UNBOUNDED:
		return 0, false, nil
	case plan.BoundType_INCLUSIVE, plan.BoundType_EXCLUSIVE:
		value, ok := plan.GetLiteralFloat64(expr)
		if !ok {
			return 0, false, moerr.NewInvalidInputNoCtx("IVF distance bound did not fold to a numeric literal")
		}
		return value, true, nil
	default:
		return 0, false, moerr.NewInvalidInputNoCtxf("invalid IVF distance bound type %d", boundType)
	}
}

func vectorDistanceInRange(
	distance float64,
	distRange *plan.DistRange,
	lower float64,
	hasLower bool,
	upper float64,
	hasUpper bool,
) bool {
	if math.IsNaN(distance) || hasLower && math.IsNaN(lower) || hasUpper && math.IsNaN(upper) {
		return false
	}
	if hasLower {
		if distRange.LowerBoundType == plan.BoundType_INCLUSIVE {
			if distance < lower {
				return false
			}
		} else if distance <= lower {
			return false
		}
	}
	if hasUpper {
		if distRange.UpperBoundType == plan.BoundType_INCLUSIVE {
			if distance > upper {
				return false
			}
		} else if distance >= upper {
			return false
		}
	}
	return true
}

func appendEntryDistances(
	sqlproc *sqlexec.SqlProcess,
	res *executor.Result,
	queryBytes []byte,
	queryType plan.Type,
	distanceFunction string,
) error {
	queryPhysicalType := types.New(types.T(queryType.Id), queryType.Width, queryType.Scale)
	for _, bat := range res.Batches {
		if len(bat.Vecs) < 4 || bat.RowCount() == 0 {
			continue
		}
		queryVec, err := vector.NewConstBytes(queryPhysicalType, queryBytes, bat.RowCount(), res.Mp)
		if err != nil {
			return err
		}
		fn, err := function.GetFunctionByName(sqlproc.GetContext(), distanceFunction,
			[]types.Type{*bat.Vecs[3].GetType(), queryPhysicalType})
		if err != nil {
			queryVec.Free(res.Mp)
			return err
		}
		distVec, err := function.RunFunctionDirectly(
			sqlproc.Proc,
			fn.GetEncodedOverloadID(),
			[]*vector.Vector{bat.Vecs[3], queryVec},
			bat.RowCount(),
		)
		queryVec.Free(res.Mp)
		if err != nil {
			return err
		}
		bat.Vecs = append(bat.Vecs, distVec)
	}
	return nil
}
