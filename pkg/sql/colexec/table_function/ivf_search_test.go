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

package table_function

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type ivfSearchTestCase struct {
	arg  *TableFunction
	proc *process.Process
}

var (
	ivfsearchdefaultAttrs = []string{"pkid", "score"}

	ivfsearchdefaultColdefs = []*plan.ColDef{
		// row_id type should be same as index type
		{
			Name: "pkid",
			Typ: plan.Type{
				Id:          int32(types.T_int64),
				NotNullable: false,
			},
		},
		{
			Name: "score",
			Typ: plan.Type{
				Id:          int32(types.T_float64),
				NotNullable: false,
				Width:       8,
			},
		},
	}
)

func newIvfSearchTestCase(t *testing.T, m *mpool.MPool, attrs []string, param string) ivfSearchTestCase {
	proc := testutil.NewProcessWithMPool(t, "", m)
	colDefs := make([]*plan.ColDef, len(attrs))
	for i := range attrs {
		for j := range ivfsearchdefaultColdefs {
			if attrs[i] == ivfsearchdefaultColdefs[j].Name {
				colDefs[i] = ivfsearchdefaultColdefs[j]
				break
			}
		}
	}

	ret := ivfSearchTestCase{
		proc: proc,
		arg: &TableFunction{
			Attrs:    attrs,
			Rets:     colDefs,
			FuncName: "ivf_search",
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			Params: []byte(param),
		},
	}
	return ret
}

func mock_ivf_runSql(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
	proc := sqlproc.Proc
	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{}}, nil
}

func mockVersion(sqlproc *sqlexec.SqlProcess, tblcfg vectorindex.IndexTableConfig) (int64, error) {
	return 0, nil
}

type MockIvfSearch[T types.RealNumbers] struct {
	Idxcfg   vectorindex.IndexConfig
	Tblcfg   vectorindex.IndexTableConfig
	searchFn func(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error)
}

func (m *MockIvfSearch[T]) Search(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	if m.searchFn != nil {
		return m.searchFn(sqlproc, query, rt)
	}
	return []any{int64(1)}, []float64{2.0}, nil
}

func (m *MockIvfSearch[T]) Destroy() {
}

func (m *MockIvfSearch[T]) Load(*sqlexec.SqlProcess) error {
	//time.Sleep(6 * time.Second)
	return nil
}

func (m *MockIvfSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {
	return nil
}

func newMockIvfAlgoFn(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (cache.VectorIndexSearchIf, error) {
	return &MockIvfSearch[float32]{Idxcfg: idxcfg, Tblcfg: tblcfg}, nil
}

func makeConstInputExprsIvfSearchWithConfig(tblcfg string) []*plan.Expr {
	return []*plan.Expr{
		{
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: 512,
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_Sval{
						Sval: tblcfg,
					},
				},
			},
		},
		plan2.MakePlan2Vecf32ConstExprWithType("[0,1,2]", 3),
	}
}

func TestIvfSearch(t *testing.T) {

	newIvfAlgo = newMockIvfAlgoFn
	getVersion = mockVersion

	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"
	ut := newIvfSearchTestCase(t, mpool.MustNewZero(), ivfsearchdefaultAttrs, param)

	inbat := makeBatchIvfSearch(ut.proc)

	ut.arg.Args = makeConstInputExprsIvfSearch()

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	// first call receive data
	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)

	require.Equal(t, result.Status, vm.ExecNext)

	err = ut.arg.ctr.state.end(ut.arg, ut.proc)
	require.Nil(t, err)

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

var failedivfsearchparam []string = []string{"{",
	"{\"op_type\": \"vector_xxx_ops\"}",
	"{\"op_type\": \"vector_l2_ops\", \"lists\":\"\"}",
	"{\"op_type\": \"vector_l2_ops\", \"lists\":\"notnumber\"}",
}

func TestIvfSearchParamFail(t *testing.T) {

	newIvfAlgo = newMockIvfAlgoFn
	getVersion = mockVersion

	for _, param := range failedivfsearchparam {
		ut := newIvfSearchTestCase(t, mpool.MustNewZero(), ivfsearchdefaultAttrs, param)

		inbat := makeBatchIvfSearch(ut.proc)

		ut.arg.Args = makeConstInputExprsIvfSearch()

		// Prepare
		err := ut.arg.Prepare(ut.proc)
		require.Nil(t, err)

		for i := range ut.arg.ctr.executorsForArgs {
			ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
			require.Nil(t, err)
		}

		// start
		err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
		require.NotNil(t, err)
	}

	/*
		// first call receive data
		result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
		require.Nil(t, err)

		require.Equal(t, result.Status, vm.ExecStop)

		err = ut.arg.ctr.state.end(ut.arg, ut.proc)
		require.Nil(t, err)

		// reset
		ut.arg.ctr.state.reset(ut.arg, ut.proc)

		// free
		ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
	*/
}

func TestIvfSearchIndexTableConfigFail(t *testing.T) {

	ivf_runSql = mock_ivf_runSql
	getVersion = mockVersion
	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"

	ut := newIvfSearchTestCase(t, mpool.MustNewZero(), ivfsearchdefaultAttrs, param)
	failbatches := makeBatchIvfSearchFail(ut.proc)
	for _, b := range failbatches {

		inbat := b.bat
		ut.arg.Args = b.args

		// Prepare
		err := ut.arg.Prepare(ut.proc)
		require.Nil(t, err)

		for i := range ut.arg.ctr.executorsForArgs {
			ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
			require.Nil(t, err)
		}

		// start
		err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
		require.NotNil(t, err)
	}

	/*
	   // first call receive data
	   result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	   require.Nil(t, err)

	   require.Equal(t, result.Status, vm.ExecStop)

	   err = ut.arg.ctr.state.end(ut.arg, ut.proc)
	   require.Nil(t, err)

	   // reset
	   ut.arg.ctr.state.reset(ut.arg, ut.proc)

	   // free
	   ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
	*/
}

func makeConstInputExprsIvfSearch() []*plan.Expr {

	tblcfg := fmt.Sprintf(`{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index", "entries":"__entries", "parttype": %d}`, int32(types.T_array_float32))
	return makeConstInputExprsIvfSearchWithConfig(tblcfg)
}
func makeBatchIvfSearch(proc *process.Process) *batch.Batch {

	bat := batch.NewWithSize(2)

	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))     // index table config
	bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 3, 0)) // float32 array [3]float32

	tblcfg := fmt.Sprintf(`{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index", "entries":"__entries", "parttype": %d}`, int32(types.T_array_float32))
	vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())

	v := []float32{0, 1, 2}
	vector.AppendArray(bat.Vecs[1], v, false, proc.Mp())

	bat.SetRowCount(1)
	return bat

}

func TestIvfSearchCallReturnsIncludeColumnsAndLazyFetchesMoreRounds(t *testing.T) {
	oldNewIvfAlgo := newIvfAlgo
	oldGetVersion := getVersion
	defer func() {
		newIvfAlgo = oldNewIvfAlgo
		getVersion = oldGetVersion
	}()

	mock := &MockIvfSearch[float32]{}
	mock.searchFn = func(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
		if rt.SearchCursor != nil {
			if len(rt.SearchCursor.RankedCentroidIDs) == 0 {
				rt.SearchCursor.RankedCentroidIDs = []int64{11, 22}
			}
			if rt.SearchCursor.CurrentBucketCount == 0 {
				rt.SearchCursor.CurrentBucketCount = 1
			}
			rt.SearchCursor.Round++
			rt.SearchCursor.Exhausted = rt.SearchCursor.NextBucketOffset+rt.SearchCursor.CurrentBucketCount >= uint(len(rt.SearchCursor.RankedCentroidIDs))
		}
		if rt.IncludeResult != nil {
			rt.IncludeResult.ColNames = []string{"rank"}
			rt.IncludeResult.Data = map[string][]any{"rank": nil}
		}

		if rt.SearchCursor.Round == 1 {
			rt.IncludeResult.Data["rank"] = []any{int32(7)}
			return []any{int64(1)}, []float64{2.0}, nil
		}
		rt.IncludeResult.Data["rank"] = []any{int32(7), int32(9)}
		return []any{int64(1), int64(2)}, []float64{2.0, 3.0}, nil
	}

	newIvfAlgo = func(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (cache.VectorIndexSearchIf, error) {
		return mock, nil
	}
	getVersion = mockVersion

	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"
	attrs := []string{"pkid", "score", catalog.SystemSI_IVFFLAT_IncludeColPrefix + "rank"}
	ut := newIvfSearchTestCase(t, mpool.MustNewZero(), attrs, param)
	ut.arg.Rets = []*plan.ColDef{
		{Name: "pkid", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "score", Typ: plan.Type{Id: int32(types.T_float64)}},
		{Name: catalog.SystemSI_IVFFLAT_IncludeColPrefix + "rank", Typ: plan.Type{Id: int32(types.T_int32)}},
	}

	tblcfg := fmt.Sprintf(
		`{"db":"db","src":"src","metadata":"__metadata","index":"__index_include_rounds","entries":"__entries","parttype":%d,"include_columns":["rank"],"include_column_types":[%d]}`,
		int32(types.T_array_float32),
		int32(types.T_int32),
	)
	inbat := makeBatchIvfSearch(ut.proc)
	ut.arg.Args = makeConstInputExprsIvfSearchWithConfig(tblcfg)
	ut.arg.Args = append(
		ut.arg.Args,
		plan2.MakePlan2StringConstExprWithType(""),
		plan2.MakePlan2Uint64ConstExprWithType(2),
		plan2.MakePlan2Uint64ConstExprWithType(1),
	)

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)

	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, 2, result.Batch.RowCount())
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
	require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 1))
	require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[2], 0))
	require.Equal(t, int32(9), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[2], 1))
}

func TestIvfSearchStartReadsOptionalQueryScopedArgs(t *testing.T) {
	oldNewIvfAlgo := newIvfAlgo
	oldGetVersion := getVersion
	defer func() {
		newIvfAlgo = oldNewIvfAlgo
		getVersion = oldGetVersion
	}()

	mock := &MockIvfSearch[float32]{}
	mock.searchFn = func(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
		require.Equal(t, "`__mo_index_include_category` >= 20", rt.PushdownFilterSQL)
		require.Equal(t, uint(12), rt.SearchRoundLimit)
		require.Equal(t, uint(10), rt.BucketExpandStep)
		return []any{int64(1)}, []float64{2.0}, nil
	}

	newIvfAlgo = func(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (cache.VectorIndexSearchIf, error) {
		return mock, nil
	}
	getVersion = mockVersion

	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"
	ut := newIvfSearchTestCase(t, mpool.MustNewZero(), ivfsearchdefaultAttrs, param)
	inbat := makeBatchIvfSearch(ut.proc)

	ut.arg.Args = append(
		makeConstInputExprsIvfSearch(),
		plan2.MakePlan2StringConstExprWithType("`__mo_index_include_category` >= 20"),
		plan2.MakePlan2Uint64ConstExprWithType(12),
		plan2.MakePlan2Uint64ConstExprWithType(10),
	)

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)
}

func TestIvfSearchStartKeepsLegacyRuntimePathWithoutIncludeArgs(t *testing.T) {
	oldNewIvfAlgo := newIvfAlgo
	oldGetVersion := getVersion
	defer func() {
		newIvfAlgo = oldNewIvfAlgo
		getVersion = oldGetVersion
	}()

	mock := &MockIvfSearch[float32]{}
	mock.searchFn = func(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
		require.Nil(t, rt.IncludeResult)
		require.Nil(t, rt.RequestedIncludeColumns)
		require.Empty(t, rt.PushdownFilterSQL)
		require.Zero(t, rt.SearchRoundLimit)
		require.Zero(t, rt.BucketExpandStep)
		require.Nil(t, rt.SearchCursor)
		return []any{int64(1)}, []float64{1.0}, nil
	}

	newIvfAlgo = func(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (cache.VectorIndexSearchIf, error) {
		return mock, nil
	}
	getVersion = mockVersion
	cache.Cache = cache.NewVectorIndexCache()

	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"
	ut := newIvfSearchTestCase(t, mpool.MustNewZero(), ivfsearchdefaultAttrs, param)
	inbat := makeBatchIvfSearch(ut.proc)

	ut.arg.Args = makeConstInputExprsIvfSearchWithConfig(
		fmt.Sprintf(`{"db":"db","src":"src","metadata":"__metadata","index":"__index_legacy_runtime_path","entries":"__entries","parttype": %d}`, int32(types.T_array_float32)),
	)
	ut.arg.Limit = plan2.MakePlan2Uint64ConstExprWithType(3)
	ut.arg.IndexReaderParam = &plan.IndexReaderParam{
		Limit: plan2.MakePlan2Uint64ConstExprWithType(12),
	}

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)
}

func TestIvfSearchStartResetsRoundStatePerNthRow(t *testing.T) {
	oldNewIvfAlgo := newIvfAlgo
	oldGetVersion := getVersion
	defer func() {
		newIvfAlgo = oldNewIvfAlgo
		getVersion = oldGetVersion
	}()

	type searchCall struct {
		query            []float32
		searchRoundLimit uint
		nextBucketOffset uint
		currentBucketCnt uint
	}
	var calls []searchCall

	mock := &MockIvfSearch[float32]{}
	mock.searchFn = func(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
		q := append([]float32(nil), query.([]float32)...)
		call := searchCall{query: q, searchRoundLimit: rt.SearchRoundLimit}
		if rt.SearchCursor != nil {
			call.nextBucketOffset = rt.SearchCursor.NextBucketOffset
			call.currentBucketCnt = rt.SearchCursor.CurrentBucketCount
			if len(rt.SearchCursor.RankedCentroidIDs) == 0 {
				rt.SearchCursor.RankedCentroidIDs = []int64{11, 22, 33}
			}
			if rt.SearchCursor.CurrentBucketCount == 0 {
				rt.SearchCursor.CurrentBucketCount = 1
			}
			rt.SearchCursor.Round++
			rt.SearchCursor.Exhausted = rt.SearchCursor.NextBucketOffset+rt.SearchCursor.CurrentBucketCount >= uint(len(rt.SearchCursor.RankedCentroidIDs))
		}
		calls = append(calls, call)

		if len(q) > 0 && q[0] == 0 {
			if rt.SearchCursor != nil && rt.SearchCursor.Round == 1 {
				return []any{}, []float64{}, nil
			}
			return []any{int64(1)}, []float64{1.0}, nil
		}
		return []any{int64(2)}, []float64{2.0}, nil
	}

	newIvfAlgo = func(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (cache.VectorIndexSearchIf, error) {
		return mock, nil
	}
	getVersion = mockVersion
	cache.Cache = cache.NewVectorIndexCache()

	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"
	ut := newIvfSearchTestCase(t, mpool.MustNewZero(), ivfsearchdefaultAttrs, param)
	inbat := makeBatchIvfSearch(ut.proc)
	ut.arg.Args = append(
		makeConstInputExprsIvfSearchWithConfig(
			fmt.Sprintf(`{"db":"db","src":"src","metadata":"__metadata","index":"__index_nthrow_reset","entries":"__entries","parttype": %d}`, int32(types.T_array_float32)),
		),
		plan2.MakePlan2StringConstExprWithType(""),
		plan2.MakePlan2Uint64ConstExprWithType(2),
		plan2.MakePlan2Uint64ConstExprWithType(1),
	)

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	queryVec := vector.NewVec(types.New(types.T_array_float32, 3, 0))
	require.NoError(t, vector.AppendArray(queryVec, []float32{0, 1, 2}, false, ut.proc.Mp()))
	require.NoError(t, vector.AppendArray(queryVec, []float32{9, 9, 9}, false, ut.proc.Mp()))
	ut.arg.ctr.argVecs[1] = queryVec

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)
	_, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 1, nil)
	require.NoError(t, err)

	require.Len(t, calls, 3)
	require.Equal(t, []float32{0, 1, 2}, calls[0].query)
	require.Equal(t, uint(2), calls[0].searchRoundLimit)
	require.Equal(t, uint(0), calls[0].nextBucketOffset)
	require.Equal(t, uint(0), calls[0].currentBucketCnt)

	require.Equal(t, []float32{0, 1, 2}, calls[1].query)
	require.Equal(t, uint(2), calls[1].searchRoundLimit)
	require.Equal(t, uint(1), calls[1].nextBucketOffset)
	require.Equal(t, uint(1), calls[1].currentBucketCnt)

	require.Equal(t, []float32{9, 9, 9}, calls[2].query)
	require.Equal(t, uint(2), calls[2].searchRoundLimit)
	require.Equal(t, uint(0), calls[2].nextBucketOffset)
	require.Equal(t, uint(0), calls[2].currentBucketCnt)
}

func TestIvfSearchStartFailsWhenIncludeDataMisaligns(t *testing.T) {
	oldNewIvfAlgo := newIvfAlgo
	oldGetVersion := getVersion
	defer func() {
		newIvfAlgo = oldNewIvfAlgo
		getVersion = oldGetVersion
	}()

	mock := &MockIvfSearch[float32]{}
	mock.searchFn = func(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
		rt.IncludeResult.ColNames = []string{"rank"}
		rt.IncludeResult.Data = map[string][]any{"rank": nil}
		return []any{int64(1)}, []float64{2.0}, nil
	}

	newIvfAlgo = func(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (cache.VectorIndexSearchIf, error) {
		return mock, nil
	}
	getVersion = mockVersion

	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"
	attrs := []string{"pkid", "score", catalog.SystemSI_IVFFLAT_IncludeColPrefix + "rank"}
	ut := newIvfSearchTestCase(t, mpool.MustNewZero(), attrs, param)
	ut.arg.Rets = []*plan.ColDef{
		{Name: "pkid", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "score", Typ: plan.Type{Id: int32(types.T_float64)}},
		{Name: catalog.SystemSI_IVFFLAT_IncludeColPrefix + "rank", Typ: plan.Type{Id: int32(types.T_int32)}},
	}
	tblcfg := fmt.Sprintf(
		`{"db":"db","src":"src","metadata":"__metadata","index":"__index_include_mismatch","entries":"__entries","parttype":%d,"include_columns":["rank"],"include_column_types":[%d]}`,
		int32(types.T_array_float32),
		int32(types.T_int32),
	)
	inbat := makeBatchIvfSearch(ut.proc)
	ut.arg.Args = makeConstInputExprsIvfSearchWithConfig(tblcfg)

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "include data length mismatch")
}

func TestIvfSearchCallStopsAfterTooManyEmptyRounds(t *testing.T) {
	oldNewIvfAlgo := newIvfAlgo
	oldGetVersion := getVersion
	defer func() {
		newIvfAlgo = oldNewIvfAlgo
		getVersion = oldGetVersion
	}()

	mock := &MockIvfSearch[float32]{}
	mock.searchFn = func(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
		if rt.SearchCursor != nil {
			if len(rt.SearchCursor.RankedCentroidIDs) == 0 {
				rt.SearchCursor.RankedCentroidIDs = []int64{11, 22, 33}
			}
			if rt.SearchCursor.CurrentBucketCount == 0 {
				rt.SearchCursor.CurrentBucketCount = 1
			}
			rt.SearchCursor.Round++
			rt.SearchCursor.Exhausted = rt.SearchCursor.NextBucketOffset+rt.SearchCursor.CurrentBucketCount >= uint(len(rt.SearchCursor.RankedCentroidIDs))
		}
		return []any{}, []float64{}, nil
	}

	newIvfAlgo = func(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (cache.VectorIndexSearchIf, error) {
		return mock, nil
	}
	getVersion = mockVersion

	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"
	ut := newIvfSearchTestCase(t, mpool.MustNewZero(), ivfsearchdefaultAttrs, param)
	inbat := makeBatchIvfSearch(ut.proc)
	ut.arg.Args = makeConstInputExprsIvfSearchWithConfig(
		fmt.Sprintf(`{"db":"db","src":"src","metadata":"__metadata","index":"__index_empty_rounds","entries":"__entries","parttype": %d}`, int32(types.T_array_float32)),
	)
	ut.arg.Args = append(
		ut.arg.Args,
		plan2.MakePlan2StringConstExprWithType(""),
		plan2.MakePlan2Uint64ConstExprWithType(1),
		plan2.MakePlan2Uint64ConstExprWithType(1),
	)

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)

	state := ut.arg.ctr.state.(*ivfSearchState)
	result, err := state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.CancelResult.Status, result.Status)
	require.NotNil(t, state.cursor)
	require.True(t, state.cursor.Exhausted)
}

func TestIvfSearchCallStopsOnceLimitRowsAreBuffered(t *testing.T) {
	oldNewIvfAlgo := newIvfAlgo
	oldGetVersion := getVersion
	defer func() {
		newIvfAlgo = oldNewIvfAlgo
		getVersion = oldGetVersion
	}()

	var calls int
	mock := &MockIvfSearch[float32]{}
	mock.searchFn = func(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
		calls++
		if rt.SearchCursor != nil && len(rt.SearchCursor.RankedCentroidIDs) == 0 {
			rt.SearchCursor.RankedCentroidIDs = []int64{11, 22, 33}
		}
		return []any{int64(1), int64(2)}, []float64{1.0, 2.0}, nil
	}

	newIvfAlgo = func(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (cache.VectorIndexSearchIf, error) {
		return mock, nil
	}
	getVersion = mockVersion
	cache.Cache = cache.NewVectorIndexCache()

	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"
	ut := newIvfSearchTestCase(t, mpool.MustNewZero(), ivfsearchdefaultAttrs, param)
	inbat := makeBatchIvfSearch(ut.proc)
	ut.arg.Args = append(
		makeConstInputExprsIvfSearchWithConfig(
			fmt.Sprintf(`{"db":"db","src":"src","metadata":"__metadata","index":"__index_limit_stop","entries":"__entries","parttype": %d}`, int32(types.T_array_float32)),
		),
		plan2.MakePlan2StringConstExprWithType(""),
		plan2.MakePlan2Uint64ConstExprWithType(2),
		plan2.MakePlan2Uint64ConstExprWithType(1),
	)
	ut.arg.Limit = plan2.MakePlan2Uint64ConstExprWithType(2)

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)

	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, 2, result.Batch.RowCount())
	require.Equal(t, 1, calls)
}

func TestIvfSearchPreparePrefersLargerIndexReaderLimit(t *testing.T) {
	newIvfAlgo = newMockIvfAlgoFn
	getVersion = mockVersion

	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"
	ut := newIvfSearchTestCase(t, mpool.MustNewZero(), ivfsearchdefaultAttrs, param)

	ut.arg.Args = makeConstInputExprsIvfSearchWithConfig(
		fmt.Sprintf(`{"db":"db","src":"src","metadata":"__metadata","index":"__index_limit_precedence","entries":"__entries","parttype": %d}`, int32(types.T_array_float32)),
	)
	ut.arg.Limit = plan2.MakePlan2Uint64ConstExprWithType(3)
	ut.arg.IndexReaderParam = &plan.IndexReaderParam{
		Limit: plan2.MakePlan2Uint64ConstExprWithType(15),
	}
	ut.arg.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{{UseBloomFilter: true}}

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	state := ut.arg.ctr.state.(*ivfSearchState)
	require.Equal(t, uint64(15), state.limit)
}

func TestIvfSearchPrepareUsesIndexReaderLimitWithoutRuntimeFilter(t *testing.T) {
	newIvfAlgo = newMockIvfAlgoFn
	getVersion = mockVersion

	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"
	ut := newIvfSearchTestCase(t, mpool.MustNewZero(), ivfsearchdefaultAttrs, param)

	ut.arg.Args = makeConstInputExprsIvfSearchWithConfig(
		fmt.Sprintf(`{"db":"db","src":"src","metadata":"__metadata","index":"__index_limit_no_runtime_filter","entries":"__entries","parttype": %d}`, int32(types.T_array_float32)),
	)
	ut.arg.Limit = plan2.MakePlan2Uint64ConstExprWithType(3)
	ut.arg.IndexReaderParam = &plan.IndexReaderParam{
		Limit: plan2.MakePlan2Uint64ConstExprWithType(15),
	}

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	state := ut.arg.ctr.state.(*ivfSearchState)
	require.Equal(t, uint64(15), state.limit)
}

func makeBatchIvfSearchFail(proc *process.Process) []failBatch {

	failBatches := make([]failBatch, 0, 3)

	{
		tblcfg := ``
		ret := []*plan.Expr{
			{
				Typ: plan.Type{
					Id:    int32(types.T_varchar),
					Width: 512,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Sval{
							Sval: tblcfg,
						},
					},
				},
			},

			plan2.MakePlan2Vecf32ConstExprWithType("[0,1,2]", 3),
		}

		bat := batch.NewWithSize(2)

		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))     // index table config
		bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 3, 0)) // float32 array [3]float32

		vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())

		v := []float32{0, 1, 2}
		vector.AppendArray(bat.Vecs[1], v, false, proc.Mp())

		bat.SetRowCount(1)

		failBatches = append(failBatches, failBatch{ret, bat})

	}
	{
		//tblcfg := `{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index"}`
		ret := []*plan.Expr{
			{

				Typ: plan.Type{
					Id: int32(types.T_int64),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I64Val{
							I64Val: 1,
						},
					},
				},
			},

			plan2.MakePlan2Vecf32ConstExprWithType("[0,1,2]", 3),
		}

		bat := batch.NewWithSize(2)

		bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))         // index table config
		bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 3, 0)) // float32 array [3]float32

		vector.AppendFixed(bat.Vecs[0], int64(1), false, proc.Mp())

		v := []float32{0, 1, 2}
		vector.AppendArray(bat.Vecs[1], v, false, proc.Mp())

		bat.SetRowCount(1)

		failBatches = append(failBatches, failBatch{ret, bat})

	}
	{
		tblcfg := `{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index"}`
		ret := []*plan.Expr{
			{
				Typ: plan.Type{
					Id:    int32(types.T_varchar),
					Width: 512,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Sval{
							Sval: tblcfg,
						},
					},
				},
			},
			{

				Typ: plan.Type{
					Id: int32(types.T_int64),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I64Val{
							I64Val: 1,
						},
					},
				},
			},
		}

		bat := batch.NewWithSize(2)

		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0)) // index table config
		bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))     // pkid int64

		vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())
		vector.AppendFixed(bat.Vecs[1], int64(1), false, proc.Mp())

		bat.SetRowCount(1)

		failBatches = append(failBatches, failBatch{ret, bat})

	}
	return failBatches
}
