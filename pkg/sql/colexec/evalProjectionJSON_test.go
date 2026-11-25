// Copyright 2026 Matrix Origin
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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func jsonProjectionTestCol(col int32, typ types.T) *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: col}}}
}

func jsonProjectionTestCall(t testing.TB, proc *process.Process, name string, result types.T, args ...*plan.Expr) *plan.Expr {
	t.Helper()
	inputs := make([]types.Type, len(args))
	for i, arg := range args {
		inputs[i] = types.MustTypeFromPlan(arg.Typ)
	}
	f, err := function.GetFunctionByName(proc.Ctx, name, inputs)
	require.NoError(t, err)
	return &plan.Expr{Typ: plan.Type{Id: int32(result)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: name, Obj: f.GetEncodedOverloadID()}, Args: args,
	}}}
}

func jsonProjectionTestExtract(t testing.TB, proc *process.Process, col int32, path string) *plan.Expr {
	doc := jsonProjectionTestCol(col, types.T_text)
	literal := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: path}}}}
	return jsonProjectionTestCall(t, proc, "json_extract", types.T_json, doc, literal)
}

func CountProjectionJSONParses(t *testing.T, frame *ProjectionJSONFrame) *int {
	t.Helper()
	require.NotNil(t, frame)
	count := new(int)
	frame.parse = func(ctx context.Context, data []byte) (bytejson.ByteJson, error) {
		*count++
		return bytejson.ParseFromByteSliceWithCancellation(ctx, data)
	}
	return count
}

func TestProjectionJSONSharesSevenConsumers(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseline := proc.Mp().CurrNB()
	func() {
		input := batch.NewWithSize(1)
		defer input.Clean(proc.Mp())
		input.Vecs[0] = vector.NewVec(types.T_text.ToType(), proc.Mp())
		input.SetRowCount(3)
		projection := Projection{}
		for i := range 7 {
			path := fmt.Sprintf("$.k%d", i)
			leaf := jsonProjectionTestExtract(t, proc, 0, path)
			projection.ProjectList = append(projection.ProjectList, jsonProjectionTestCall(t, proc, "json_unquote", types.T_varchar, leaf))
		}
		require.NoError(t, projection.PrepareProjection(proc))
		defer projection.FreeProjection(proc)
		count := CountProjectionJSONParses(t, projection.jsonFrame)
		require.Len(t, projection.jsonFrame.groups, 1)
		require.Len(t, projection.jsonFrame.groups[0].paths, 7)

		// Refill the same batch header with different bytes across executions.
		for _, prefix := range []string{"first", "second"} {
			input.Vecs[0].ResetWithSameType()
			fields := make([]string, 7)
			for i := range fields {
				fields[i] = fmt.Sprintf(`"k%d":"%s-%d"`, i, prefix, i)
			}
			doc := "{" + strings.Join(fields, ",") + "}"
			require.NoError(t, vector.AppendStringList(input.Vecs[0], []string{doc, doc, ""}, []bool{false, false, true}, proc.Mp()))
			*count = 0
			result, err := projection.EvalProjection(input, proc)
			require.NoError(t, err)
			require.Equal(t, 2, *count)
			for i, v := range result.Vecs {
				want := fmt.Sprintf("%s-%d", prefix, i)
				require.Equal(t, want, v.UnsafeGetStringAt(0))
				require.Equal(t, want, v.UnsafeGetStringAt(1))
				require.True(t, v.IsNull(2))
			}
			require.Nil(t, projection.jsonFrame.input)
			require.Empty(t, projection.jsonFrame.pages)
			require.Nil(t, projection.jsonFrame.groups[0].metadata)
			projection.ResetProjection(proc)

			input.Vecs[0].ResetWithSameType()
			require.NoError(t, vector.AppendStringList(input.Vecs[0], []string{doc, "invalid JSON", doc}, nil, proc.Mp()))
			_, err = projection.EvalProjection(input, proc)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
			require.Nil(t, projection.jsonFrame.input)
			require.Empty(t, projection.jsonFrame.pages)
			require.Nil(t, projection.jsonFrame.groups[0].metadata)
			require.Zero(t, projection.jsonFrame.cacheBytes)
			projection.ResetProjection(proc)
		}
	}()
	require.Equal(t, baseline, proc.Mp().CurrNB())
}

func TestProjectionJSONSharesSelectedPhysicalRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	baseline := mp.CurrNB()
	t.Cleanup(func() { require.Equal(t, baseline, mp.CurrNB()) })
	input := batch.NewWithSize(1)
	defer input.Clean(mp)
	input.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
	var selection bitmap.Bitmap
	selection.Init(bitmapTestMP)
	mustBitmapOK(selection.InitWithSize(3, bitmapTestMP))
	selection.Add(0)
	selection.Add(2)
	input.SetSelection(3, &selection)
	projection := Projection{ProjectList: []*plan.Expr{
		jsonProjectionTestCall(t, proc, "json_unquote", types.T_varchar, jsonProjectionTestExtract(t, proc, 0, "$.a")),
		jsonProjectionTestCall(t, proc, "json_unquote", types.T_varchar, jsonProjectionTestExtract(t, proc, 0, "$.b")),
	}}
	require.NoError(t, projection.PrepareProjection(proc))
	defer projection.FreeProjection(proc)
	count := CountProjectionJSONParses(t, projection.jsonFrame)
	mask := input.Selection()
	var physical batch.Batch
	eval := func(docs []string) (*batch.Batch, error) {
		input.Vecs[0].ResetWithSameType()
		require.NoError(t, vector.AppendStringList(input.Vecs[0], docs, nil, mp))
		input.PhysicalView(&physical)
		result, err := projection.EvalProjectionSelected(input, &physical, mask, proc)
		require.Nil(t, projection.jsonFrame.input)
		require.Empty(t, projection.jsonFrame.pages)
		require.Zero(t, projection.jsonFrame.cacheBytes)
		return result, err
	}
	first := `{"a":"first a","b":"first b"}`
	last := `{"a":"last a","b":"last b"}`
	result, err := eval([]string{first, "invalid masked row", last})
	require.NoError(t, err)
	require.Equal(t, 2, *count)
	require.Equal(t, []int64{0, 2}, result.SelectionRows(nil))
	for col, expected := range [][2]string{{"first a", "last a"}, {"first b", "last b"}} {
		require.Equal(t, expected[0], result.Vecs[col].UnsafeGetStringAt(0))
		require.True(t, result.Vecs[col].IsNull(1))
		require.Equal(t, expected[1], result.Vecs[col].UnsafeGetStringAt(2))
	}
	projection.ResetProjection(proc)
	_, err = eval([]string{first, "invalid masked row", "invalid selected row"})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
}

func TestProjectionJSONSparseMasksUseOriginalRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseline := proc.Mp().CurrNB()
	t.Cleanup(func() { require.Equal(t, baseline, proc.Mp().CurrNB()) })
	input := batch.NewWithSize(4)
	defer input.Clean(proc.Mp())
	input.SetRowCount(5)
	docs := []string{`{"a":40,"b":41}`, "invalid document", `{"a":20,"b":21}`, `{"a":null}`, `{"a":10,"b":11}`}
	for _, col := range []int{0, 3} {
		input.Vecs[col] = vector.NewVec(types.T_text.ToType(), proc.Mp())
		require.NoError(t, vector.AppendStringList(input.Vecs[col], docs, nil, proc.Mp()))
	}
	left := []bool{true, false, true, true, false}
	right := []bool{false, false, true, true, true}
	for col, mask := range [][]bool{left, right} {
		input.Vecs[col+1] = vector.NewVec(types.T_bool.ToType(), proc.Mp())
		require.NoError(t, vector.AppendFixedList(input.Vecs[col+1], mask, nil, proc.Mp()))
	}
	build := func(col, mask int32, key string) *plan.Expr {
		return jsonProjectionTestCall(t, proc, "case", types.T_json,
			jsonProjectionTestCol(mask, types.T_bool), jsonProjectionTestExtract(t, proc, col, key))
	}
	p := Projection{ProjectList: []*plan.Expr{build(0, 1, "$.a"), build(0, 2, "$.b"), build(3, 1, "$.a"), build(3, 2, "$.b")}}
	require.NoError(t, p.PrepareProjection(proc))
	defer p.FreeProjection(proc)
	require.Nil(t, p.jsonFrame, "masked leaves must not precompute their siblings' paths")

	result, err := p.EvalProjection(input, proc)
	require.NoError(t, err)
	for col, want := range [][]string{
		{"40", "", "20", "null", ""}, {"", "", "21", "", "11"},
		{"40", "", "20", "null", ""}, {"", "", "21", "", "11"},
	} {
		v := result.Vecs[col]
		for row, expected := range want {
			if expected == "" {
				require.True(t, v.IsNull(uint64(row)))
				continue
			}
			require.False(t, v.IsNull(uint64(row)))
			require.Equal(t, expected, types.DecodeJson(v.GetBytesAt(row)).String())
		}
	}
	vector.MustFixedColNoTypeCheck[bool](input.Vecs[1])[1] = true
	_, err = p.EvalProjection(input, proc)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))

	p.ResetProjection(proc)
	vector.MustFixedColNoTypeCheck[bool](input.Vecs[1])[1] = false
	result, err = p.EvalProjection(input, proc)
	require.NoError(t, err)
	require.True(t, result.Vecs[0].IsNull(1))
	require.Equal(t, "40", types.DecodeJson(result.Vecs[0].GetBytesAt(0)).String())
}

func TestProjectionJSONSharedGuardHonorsPhysicalMask(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := batch.NewWithSize(3)
	defer input.Clean(proc.Mp())
	input.SetRowCount(4)
	input.Vecs[0] = vector.NewVec(types.T_text.ToType(), proc.Mp())
	require.NoError(t, vector.AppendStringList(input.Vecs[0], []string{`{"a":10,"b":20}`, "invalid", "invalid", `{"a":30}`}, nil, proc.Mp()))
	for i, values := range [][]bool{{true, false, true, true}, {true, true, false, false}} {
		input.Vecs[i+1] = vector.NewVec(types.T_bool.ToType(), proc.Mp())
		require.NoError(t, vector.AppendFixedList(input.Vecs[i+1], values, nil, proc.Mp()))
	}
	build := func(path string, guard int32) *plan.Expr {
		extract := jsonProjectionTestExtract(t, proc, 0, path)
		missing := &plan.Expr{Typ: plan.Type{Id: int32(types.T_json)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
		fallback := jsonProjectionTestCall(t, proc, "coalesce", types.T_json, extract, missing)
		return jsonProjectionTestCall(t, proc, "if", types.T_json, jsonProjectionTestCol(guard, types.T_bool), fallback, missing)
	}
	exprs := []*plan.Expr{build("$.a", 1), build("$.b", 1)}
	_, separate := bindProjectionJSON([]*plan.Expr{exprs[0], build("$.b", 2)})
	require.Nil(t, separate)
	executors, frame, err := NewProjectionExecutors(proc, exprs, nil)
	require.NoError(t, err)
	require.NotNil(t, frame)
	for _, executor := range executors {
		defer executor.Free()
	}
	parses := CountProjectionJSONParses(t, frame)
	frame.Begin(input)
	defer frame.End()
	for i, want := range []string{"10", "20"} {
		vec, err := executors[i].Eval(proc, []*batch.Batch{input}, boolSelection([]bool{true, true, false, true}))
		require.NoError(t, err)
		require.Equal(t, want, types.DecodeJson(vec.GetBytesAt(0)).String())
		require.True(t, vec.IsNull(1))
		require.True(t, vec.IsNull(2))
		if i == 0 {
			require.Equal(t, "30", types.DecodeJson(vec.GetBytesAt(3)).String())
		} else {
			require.True(t, vec.IsNull(3))
		}
	}
	require.Equal(t, 2, *parses, "one parse per demanded physical row")
}

func TestProjectionJSONRequiredOutputBudget(t *testing.T) {
	const limit = 33536
	proc := testutil.NewProcess(t)
	pool := proc.Mp()
	input := batch.NewWithSize(1)
	defer input.Clean(pool)
	input.Vecs[0] = vector.NewVec(types.T_text.ToType(), pool)
	input.SetRowCount(1)
	value := strings.Repeat("x", 2048)
	doc := fmt.Sprintf(`{"a":%q,"b":%q}`, value, value)
	require.NoError(t, vector.AppendStringList(input.Vecs[0], []string{doc}, nil, pool))
	exprs := []*plan.Expr{
		jsonProjectionTestExtract(t, proc, 0, "$.a"),
		jsonProjectionTestExtract(t, proc, 0, "$.b"),
	}
	check := func(t *testing.T, proc *process.Process, execs []ExpressionExecutor, frame *ProjectionJSONFrame) {
		t.Helper()
		defer func() {
			for _, exec := range execs {
				exec.Free()
			}
		}()
		if frame != nil {
			frame.Begin(input)
			defer frame.End()
		}
		for _, exec := range execs {
			got, err := exec.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, value, string(types.DecodeJson(got.GetBytesAt(0)).GetString()))
		}
	}
	compare := func(t *testing.T, proc *process.Process, selection *mpool.MPool, share bool) {
		t.Helper()
		t.Run("ordinary", func(t *testing.T) {
			execs, err := NewExpressionExecutorsFromPlanExpressionsWithAllocation(proc, exprs, selection)
			require.NoError(t, err)
			check(t, proc, execs, nil)
		})
		t.Run("projection", func(t *testing.T) {
			execs, frame, err := NewProjectionExecutors(proc, exprs, selection)
			require.NoError(t, err)
			check(t, proc, execs, frame)
			require.Equal(t, share, frame != nil)
		})
	}
	t.Run("account", func(t *testing.T) {
		registry, err := mpool.NewAllocationAccountRegistry(1, 256)
		require.NoError(t, err)
		account, err := registry.Open(limit)
		require.NoError(t, err)
		defer func() {
			require.Zero(t, account.Snapshot().Used)
			account.Seal()
			_, err := registry.Finalize(account)
			require.NoError(t, err)
		}()
		selection, err := pool.WithAccount(account, mpool.AllocationOwnerDependent, 1, mpool.AllocationCapacityClassDefault)
		require.NoError(t, err)
		compare(t, proc, selection, false)
	})
	t.Run("physical admission inherited from process", func(t *testing.T) {
		used := uint64(0)
		selection, err := pool.WithAdmission(func(size uint64) (func(), error) {
			if size > limit-used {
				return nil, mpool.ErrAllocationAccountCapacity
			}
			used += size
			return func() { used -= size }, nil
		})
		require.NoError(t, err)
		inherited := testutil.NewProcess(t, testutil.WithMPool(selection))
		compare(t, inherited, nil, false)
		require.Zero(t, used)
	})
	t.Run("base pool off-heap limit", func(t *testing.T) {
		limited, err := mpool.NewMPool("json-projection-output-budget", 1<<20, mpool.NoFixed)
		require.NoError(t, err)
		defer mpool.DeleteMPool(limited)
		reserved, err := limited.Alloc(int(limited.Cap())-limit, true)
		require.NoError(t, err)
		defer limited.Free(reserved)
		compare(t, proc, limited, true)
		require.EqualValues(t, len(reserved), limited.CurrNB())
		bytes, objects := limited.OnHeapOutstanding()
		require.Zero(t, bytes)
		require.Zero(t, objects)
	})
}

func TestProjectionJSONMetadataBudget(t *testing.T) {
	tests := []struct {
		name        string
		rows, paths int
		admitted    bool
	}{
		{name: "dense", rows: 1024, paths: 7, admitted: true},
		{name: "wide batch", rows: 8192, paths: 128},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			pool := proc.Mp()
			start := pool.CurrNB()
			heapStart, objectsStart := pool.OnHeapOutstanding()
			frame := &ProjectionJSONFrame{mp: pool, parse: bytejson.ParseFromByteSliceWithCancellation}
			group := &jsonProjectionGroup{frame: frame, paths: make([]bytejson.Path, tt.paths)}
			frame.groups = append(frame.groups, group)
			path, err := bytejson.ParseJsonPath("$.a")
			require.NoError(t, err)
			for i := range group.paths {
				group.paths[i] = path
			}
			input := batch.NewWithSize(0)
			input.SetRowCount(tt.rows)
			frame.Begin(input)
			defer func() {
				frame.End()
				heap, objects := pool.OnHeapOutstanding()
				require.Equal(t, heapStart, heap)
				require.Equal(t, objectsStart, objects)
			}()
			first := jsonProjectionBinding{group: group}
			value, exists, err := first.extract(proc.Ctx, 0, []byte(`{"a":1}`))
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, "1", value.String())
			require.Equal(t, tt.admitted, group.metadata != nil)
			require.Equal(t, start, pool.CurrNB())
			heap, _ := pool.OnHeapOutstanding()
			require.EqualValues(t, frame.cacheBytes, heap-heapStart)
			require.LessOrEqual(t, frame.cacheBytes, jsonProjectionCacheLimit)
			if !tt.admitted {
				require.Zero(t, frame.cacheBytes)
			}

			last := jsonProjectionBinding{group: group, slot: tt.paths - 1}
			value, exists, err = last.extract(proc.Ctx, 0, []byte(`{"a":1}`))
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, "1", value.String())
		})
	}
}

func TestProjectionJSONCombinedCacheLimit(t *testing.T) {
	proc := testutil.NewProcess(t)
	pool := proc.Mp()
	start := pool.CurrNB()
	heapStart, objectsStart := pool.OnHeapOutstanding()
	firstLeaf := jsonProjectionTestExtract(t, proc, 0, "$.a")
	lastLeaf := jsonProjectionTestExtract(t, proc, 0, "$.b")
	bindings, frame := bindProjectionJSON([]*plan.Expr{firstLeaf, lastLeaf})
	frame.mp = pool
	first, last := bindings[firstLeaf.GetF()], bindings[lastLeaf.GetF()]
	input := batch.NewWithSize(0)
	input.SetRowCount(1024)
	frame.Begin(input)
	defer func() {
		frame.End()
		require.Equal(t, start, pool.CurrNB())
		heap, objects := pool.OnHeapOutstanding()
		require.Equal(t, heapStart, heap)
		require.Equal(t, objectsStart, objects)
	}()

	payload := strings.Repeat("x", 3072)
	raw := []byte(fmt.Sprintf(`{"a":%q,"b":%q}`, payload, payload))
	for row := range input.RowCount() {
		_, _, err := first.extract(proc.Ctx, row, raw)
		require.NoError(t, err)
	}
	require.True(t, frame.pagesFull)
	require.Equal(t, start, pool.CurrNB())
	heap, _ := pool.OnHeapOutstanding()
	require.EqualValues(t, frame.cacheBytes, heap-heapStart)
	require.NotEmpty(t, first.group.metadata)
	require.Less(t, len(frame.pages)*jsonProjectionPageSize, jsonProjectionCacheLimit)
	require.LessOrEqual(t, frame.cacheBytes, jsonProjectionCacheLimit)

	// The first row is served from cached bytes even after the cache fills.
	got, exists, err := last.extract(proc.Ctx, 0, nil)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, payload, string(got.GetString()))
	// A row that did not fit is still available through direct extraction.
	got, exists, err = last.extract(proc.Ctx, input.RowCount()-1, raw)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, payload, string(got.GetString()))
}

func BenchmarkProjectionJSON(b *testing.B) {
	const rows = 1024
	for _, workload := range []struct {
		name   string
		leaves int
		sparse bool
	}{
		{name: "dense-seven", leaves: 7},
		{name: "sparse-case", leaves: 64, sparse: true},
	} {
		for _, mode := range []string{"projection", "ordinary"} {
			b.Run(workload.name+"/"+mode, func(b *testing.B) {
				proc := testutil.NewProcess(b)
				columns := 1
				if workload.sparse {
					columns += workload.leaves
				}
				input := batch.NewWithSize(columns)
				defer input.Clean(proc.Mp())
				input.SetRowCount(rows)
				input.Vecs[0] = vector.NewVec(types.T_text.ToType(), proc.Mp())
				fields := make([]string, workload.leaves)
				for i := range workload.leaves {
					fields[i] = fmt.Sprintf(`"k%d":"value-%d"`, i, i)
				}
				if !workload.sparse {
					fields = append(fields, `"padding":"`+strings.Repeat("x", 4096)+`"`)
				}
				doc := "{" + strings.Join(fields, ",") + "}"
				docs := make([]string, rows)
				for i := range docs {
					docs[i] = doc
				}
				require.NoError(b, vector.AppendStringList(input.Vecs[0], docs, nil, proc.Mp()))
				projection := Projection{}
				for col := range workload.leaves {
					leaf := jsonProjectionTestExtract(b, proc, 0, fmt.Sprintf("$.k%d", col))
					if workload.sparse {
						input.Vecs[col+1] = vector.NewVec(types.T_bool.ToType(), proc.Mp())
						mask := make([]bool, rows)
						for row := col; row < rows; row += workload.leaves {
							mask[row] = true
						}
						require.NoError(b, vector.AppendFixedList(input.Vecs[col+1], mask, nil, proc.Mp()))
						leaf = jsonProjectionTestCall(b, proc, "case", types.T_json, jsonProjectionTestCol(int32(col+1), types.T_bool), leaf)
					} else {
						leaf = jsonProjectionTestCall(b, proc, "json_unquote", types.T_varchar, leaf)
					}
					projection.ProjectList = append(projection.ProjectList, leaf)
				}
				require.NoError(b, projection.PrepareProjection(proc))
				defer projection.FreeProjection(proc)
				if mode == "ordinary" {
					projection.jsonFrame = nil
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_, err := projection.EvalProjection(input, proc)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
