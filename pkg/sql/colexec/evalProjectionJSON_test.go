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

func countProjectionParses(t *testing.T, projection *Projection) *int {
	t.Helper()
	require.NotNil(t, projection.jsonFrame)
	count := new(int)
	projection.jsonFrame.parse = func(ctx context.Context, data []byte) (bytejson.ByteJson, error) {
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
		input.Vecs[0] = vector.NewVec(types.T_text.ToType())
		input.SetRowCount(3)
		projection := Projection{}
		for i := range 7 {
			path := fmt.Sprintf("$.k%d", i)
			leaf := jsonProjectionTestExtract(t, proc, 0, path)
			projection.ProjectList = append(projection.ProjectList, jsonProjectionTestCall(t, proc, "json_unquote", types.T_varchar, leaf))
		}
		require.NoError(t, projection.PrepareProjection(proc))
		defer projection.FreeProjection(proc)
		count := countProjectionParses(t, &projection)
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

func TestProjectionJSONSparseMasksUseOriginalRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseline := proc.Mp().CurrNB()
	t.Cleanup(func() { require.Equal(t, baseline, proc.Mp().CurrNB()) })
	input := batch.NewWithSize(4)
	defer input.Clean(proc.Mp())
	input.SetRowCount(5)
	docs := []string{`{"a":10,"b":11}`, "invalid document", `{"a":20,"b":21}`, `{"a":null}`, `{"a":40,"b":41}`}
	for _, col := range []int{0, 3} {
		input.Vecs[col] = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendStringList(input.Vecs[col], docs, nil, proc.Mp()))
	}
	left := []bool{true, false, true, true, false}
	right := []bool{false, false, true, true, true}
	for col, mask := range [][]bool{left, right} {
		input.Vecs[col+1] = vector.NewVec(types.T_bool.ToType())
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

	// Reorder both document vectors without flattening. Rows 0 and 4 also
	// change their physical identity relative to the Boolean masks.
	dict := make([]*vector.Vector, 2)
	sources := []*vector.Vector{input.Vecs[0], input.Vecs[3]}
	admitted, err := vector.TrySelectDictionaryVectors(sources, dict, []int64{4, 1, 2, 3, 0}, input.RowCount(), proc.Mp(), nil)
	require.NoError(t, err)
	require.True(t, admitted)
	defer func() {
		input.Vecs[0], input.Vecs[3] = sources[0], sources[1]
		for _, v := range dict {
			v.Free(proc.Mp())
		}
	}()
	input.Vecs[0], input.Vecs[3] = dict[0], dict[1]
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

func TestProjectionJSONCombinedCacheAdmission(t *testing.T) {
	tests := []struct {
		name        string
		rows, paths int
		account     uint64
		admitted    bool
	}{
		{name: "dense", rows: 1024, paths: 7, account: jsonProjectionCacheLimit, admitted: true},
		{name: "wide batch", rows: 8192, paths: 128, account: 2 * jsonProjectionCacheLimit},
		{name: "metadata denied", rows: 1024, paths: 7, account: 4096},
		{name: "pages denied", rows: 1024, paths: 7, account: 65536},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			pool := proc.Mp()
			start := pool.CurrNB()
			registry, err := mpool.NewAllocationAccountRegistry(1, 256)
			require.NoError(t, err)
			account, err := registry.Open(tt.account)
			require.NoError(t, err)
			defer func() {
				require.Zero(t, account.Snapshot().Used)
				require.Equal(t, start, pool.CurrNB())
				account.Seal()
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			}()
			selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerDependent, 1, 2)
			require.NoError(t, err)
			frame := &ProjectionJSONFrame{mp: pool, allocation: selection, parse: bytejson.ParseFromByteSliceWithCancellation}
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
			defer frame.End()
			first := jsonProjectionBinding{group: group}
			value, exists, err := first.extract(proc.Ctx, 0, []byte(`{"a":1}`))
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, "1", value.String())
			require.Equal(t, tt.admitted, group.metadata != nil)
			require.EqualValues(t, frame.cacheBytes, pool.CurrNB()-start)
			require.EqualValues(t, frame.cacheBytes, account.Snapshot().Used)
			require.LessOrEqual(t, frame.cacheBytes, jsonProjectionCacheLimit)
			if tt.name == "pages denied" {
				require.True(t, frame.pagesFull)
				require.Empty(t, frame.pages)
			}
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
	}()

	payload := strings.Repeat("x", 3072)
	raw := []byte(fmt.Sprintf(`{"a":%q,"b":%q}`, payload, payload))
	for row := range input.RowCount() {
		_, _, err := first.extract(proc.Ctx, row, raw)
		require.NoError(t, err)
	}
	require.True(t, frame.pagesFull)
	require.EqualValues(t, frame.cacheBytes, pool.CurrNB()-start)
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
				input.Vecs[0] = vector.NewVec(types.T_text.ToType())
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
						input.Vecs[col+1] = vector.NewVec(types.T_bool.ToType())
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
