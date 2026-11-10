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
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// This is a resident 64-row expression measurement, not SQL acceptance. Source
// construction/admission is outside timing and stays visible in peak backing.
// The original mutable-vector function benchmark is retained independently.
func BenchmarkJSONValueResidentProjection64(b *testing.B) {
	for _, content := range []string{"integer", "string", "mixed", "wide", "deep"} {
		for _, count := range []int{16, 4096} {
			for _, paths := range []int{1, 2, 8} {
				b.Run(fmt.Sprintf("%s/elements=%d/paths=%d", content, count, paths), func(b *testing.B) {
					text, prefix, expected := jsonProjectionBenchmarkDocument(content, count)
					doc, err := types.ParseStringToByteJson(text)
					require.NoError(b, err)
					raw, err := doc.Marshal()
					require.NoError(b, err)
					proc := testutil.NewProcess(b)
					proc.SetBaseProcessRunningStatus(true)
					initial := proc.Mp().CurrNB()
					b.Cleanup(func() { require.Equal(b, initial, proc.Mp().CurrNB()) })
					input := batch.NewWithSize(1)
					input.Vecs[0] = vector.NewVec(types.T_json.ToType())
					b.Cleanup(func() { input.Clean(proc.Mp()) })
					for range 64 {
						require.NoError(b, vector.AppendBytes(input.Vecs[0], raw, false, proc.Mp()))
					}
					input.SetRowCount(64)
					projection := &Projection{}
					b.Cleanup(func() { projection.FreeProjection(proc) })
					overload, err := function.GetFunctionByName(proc.Ctx, "json_value", []types.Type{types.T_json.ToType(), types.T_varchar.ToType()})
					require.NoError(b, err)
					for i := 0; i < paths; i++ {
						key := "keep"
						if i > 0 {
							key = fmt.Sprintf("p%d", i)
						}
						column := &plan.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
						path := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: prefix + "." + key}}}}
						projection.ProjectList = append(projection.ProjectList, &plan.Expr{Typ: overload.GetReturnType().PlanType(), Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: overload.GetEncodedOverloadID(), ObjName: "json_value"}, Args: []*plan.Expr{column, path}}}})
					}
					require.NoError(b, projection.PrepareProjection(proc))
					evaluate := jsonProjectionResidentBenchmarkEvaluator(b, proc, projection, input)
					check := func(output *batch.Batch) {
						require.Equal(b, 64, output.RowCount())
						require.Len(b, output.Vecs, paths)
						for _, column := range output.Vecs {
							for row := 0; row < 64; row++ {
								require.False(b, column.IsNull(uint64(row)))
								require.Equal(b, expected, string(column.GetBytesAt(row)))
							}
						}
					}
					output, err := evaluate()
					require.NoError(b, err)
					check(output)
					peak := proc.Mp().CurrNB()
					b.Logf("input_sha256=%x rows=64 bytes_per_document=%d", sha256.Sum256(raw), len(raw))
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						output, err = evaluate()
						if err != nil {
							b.Fatal(err)
						}
						if current := proc.Mp().CurrNB(); current > peak {
							peak = current
						}
					}
					b.StopTimer()
					check(output)
					b.ReportMetric(float64(len(raw)), "bytes/document")
					b.ReportMetric(float64(paths), "paths/document")
					b.ReportMetric(float64(peak), "peak_mpool_B")
				})
			}
		}
	}
}

func jsonProjectionBenchmarkDocument(content string, count int) (string, string, string) {
	value, expected := `"value"`, "value"
	if content == "integer" {
		value, expected = "1", "1"
	}
	capacity := 9
	if content == "wide" {
		capacity = count + 8
	}
	fields := make([]string, 0, capacity)
	fields = append(fields, `"keep":`+value)
	for i := 1; i < 8; i++ {
		fields = append(fields, fmt.Sprintf(`"p%d":%s`, i, value))
	}
	if content == "wide" {
		for i := 0; i < count; i++ {
			fields = append(fields, fmt.Sprintf(`"unrelated%04d":1`, i))
		}
	} else {
		items := make([]string, count)
		mixed := []string{"1", `"s"`, "null", "true", `{"v":2}`}
		for i := range items {
			items[i] = "1"
			if content == "mixed" {
				items[i] = mixed[i%len(mixed)]
			}
		}
		fields = append(fields, `"large":[`+strings.Join(items, ",")+`]`)
	}
	text, prefix := "{"+strings.Join(fields, ",")+"}", "$"
	if content == "deep" {
		for range 32 {
			text = `{"level":` + text + `}`
			prefix += ".level"
		}
	}
	return text, prefix, expected
}
