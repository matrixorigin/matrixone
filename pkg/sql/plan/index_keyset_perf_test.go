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

package plan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// BenchmarkIndexKeySetCandidates measures the index-choice phase separately
// from parsing, data setup, and execution. Each candidate has a distinct
// equality part and shares the IN column. A non-index payload prevents a
// covering-index shortcut. The broad scan estimates trigger the gate.
func BenchmarkIndexKeySetCandidates(b *testing.B) {
	cases := []struct {
		name                     string
		candidates, count, width int
		duplicate                bool
	}{
		{"small_one_unique", 1, 2, 12, false},
		{"small_one_duplicate", 1, 2, 12, true},
		{"small_eight_unique", 8, 2, 12, false},
		{"small_eight_duplicate", 8, 2, 12, true},
		{"threshold_eight_unique", 1, 8, 12, false},
		{"threshold_eight_duplicate", 1, 8, 12, true},
		{"threshold_sixteen_unique", 1, 16, 12, false},
		{"threshold_sixteen_duplicate", 1, 16, 12, true},
		{"large_one_unique", 1, 128, 65535, false},
		{"large_one_duplicate", 1, 128, 65535, true},
		{"large_eight_unique", 8, 128, 65535, false},
		{"large_eight_duplicate", 8, 128, 65535, true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			mock := NewMockOptimizer(true)
			proc := mock.ctxt.GetProcess()
			defer func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) }()
			addIndexHintChoiceTableForTest(mock)
			table := mock.ctxt.tables["index_hint_t"]
			table.Indexes = nil
			stringType := planpb.Type{Id: int32(types.T_varchar), Width: int32(tc.width)}
			table.Cols[table.Name2ColIndex["b"]].Typ = stringType
			payload := *table.Cols[table.Name2ColIndex["a"]]
			payload.Name, payload.OriginName, payload.ColId = "payload", "payload", 1000
			table.Name2ColIndex[payload.Name] = int32(len(table.Cols))
			table.Cols = append(table.Cols, &payload)
			for i := 0; i < tc.candidates; i++ {
				name := fmt.Sprintf("a%d", i)
				col := *table.Cols[table.Name2ColIndex["a"]]
				col.Name, col.OriginName, col.ColId = name, name, uint64(100+i)
				table.Name2ColIndex[name] = int32(len(table.Cols))
				table.Cols = append(table.Cols, &col)
				indexName := fmt.Sprintf("uk_%d", i)
				indexTableName := fmt.Sprintf("uk_table_%d", i)
				addIndexHintIndexTableForTest(mock, indexTableName, uint64(27000+i))
				table.Indexes = append(table.Indexes, &IndexDef{IndexName: indexName, IndexTableName: indexTableName, Parts: []string{name, "b"}, Unique: true, TableExist: true})
			}
			// Builder binding tags start at the same value on every iteration.
			probe := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
			tag := probe.genNewBindTag()
			filters := make([]*planpb.Expr, 0, tc.candidates+1)
			for i := 0; i < tc.candidates; i++ {
				pos := table.Name2ColIndex[fmt.Sprintf("a%d", i)]
				value := &planpb.Expr{Typ: table.Cols[pos].Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 1}}}}
				eq, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), "=", []*planpb.Expr{GetColExpr(table.Cols[pos].Typ, tag, pos), value})
				if err != nil {
					b.Fatal(err)
				}
				eq.Selectivity = 1
				filters = append(filters, eq)
			}
			values := vector.NewVec(types.T_varchar.ToType())
			for i := 0; i < tc.count; i++ {
				index := i
				if tc.duplicate {
					index = 0
				}
				value := fmt.Sprintf("%04d", index) + strings.Repeat("x", tc.width-4)
				if err := vector.AppendBytes(values, []byte(value), false, proc.Mp()); err != nil {
					b.Fatal(err)
				}
			}
			data, err := values.MarshalBinary()
			values.Free(proc.Mp())
			if err != nil {
				b.Fatal(err)
			}
			pos := table.Name2ColIndex["b"]
			in, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), "in", []*planpb.Expr{GetColExpr(stringType, tag, pos), {Typ: stringType, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: int32(tc.count), Data: data}}}})
			if err != nil {
				b.Fatal(err)
			}
			in.Selectivity = 0.5
			filters = append(filters, in)
			refs := map[[2]int32]int{{tag, table.Name2ColIndex["payload"]}: 1}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
				if builder.genNewBindTag() != tag {
					b.Fatal("inconsistent binding tag")
				}
				node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[table.Name], TableDef: table, BindingTags: []int32{tag}, FilterList: filters}
				scanID := builder.appendNode(node, NewBindContext(builder, nil))
				node.Stats = &planpb.Stats{TableCnt: 60000, Cost: 60000, Outcnt: 30000, Selectivity: 0.5}
				builder.applyIndicesForFiltersRegularIndex(scanID, node, refs, map[[2]int32]*planpb.Expr{})
			}
		})
	}
}
