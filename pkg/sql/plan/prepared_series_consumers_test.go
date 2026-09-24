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
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparedSeriesConsumers(t *testing.T) {
	for _, sql := range []string{
		"select sum(result),avg(result) from generate_series(?,?,?) g",
		"select result from generate_series(?,?,?) g order by result",
		"select result+1 from generate_series(?,?,?) g where result>2",
		"select result, row_number() over(order by result) from generate_series(?,?,?) g",
		"select result from generate_series(?,?,?) g union all select 4",
		"create table gs_review as select result from generate_series(?,?,?) g",
	} {
		t.Run(sql, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare review_probe from '"+sql+"'")
			if err != nil {
				t.Errorf("prepare: %v", err)
				return
			}
			vals := []any{}
			for _, v := range []int64{1, 9, 2} {
				vals = append(vals, ParamValue{Value: v, SourceType: types.T_int64.ToType(), HasSourceType: true})
			}
			filled, changed, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), prepared.GetDcl().GetPrepare().Plan, vals)
			if err != nil {
				t.Errorf("specialize: %v", err)
				return
			}
			require.True(t, changed)
			q := filled.GetQuery()
			if q == nil && filled.GetDdl() != nil {
				q = filled.GetDdl().Query
			}
			require.NotNil(t, q)
			found := false
			for _, n := range q.Nodes {
				if n.TableDef.GetTblFunc().GetName() == "generate_series" {
					found = true
					require.Equal(t, types.T_int64, types.T(n.TableDef.Cols[0].Typ.Id))
					require.Equal(t, types.T_int64, types.T(n.TblFuncExprList[0].Typ.Id))
				}
			}
			require.True(t, found)
			if strings.Contains(sql, "union all") {
				columns := GetResultColumnsFromPlan(filled)
				require.Len(t, columns, 1)
				require.Equal(t, types.T_int64, types.T(columns[0].Typ.Id))
			}
		})
	}
}

func TestPreparedUnnestLargeJSON(t *testing.T) {
	p, err := runOneStmt(NewMockOptimizer(false), t, "prepare long_json from 'select value from unnest(?) u'")
	if err != nil {
		t.Fatal(err)
	}
	input := `["` + strings.Repeat("x", 70000) + `"]`
	bound, err := FillValuesOfParamsInPlan(context.Background(), p.GetDcl().GetPrepare().Plan, []any{ParamValue{Value: input, SourceType: types.T_text.ToType(), HasSourceType: true}})
	if err != nil {
		t.Fatal(err)
	}
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, n := range bound.GetQuery().Nodes {
		if n.TableDef.GetTblFunc().GetName() != "unnest" {
			continue
		}
		vec, free, err := colexec.GetReadonlyResultFromExpression(proc, n.TblFuncExprList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
		if err != nil {
			t.Fatal(err)
		}
		defer free()
		got := vec.GetStringAt(0)
		if got != input {
			t.Errorf("prepared UNNEST input truncated: want %d bytes, got %d", len(input), len(got))
		}
		if _, err := types.ParseStringToByteJson(got); err != nil {
			t.Errorf("converted JSON invalid after %d bytes: %T", len(got), err)
		}
	}
}
