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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
		require.Equal(t, types.T_json, vec.GetType().Oid)
		got := types.DecodeJson(vec.GetBytesAt(0)).String()
		if got != input {
			t.Errorf("prepared UNNEST input truncated: want %d bytes, got %d", len(input), len(got))
		}
	}
}

func TestPreparedSeriesCTASColumnOrderAndExplicitType(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sql           string
		resultOrdinal int
		wantWidth     int32
		originalType  types.T
	}{
		{"inferred", "create table gs_inferred as select result from generate_series(?,?,?) g", 0, int32(types.MaxVarcharLen), types.T_int64},
		{"target-only prefix", "create table gs_prefix (extra int default 1) as select result from generate_series(?,?,?) g", 1, int32(types.MaxVarcharLen), types.T_int64},
		{"explicit override", "create table gs_explicit (result varchar(40)) as select result from generate_series(?,?,?) g", 0, 40, types.T_varchar},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare gs_ctas from '"+tc.sql+"'")
			require.NoError(t, err)
			original := prepared.GetDcl().GetPrepare().Plan
			values := []any{
				ParamValue{Value: "2020-01-01", SourceType: types.T_text.ToType(), HasSourceType: true},
				ParamValue{Value: "2020-01-03", SourceType: types.T_text.ToType(), HasSourceType: true},
				ParamValue{Value: "1 day", SourceType: types.T_text.ToType(), HasSourceType: true},
			}
			filled, changed, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, values)
			require.NoError(t, err)
			require.True(t, changed)
			stmt, err := mysql.ParseOne(context.Background(), tc.sql, 1)
			require.NoError(t, err)
			require.NoError(t, RefreshPreparedCTASInferredColumns(context.Background(), filled, original, stmt))
			target := filled.GetDdl().GetCreateTable().TableDef.Cols[tc.resultOrdinal]
			require.Equal(t, "result", target.Name)
			require.Equal(t, types.T_varchar, types.T(target.Typ.Id))
			require.Equal(t, tc.wantWidth, target.Typ.Width)
			require.True(t, target.Default.NullAbility)
			require.Equal(t, tc.originalType, types.T(original.GetDdl().GetCreateTable().TableDef.Cols[tc.resultOrdinal].Typ.Id),
				"execution must not mutate the cached PREPARE target")
		})
	}
}
