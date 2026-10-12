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

package frontend

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestPreparedGeometrySRIDUsesCurrentConfiguration(t *testing.T) {
	type execution struct {
		values      []any
		want        int64
		null, fails bool
	}
	wkb := string(geo.WriteWKB(geo.Point{X: 1, Y: 2}))
	for _, tc := range []struct {
		name, sql  string
		types      []defines.MysqlType
		executions []execution
	}{
		{"setter", "select st_srid(st_srid(st_geomfromtext('POINT(1 2)'), ?))", []defines.MysqlType{defines.MYSQL_TYPE_LONG}, []execution{
			{[]any{"4326"}, 4326, false, false}, {[]any{"4326"}, 4326, false, false},
			{[]any{"3857"}, 3857, false, false}, {[]any{nil}, 0, true, false},
			{[]any{"-1"}, 0, false, true}, {[]any{"0"}, 0, false, false},
		}},
		{"typed null SRID", "select st_srid(st_srid(st_geomfromtext('POINT(1 2)'), ?))", []defines.MysqlType{defines.MYSQL_TYPE_BLOB}, []execution{
			{[]any{nil}, 0, true, false}, {[]any{"<null>"}, 0, false, true},
		}},
		{"constructor", "select st_srid(st_geomfromwkb(?, ?))", []defines.MysqlType{defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_LONG}, []execution{
			{[]any{nil, "4326"}, 0, true, false}, {[]any{wkb, "4326"}, 4326, false, false},
			{[]any{nil, "4326"}, 0, true, false}, {[]any{nil, "-1"}, 0, true, false},
			{[]any{wkb, "-1"}, 0, false, true}, {[]any{wkb, "3857"}, 3857, false, false},
		}},
		{"fixed WKB SRID", "select st_srid(st_geomfromwkb(?, 4326))", []defines.MysqlType{defines.MYSQL_TYPE_BLOB}, []execution{
			{[]any{nil}, 0, true, false}, {[]any{wkb}, 4326, false, false},
			{[]any{wkb}, 4326, false, false}, {[]any{nil}, 0, true, false},
		}},
		{"fixed WKT SRID", "select st_srid(st_geomfromtext(?, 4326))", []defines.MysqlType{defines.MYSQL_TYPE_BLOB}, []execution{
			{[]any{nil}, 0, true, false}, {[]any{"POINT(1 2)"}, 4326, false, false},
			{[]any{"POINT(1 2)"}, 4326, false, false}, {[]any{nil}, 0, true, false},
		}},
		{"null through cast", "select st_srid(st_srid(cast(? as geometry), ?))", []defines.MysqlType{defines.MYSQL_TYPE_NULL, defines.MYSQL_TYPE_LONG}, []execution{
			{[]any{nil, "-1"}, 0, true, false},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 28795, tc.sql)
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil); prepared.Close() })
			original := prepared.PreparePlan.GetDcl().GetPrepare().Plan
			snapshot := proto.Clone(original).(*plan.Plan)
			for _, run := range tc.executions {
				if prepared.params != nil {
					cw.proc.SetPrepareParams(nil)
					prepared.params.Free(cw.proc.Mp())
				}
				prepared.params = vector.NewVec(types.T_text.ToType())
				prepared.ParamTypes = nil
				for i, value := range run.values {
					var bytes []byte
					if value != nil {
						bytes = []byte(value.(string))
					}
					require.NoError(t, vector.AppendBytes(prepared.params, bytes, value == nil, cw.proc.Mp()))
					prepared.ParamTypes = append(prepared.ParamTypes, byte(tc.types[i]), 0)
				}
				comp, p, stmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepared.Name)
				if owned && stmt != nil {
					stmt.Free()
				}
				require.Nil(t, comp)
				require.Nil(t, cw.runtimeCachePlan, "value-dependent SRID metadata must be rebuilt for each execution")
				require.Nil(t, prepared.runtimePlan)
				require.True(t, proto.Equal(snapshot, original), "execution must not mutate the PREPARE plan")
				if run.fails {
					require.Error(t, err)
					continue
				}
				require.NoError(t, err)
				q := p.GetQuery()
				expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
				func() {
					result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
					require.NoError(t, err)
					defer free()
					require.Equal(t, run.null, result.IsNull(0))
					if !run.null {
						require.Equal(t, types.T_uint32, result.GetType().Oid)
						require.Equal(t, run.want, int64(vector.GetFixedAtNoTypeCheck[uint32](result, 0)))
					}
				}()
			}
		})
	}
}
