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
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestTableDumpHistoricalUnixTimestampDatetimeDefault(t *testing.T) {
	for _, tc := range []struct{ name, value, column string }{
		{"seconds", "2024-01-01 01:02:03", "bigint"},
		{"fractional", "2024-01-01 01:02:03.123456", "decimal(38,6)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			proc.GetSessionInfo().TimeZone = time.UTC
			compiler := NewMockCompilerContext(false, proc)
			sql := "create table historical_datetime (a " + tc.column +
				" default (unix_timestamp(cast('" + tc.value + "' as datetime))), " +
				"b varchar(64) default (monthname('2024-01-01')))"
			stmt, err := mysql.ParseOne(t.Context(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			build := func(legacy bool) *planpb.TableDef {
				var ctx CompilerContext = compiler
				if legacy {
					ctx = tableDumpBindContext{CompilerContext: compiler,
						ctx: function.WithLegacyUnixTimestampDatetime(compiler.GetContext())}
				}
				built, buildErr := BuildPlan(ctx, stmt, false)
				require.NoError(t, buildErr)
				return built.GetDdl().GetCreateTable().GetTableDef()
			}
			source, target := build(true), build(false)
			require.False(t, sameTableDumpBoundExpression(source.Cols[0].Default.Expr, target.Cols[0].Default.Expr),
				"source=%s target=%s", source.Cols[0].Default.Expr, target.Cols[0].Default.Expr)
			data, err := proto.Marshal(source)
			require.NoError(t, err)
			var restored planpb.TableDef
			require.NoError(t, proto.Unmarshal(data, &restored))
			snapshot := proto.Clone(&restored)
			_, err = AnalyzeTableDumpBindings(compiler, &restored, &restored)
			require.NoError(t, err)
			_, err = AnalyzeTableDumpBindings(compiler, target, &restored)
			require.NoError(t, err)
			require.True(t, proto.Equal(snapshot, &restored))

			forged := proto.Clone(&restored).(*planpb.TableDef)
			forged.Cols[0].Default.Expr.Typ.Scale++
			_, err = AnalyzeTableDumpBindings(compiler, target, forged)
			require.ErrorContains(t, err, "cannot verify bound default expression a")
		})
	}
}
