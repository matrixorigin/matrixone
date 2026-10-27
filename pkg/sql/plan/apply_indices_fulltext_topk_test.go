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

package plan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ft2plan "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestConditionalFulltextTopKPublicBinding(t *testing.T) {
	match := "match(title) against('needle' in bm25 mode)"
	for _, tc := range []struct {
		name, projection, where, order, limit string
		mutate                                func(*planpb.TableDef)
		want                                  uint64
		patternParameter                      bool
		wantBindError                         bool
	}{
		{name: "original", want: 2},
		{name: "pattern parameter", patternParameter: true, want: 2},
		{name: "offset", limit: "limit 2 offset 1", want: 3},
		{name: "maximum", limit: "limit 8192", want: 8192},
		{name: "lossless score predicate", where: "cast(" + match + " as double)>0", want: 2},
		{name: "score ascending", order: "score asc,id"},
		{name: "pk descending", order: "score desc,id desc"},
		{name: "score only", order: "score desc"},
		{name: "unicode order expression", order: "score desc,id collate utf8mb4_unicode_ci"},
		{name: "residual source filter", where: match + ">0 and base_id='b1'"},
		{name: "positive threshold", where: match + ">0.1"},
		{name: "inclusive threshold", where: match + ">=0"},
		{name: "negative threshold", where: match + ">-1"},
		{name: "score transform", projection: "id,round(" + match + ",2) as score"},
		{name: "other match", where: match + ">0 and match(title) against('other' in bm25 mode)>0"},
		{name: "too many", limit: "limit 8193"},
		{name: "offset boundary", limit: "limit 8192 offset 1"},
		{name: "overflow", limit: "limit 18446744073709551615 offset 1"},
		{name: "zero limit", limit: "limit 0"},
		{name: "parameter limit", limit: "limit ?"},
		{name: "found rows", projection: "sql_calc_found_rows id," + match + " as score"},
		{name: "distinct", projection: "distinct id," + match + " as score"},
		{name: "window", projection: "id," + match + " as score,row_number() over()"},
		{name: "numeric pk", mutate: func(d *planpb.TableDef) { d.Cols[0].Typ.Id = int32(types.T_int64) }},
		{name: "legacy pk", mutate: func(d *planpb.TableDef) { d.Cols[0].Typ.Charset = uint32(types.CharsetLegacy) }, want: 2},
		{name: "binary collation pk", mutate: func(d *planpb.TableDef) { d.Cols[0].Typ.Charset = uint32(types.CharsetUTF8MB4Bin) }, want: 2},
		{name: "unknown collation identity", mutate: func(d *planpb.TableDef) { d.Cols[0].Typ.Charset = 255 }, wantBindError: true},
		{name: "future collation revision", mutate: func(d *planpb.TableDef) { d.Cols[0].Typ.CollationVersion = 1 }, wantBindError: true},
		{name: "unicode pk", mutate: func(d *planpb.TableDef) {
			d.Cols[0].Typ.Charset = uint32(types.CharsetUTF8MB4UnicodeCI)
			d.Cols[0].Typ.CollationVersion = 1
		}},
		{name: "composite pk", mutate: func(d *planpb.TableDef) { d.Pkey.Names = []string{"id", "base_id"} }},
		{name: "include index", mutate: func(d *planpb.TableDef) { d.Indexes[0].IncludedColumns = []string{"base_id"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, scanID, _ := fullTextOrderFixture(t, "fulltext2")
			table := b.qry.Nodes[scanID].TableDef
			table.Name = "ft_conditional"
			table.TblId = 10001
			pkType := types.New(types.T_varchar, 128, 0)
			pkType.SetNotNull(true)
			require.Equal(t, types.CharsetUTF8, pkType.Charset, "fixture must carry the live CREATE TABLE default")
			table.Cols[0].Typ = makePlan2Type(&pkType)
			table.Cols[0].Typ.NotNullable = pkType.GetNotNull()
			table.Pkey.Names = []string{"id"}
			for _, idx := range table.Indexes {
				registerFullTextJoinRegularIndexTable(b, idx.IndexTableName)
				idx.Parts = []string{"title"}
			}
			if tc.mutate != nil {
				tc.mutate(table)
			}
			ctx := b.compCtx.(*fullTextJoinMockCompilerContext)
			ctx.tables[table.Name] = table
			ctx.objects[table.Name] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
			projection, where, order, limit := tc.projection, tc.where, tc.order, tc.limit
			if projection == "" {
				projection = "id," + match + " as score"
			}
			if where == "" {
				where = match + ">0"
			}
			if order == "" {
				order = "score desc,id"
			}
			if limit == "" {
				limit = "limit 2"
			}
			sql := fmt.Sprintf("select %s from ft_conditional where %s order by %s %s", projection, where, order, limit)
			if tc.patternParameter {
				sql = "select id,score from (select id,match(title) against(? in bm25 mode) as score from ft_conditional) q where score>0 order by score desc,id limit 2"
			}
			statements, err := mysql.Parse(ctx.GetContext(), sql, 1)
			require.NoError(t, err)
			require.Len(t, statements, 1)
			defer statements[0].Free()
			p, err := BuildPlan(&fullTextOrderPythonUdfContext{ctx}, statements[0], strings.Contains(sql, "?"))
			if tc.wantBindError {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "unsupported catalog collation must be rejected before acceleration: %v", err)
				return
			}
			require.NoError(t, err)
			q := p.GetQuery()
			require.NotNil(t, q)
			scans := collectFullTextFunctionScans(&QueryBuilder{qry: q}, q.Steps[0])
			attached := 0
			for _, scan := range scans {
				opts, err := ft2plan.DecodeScanOptions(scan.IndexSearchScan.AlgoOptions)
				require.NoError(t, err)
				if opts.ConditionalStreamTopK != nil {
					attached++
					require.Equal(t, tc.want, opts.ConditionalStreamTopK.K)
					require.Equal(t, ft2plan.ConditionalStreamTopKVersion, opts.ConditionalStreamTopK.Semantics)
					require.Nil(t, scan.IndexSearchScan.CandidateLimit)
				}
			}
			if tc.want == 0 {
				require.Zero(t, attached)
			} else {
				require.Equal(t, 1, attached, "descriptor must reach public SQL plan: %s", sql)
				require.Equal(t, 1, countReachableNodeType(q, planpb.Node_JOIN))
				require.GreaterOrEqual(t, countReachableNodeType(q, planpb.Node_SORT), 1)
			}
		})
	}
}
