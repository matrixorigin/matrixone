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
	"testing"

	"github.com/gogo/protobuf/proto"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/stretchr/testify/require"
)

func TestNativePADReplacementRevalidatesInheritedIndex(t *testing.T) {
	for _, index := range []string{"", ", key k(s)", ", unique key k(s)"} {
		for _, rename := range []bool{false, true} {
			t.Run(fmt.Sprintf("index=%s/rename=%v", index, rename), func(t *testing.T) {
				ctx, build := nativePADDDLContext(t)
				p, err := build("create table n (id int primary key, s varchar(8) collate utf32_bin" + index + ")")
				require.NoError(t, err)
				source := p.GetDdl().GetCreateTable().TableDef
				source.TblId = 1001
				for i, col := range source.Cols {
					col.ColId = uint64(i + 1)
				}
				ctx.tables["n"] = source
				ctx.objects["n"] = &ObjectRef{Obj: 1001, SchemaName: "tpch", ObjName: "n"}
				if len(source.Indexes) > 0 {
					mock := &MockOptimizer{ctxt: *ctx}
					registerAlterIndexVisibilityRows(t, mock, source)
					ctx.internalSQLExecutor = mock.ctxt.internalSQLExecutor
				}
				before := proto.Clone(source)
				clause := "drop column s, add column s varchar(8) collate utf8mb4_general_ci"
				if rename {
					clause = "drop column s, add column fresh varchar(8) collate utf8mb4_general_ci, rename column fresh to s"
				}
				p, err = build("alter table n " + clause)
				if index != "" {
					require.ErrorContains(t, err, "native PAD collation physical key on column s is disabled")
					require.Nil(t, p)
				} else {
					require.NoError(t, err)
					column := FindColumn(p.GetDdl().GetAlterTable().CopyTableDef.Cols, "s")
					require.NotNil(t, column)
					require.Equal(t, uint32(1), column.Typ.CollationVersion)
					require.NotEqual(t, FindColumn(source.Cols, "s").ColId, column.ColId,
						"继承索引的名字不应把fresh数据列变成旧来源")
				}
				require.True(t, proto.Equal(before, source))
			})
		}
	}
}

func TestNativePADSharedCTEPublicationKeepsFullType(t *testing.T) {
	for _, revision := range []uint32{0, 1} {
		t.Run(fmt.Sprint(revision), func(t *testing.T) {
			mock := nativePADSetOperationMock(t, 3)
			FindColumn(mock.ctxt.tables["nation"].Cols, "n_name").Typ.CollationVersion = revision
			p, err := runOneStmt(mock, t, `with r as (
				select n_regionkey g, sum(n_nationkey) discarded, max(n_name) s, count(*) n
				from nation group by n_regionkey)
				select a.g,a.s,b.s from r a join r b on a.g=b.g`)
			require.NoError(t, err)
			columns := GetResultColumnsFromPlan(p)
			require.Len(t, columns, 3)
			for _, col := range columns[1:] {
				require.Equal(t, uint32(3), col.Typ.Charset)
				require.Equal(t, revision, col.Typ.CollationVersion)
			}
			features, err := pb.RequiredRemoteExpressionFeatures(p)
			require.NoError(t, err)
			require.Equal(t, revision == 1, features.NativePADCollation)
			query := p.GetQuery()
			shared := 0
			for _, node := range query.Nodes {
				if node.NodeType != pb.Node_SINK || node.ExtraOptions != materialized.CTESinkOption {
					continue
				}
				shared++
				child := query.Nodes[node.Children[0]]
				require.Equal(t, pb.Node_PROJECT, child.NodeType)
				require.Len(t, node.ProjectList, 2)
				require.Equal(t, node.ProjectList[1].Typ, child.ProjectList[1].Typ)
				require.Equal(t, revision, node.ProjectList[1].Typ.CollationVersion)
			}
			require.Equal(t, 1, shared, "必须实际覆盖共享CTE发布投影，而不是inline退路")
		})
	}
}
