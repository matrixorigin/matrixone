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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func newShowIndexIgnoredTestContext(t *testing.T) *MockCompilerContext {
	cc := NewMockCompilerContext(false, newPlanTestProcess(t))
	vt := &planpb.TableDef{
		Name:  "vt",
		TblId: 9001,
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64), NotNullable: true}, Primary: true},
			{Name: "v", Typ: planpb.Type{Id: int32(types.T_array_float32), Width: 2}},
		},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
		Pkey:          &planpb.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
	}
	for i, name := range []string{"i", "j"} {
		for _, typ := range []string{catalog.Hnsw_TblType_Metadata, catalog.Hnsw_TblType_Storage, catalog.IndexIgnored_TblType} {
			vt.Indexes = append(vt.Indexes, &planpb.IndexDef{
				IndexName: name, IndexAlgo: catalog.MoIndexHnswAlgo.ToString(), IndexAlgoTableType: typ,
				IndexTableName: "__mo_index_" + name + "_" + typ, Parts: []string{"v"}, TableExist: true,
			})
		}
		ignored := "__mo_index_" + name + "_" + catalog.IndexIgnored_TblType
		cc.tables[ignored] = &planpb.TableDef{
			Name:  ignored,
			TblId: uint64(9010 + i),
			Cols: []*planpb.ColDef{
				{Name: catalog.IndexIgnored_TblCol_Pk, Typ: planpb.Type{Id: int32(types.T_int64), NotNullable: true}, Primary: true},
				{Name: catalog.IndexIgnored_TblCol_Code, Typ: planpb.Type{Id: int32(types.T_int32)}},
				{Name: catalog.IndexIgnored_TblCol_Reason, Typ: planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
				{Name: catalog.IndexIgnored_TblCol_Ts, Typ: planpb.Type{Id: int32(types.T_int64)}},
			},
			Name2ColIndex: map[string]int32{catalog.IndexIgnored_TblCol_Pk: 0, catalog.IndexIgnored_TblCol_Code: 1, catalog.IndexIgnored_TblCol_Reason: 2, catalog.IndexIgnored_TblCol_Ts: 3},
			Pkey:          &planpb.PrimaryKeyDef{Names: []string{catalog.IndexIgnored_TblCol_Pk}, PkeyColName: catalog.IndexIgnored_TblCol_Pk},
		}
		cc.objects[ignored] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: ignored, Obj: int64(9010 + i)}
	}
	// an index created before ignored tables existed has none
	for _, typ := range []string{catalog.Hnsw_TblType_Metadata, catalog.Hnsw_TblType_Storage} {
		vt.Indexes = append(vt.Indexes, &planpb.IndexDef{
			IndexName: "old", IndexAlgo: catalog.MoIndexHnswAlgo.ToString(), IndexAlgoTableType: typ,
			IndexTableName: "__mo_index_old_" + typ, Parts: []string{"v"}, TableExist: true,
		})
	}
	cc.tables["vt"], cc.objects["vt"] = vt, &planpb.ObjectRef{SchemaName: "tpch", ObjName: "vt", Obj: 9001}
	return cc
}

func buildShowIndexIgnoredTestPlan(t *testing.T, sql string) (*planpb.Query, error) {
	cc := newShowIndexIgnoredTestContext(t)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(cc, stmt, false)
	if err != nil {
		return nil, err
	}
	return p.GetQuery(), nil
}

func showIndexIgnoredScans(query *planpb.Query) []string {
	var scans []string
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil {
			scans = append(scans, node.TableDef.Name)
		}
	}
	return scans
}

func TestShowIndexIgnored(t *testing.T) {
	query, err := buildShowIndexIgnoredTestPlan(t, "show index ignored from vt")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"__mo_index_i_ignored", "__mo_index_j_ignored"}, showIndexIgnoredScans(query))
	// the ignored tables are read on behalf of vt, whose privileges are checked
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_TABLE_SCAN {
			require.Equal(t, "vt", node.GetParentObjRef().GetObjName())
		}
	}
	require.Equal(t, []string{"Key_name", "Primary_key", "Error_code", "Reason", "Ignored_at"}, query.Headings)

	query, err = buildShowIndexIgnoredTestPlan(t, fmt.Sprintf("show index ignored from tpch.vt where Key_name = 'i' and Error_code = %d and Reason like '%%range%%'", moerr.ErrIndexVectorOutOfRange))
	require.NoError(t, err)
	require.NotEmpty(t, showIndexIgnoredScans(query))

	// a table whose indexes record no ignored rows has an empty result of the same columns
	query, err = buildShowIndexIgnoredTestPlan(t, "show index ignored from nation")
	require.NoError(t, err)
	require.Empty(t, showIndexIgnoredScans(query))
	require.Equal(t, []string{"Key_name", "Primary_key", "Error_code", "Reason", "Ignored_at"}, query.Headings)

	_, err = buildShowIndexIgnoredTestPlan(t, "show index ignored from nope")
	require.Error(t, err)
}

// A row without a message shows the message of its code.
func TestIgnoredReasonSql(t *testing.T) {
	sql := ignoredReasonSql()
	require.Contains(t, sql, "COALESCE(`__mo_index_reason`, CASE `__mo_index_code`")
	for _, code := range []uint16{moerr.ErrIndexVectorDimension, moerr.ErrIndexVectorNonFinite, moerr.ErrIndexVectorOutOfRange} {
		require.Contains(t, sql, fmt.Sprintf(" WHEN %d THEN '%s'", code, moerr.MessageOf(code)))
	}
}
