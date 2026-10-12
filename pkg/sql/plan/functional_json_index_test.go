// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFunctionalJSONTypedKeyDDLAndMetadata(t *testing.T) {
	proc := newPlanTestProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
		}
	})
	ctx := NewMockCompilerContext(false, proc)
	build := func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(ctx, stmt, false)
	}
	p, err := build("create table docs(id int primary key,doc json,index idx_kind((cast(doc ->> '$.kind' as char(20)))))")
	require.NoError(t, err)
	table := p.GetDdl().GetCreateTable().TableDef
	require.Len(t, table.Indexes, 1)
	required, err := pb.JSONFunctionalIndexSemantics(table.Indexes[0])
	require.NoError(t, err)
	require.True(t, required)
	col := functionalIndexPartColumn(table, table.Indexes[0], 0)
	require.NotNil(t, col)
	require.Equal(t, int32(20), col.Typ.Width)
	require.NoError(t, validateFunctionalTable(t.Context(), table))
	floor, err := RequiredPersistedExpressionProtocolVersion(table)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion115, floor)
	bad := DeepCopyTableDef(table, true)
	bad.Indexes[0].IndexAlgoParams = ""
	require.Error(t, validateFunctionalTable(t.Context(), bad))
	bad = DeepCopyTableDef(table, true)
	bad.Indexes[0].IndexAlgoParams = `{"functional_json_semantics":"2"}`
	require.Error(t, validateFunctionalTable(t.Context(), bad))
	for _, sql := range []string{
		"create table bad(id int primary key,doc json,index j((doc ->> '$.kind')))",
		"create table bad(id int primary key,doc json,index j((cast(doc ->> '$.*' as char(20)))))",
		"create table bad(id int primary key,ts timestamp,index j((cast(ts as char(20)))))",
	} {
		_, err = build(sql)
		require.Error(t, err, sql)
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion114)
	_, err = build("create table old_json(id int primary key,doc json,index j((cast(doc ->> '$.kind' as char(20)))))")
	require.Error(t, err)
	_, err = build("create table old_ordinary(id int primary key,name varchar(20),index j((lower(name))))")
	require.NoError(t, err)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
	_, err = build("create table child(id int primary key,pid int,index j((cast(json_unquote(json_extract(cast(pid as json),'$')) as char(20)))))")
	require.NoError(t, err)
}
