// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestNativePADIndependentDDLOwners(t *testing.T) {
	for _, sql := range []string{
		"create index k_next on n(id)",
		"drop index k on n",
		"rename table n to renamed",
		"truncate table n",
	} {
		t.Run(sql, func(t *testing.T) {
			ctx, build := nativePADDDLContext(t)
			created, err := build("create table n (id int primary key, s varchar(8) collate utf8mb4_general_ci, key k(id))")
			require.NoError(t, err)
			table := created.GetDdl().GetCreateTable().TableDef
			ctx.tables["n"] = table
			ctx.objects["n"] = &ObjectRef{ObjName: "n", SchemaName: "tpch"}
			before := proto.Clone(table)
			_, err = build(sql)
			require.NoError(t, err)
			require.Equal(t, before, table, "planning must not mutate the catalog schema")
			rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
			_, err = build(sql)
			require.ErrorContains(t, err, "protocol version 130")
			require.Equal(t, before, table)
		})
	}
}

func TestNativePADPersistedExpressionsCannotHideNativeInputs(t *testing.T) {
	ctx, build := nativePADDDLContext(t)
	for _, sql := range []string{
		"create table g (s varchar(8) collate utf8mb4_general_ci, b bool as (s = 'a'))",
		"create table g (s varchar(8) collate utf8mb4_general_ci, check (s = 'a'))",
		"create table g (s varchar(8) collate utf8mb4_general_ci, index k ((cast(s as varchar(8)))))",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := build(sql)
			require.ErrorContains(t, err, "native PAD collation persistence is disabled")
		})
	}
	created, err := build("create table n (s varchar(8) collate utf8mb4_general_ci default 'x')")
	require.NoError(t, err, "ordinary defaults on a native column are not semantic computations")
	ctx.tables["n"] = created.GetDdl().GetCreateTable().TableDef
	for i, col := range ctx.tables["n"].Cols {
		col.ColId = uint64(i)
	}
	ctx.objects["n"] = &ObjectRef{ObjName: "n", SchemaName: "tpch"}
	_, err = build("create index k on n ((cast(s as varchar(8))))")
	require.ErrorContains(t, err, "native PAD collation persistence is disabled")
}

func TestNativePADCloneRetainsMixedGenerations(t *testing.T) {
	ctx, build := nativePADDDLContext(t)
	created, err := build("create table n (s varchar(8) collate utf8mb4_general_ci, b varchar(8) collate utf8mb4_bin, r varchar(8) character set utf8mb4)")
	require.NoError(t, err)
	source := created.GetDdl().GetCreateTable().TableDef
	ctx.tables["n"] = source
	ctx.objects["n"] = &ObjectRef{ObjName: "n", SchemaName: "tpch"}
	before := proto.Clone(source)
	cloned, err := build("create table copied clone tpch.n")
	require.NoError(t, err)
	clone := cloned.GetDdl().GetCloneTable()
	target := clone.GetCreateTable().GetDdl().GetCreateTable().TableDef
	for i, name := range []string{"s", "b", "r"} {
		require.Equal(t, name, target.Cols[i].Name)
		require.Equal(t, source.Cols[i].Typ, target.Cols[i].Typ)
	}
	require.Equal(t, source.DefaultCharset, target.DefaultCharset)
	require.Equal(t, source.CollationVersion, target.CollationVersion)
	require.Equal(t, before, source)
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		rt.SetGlobalVariables(key, defines.MORPCVersion120)
		_, err = build("create table copied clone tpch.n")
		require.ErrorContains(t, err, "protocol version 130", key)
		rt.SetGlobalVariables(key, defines.MORPCVersion130)
	}
}

func TestNativePADActionOnlyAdmission(t *testing.T) {
	ctx, _ := nativePADDDLContext(t)
	col := &planpb.ColDef{Name: "s", Typ: planpb.Type{Id: int32(types.T_varchar), Charset: 3, CollationVersion: 1}}
	source := &planpb.TableDef{Name: "n", Cols: []*planpb.ColDef{col}}
	create := &planpb.CreateIndex{TableDef: source, Index: &planpb.CreateTable{TableDef: &planpb.TableDef{Indexes: []*planpb.IndexDef{{Parts: []string{"s"}}}}}}
	before := proto.Clone(create)
	require.ErrorContains(t, RequireCreateIndexCollationsForAuthoring(ctx.GetContext(), ctx.GetProcess(), create), "native PAD")
	require.Equal(t, before, create)
	// A partial action can carry a native domain absent from both schema snapshots.
	alter := &planpb.AlterTable{TableDef: &planpb.TableDef{Name: "n"}, Actions: []*planpb.AlterTable_Action{{Action: &planpb.AlterTable_Action_AddColumn{AddColumn: &planpb.AlterAddColumn{Name: "s", Type: col.Typ}}}}}
	moruntime.ServiceRuntime(ctx.GetProcess().GetService()).SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
	require.ErrorContains(t, RequireAlterTableCollationsForAuthoring(ctx.GetContext(), ctx.GetProcess(), alter), "protocol version 130")
}

func TestNativePADIndependentIndexRejectsNativeKey(t *testing.T) {
	ctx, build := nativePADDDLContext(t)
	created, err := build("create table n (id int primary key, s varchar(8) collate utf8mb4_general_ci)")
	require.NoError(t, err)
	ctx.tables["n"] = created.GetDdl().GetCreateTable().TableDef
	ctx.objects["n"] = &ObjectRef{ObjName: "n", SchemaName: "tpch"}
	for _, sql := range []string{"create index k on n(s)", "create unique index k on n(s)"} {
		_, err = build(sql)
		require.ErrorContains(t, err, "native PAD")
	}
}
