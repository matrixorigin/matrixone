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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestAlterCopyCloneRequiresFinalValuesAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*TableDef, *AlterTableContext)
		want   bool
	}{
		{"unchanged", func(*TableDef, *AlterTableContext) {}, true},
		{"type provenance only", func(d *TableDef, _ *AlterTableContext) { d.Cols[0].Typ.Table = "" }, true},
		{"primary encoding changed", func(d *TableDef, _ *AlterTableContext) { d.Cols[0].Typ.Id = int32(types.T_int64) }, false},
		{"index column replaced", func(d *TableDef, c *AlterTableContext) {
			c.alterColMap["v"] = selectExpr{sexprType: exprConstValue, sexprStr: "7"}
		}, false},
		{"index replaced under same name", func(d *TableDef, _ *AlterTableContext) { d.Indexes[0].IndexTableName = "new_hidden" }, false},
		{"prefix changed", func(d *TableDef, _ *AlterTableContext) { d.Indexes[0].IndexAlgoParams = `{"prefix_lengths":"v:2"}` }, false},
		{"primary reordered", func(d *TableDef, _ *AlterTableContext) { d.Pkey.Names = []string{"v", "id"} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := &TableDef{Name: "src", Cols: []*ColDef{
				{Name: "id", ColId: 0, Seqnum: 1, Typ: Type{Id: int32(types.T_int32), NotNullable: true, Table: "src"}},
				{Name: "v", ColId: 1, Seqnum: 2, Typ: Type{Id: int32(types.T_varchar), Width: 20}},
			}, Pkey: &PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}, Indexes: []*pb.IndexDef{{IndexName: "v", IndexTableName: "hidden", Parts: []string{"v", catalog.CreateAlias("id")}}}}
			target := DeepCopyTableDef(old, true)
			target.Name = "copy"
			c := initAlterTableContext(old, target, "db")
			tc.change(target, c)
			locator := alterCopyPrimaryKeyUnchanged(old, target, c, nil)
			require.Equal(t, tc.want, alterCopyCloneIndexes(old, target, c, nil, locator)["v"])
		})
	}
}

func TestAlterCopyClonesUnchangedCompositeLocator(t *testing.T) {
	old := &TableDef{Name: "src", Cols: []*ColDef{
		{Name: "a", ColId: 0, Seqnum: 1, Typ: Type{Id: int32(types.T_int32), NotNullable: true}},
		{Name: "b", ColId: 1, Seqnum: 2, Typ: Type{Id: int32(types.T_int32), NotNullable: true}},
		{Name: "v", ColId: 2, Seqnum: 3, Typ: Type{Id: int32(types.T_int32)}},
		{Name: catalog.CPrimaryKeyColName, ColId: 3, Seqnum: 4, Hidden: true, Typ: Type{Id: int32(types.T_varbinary)}},
	}, Pkey: &PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"a", "b"}},
		Indexes: []*pb.IndexDef{{IndexName: "v", IndexTableName: "hidden", Parts: []string{"v", catalog.CreateAlias(catalog.CPrimaryKeyColName)}}}}
	target := DeepCopyTableDef(old, true)
	target.Name = "copy"
	ctx := initAlterTableContext(old, target, "db")
	locator := alterCopyPrimaryKeyUnchanged(old, target, ctx, nil)
	require.True(t, locator)
	require.True(t, alterCopyCloneIndexes(old, target, ctx, nil, locator)["v"])
}

func TestCopyIndexMaintenanceKeepsDedupSeparate(t *testing.T) {
	for _, tc := range []struct {
		name, algo          string
		unique, clone, want bool
	}{
		{"affected regular", catalog.MoIndexDefaultAlgo.ToString(), false, false, false},
		{"affected unique", catalog.MoIndexDefaultAlgo.ToString(), true, false, false},
		{"affected master", catalog.MOIndexMasterAlgo.ToString(), false, false, false},
		{"cloned unique", catalog.MoIndexDefaultAlgo.ToString(), true, true, true},
		{"affected plugin", catalog.MOIndexFullTextAlgo.ToString(), false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx := &pb.IndexDef{IndexName: "idx", Unique: tc.unique, IndexAlgo: tc.algo}
			opts := &pb.AlterCopyOpt{SkipUniqueIdxDedup: map[string]bool{"idx": true}, SkipIndexesCopy: map[string]bool{"idx": tc.clone}}
			require.Equal(t, tc.want, skipAlterCopyIndexMaintenance(opts, idx))
			require.False(t, skipAlterCopyIndexMaintenance(nil, idx), "ordinary INSERT still owns its maintenance")
		})
	}
}
