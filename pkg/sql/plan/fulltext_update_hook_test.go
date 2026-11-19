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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestFulltextUpdateHookKeepsUpdateAndODKUInSync(t *testing.T) {
	ref := func(pos int32, name string) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos, Name: name}}}
	}
	index := &planpb.IndexDef{IndexName: "ft", IndexTableName: "ft_hidden", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), Parts: []string{"body"}, TableExist: true}
	table := &planpb.TableDef{
		FeatureFlag: features.Partitioned,
		Cols: []*planpb.ColDef{
			{Name: "id"}, {Name: "source"},
			{Name: "route", GeneratedCol: &planpb.GeneratedCol{Expr: ref(1, "source")}},
			{Name: "body"}, {Name: "note"},
		},
		Name2ColIndex: map[string]int32{"id": 0, "source": 1, "route": 2, "body": 3, "note": 4},
		Pkey:          &planpb.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Partition:     &planpb.Partition{PartitionDefs: []*planpb.PartitionDef{{Def: ref(2, "t.route")}}},
		Indexes:       []*planpb.IndexDef{index},
	}
	for _, tc := range []struct {
		name     string
		column   string
		affected bool
	}{
		{"primary key", "id", true},
		{"partition column", "route", true},
		{"generated partition source", "source", true},
		{"indexed column", "body", true},
		{"unrelated column", "note", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inline, unsupported, err := classifyIrregularIndexesForUpdate(context.Background(), table, map[string]tree.Expr{tc.column: nil})
			require.NoError(t, err)
			require.False(t, unsupported)
			affected, insertOnly, err := splitIrregularIndexesByUpdatedColumns(table, []*planpb.IndexDef{index}, map[string]struct{}{tc.column: {}})
			require.NoError(t, err)
			if tc.affected {
				require.Equal(t, []*planpb.IndexDef{index}, inline)
				require.Equal(t, []*planpb.IndexDef{index}, affected)
				require.Empty(t, insertOnly)
			} else {
				require.Empty(t, inline)
				require.Empty(t, affected)
				require.Equal(t, []*planpb.IndexDef{index}, insertOnly)
			}
		})
	}

	automatic := *table
	automatic.Cols = append([]*planpb.ColDef(nil), table.Cols...)
	source := *table.Cols[1]
	source.OnUpdate = &planpb.OnUpdate{}
	automatic.Cols[1] = &source
	inline, unsupported, err := classifyIrregularIndexesForUpdate(context.Background(), &automatic, map[string]tree.Expr{"note": nil})
	require.NoError(t, err)
	require.False(t, unsupported)
	require.Equal(t, []*planpb.IndexDef{index}, inline)
	affected, insertOnly, err := splitIrregularIndexesByUpdatedColumns(&automatic, []*planpb.IndexDef{index}, map[string]struct{}{"note": {}})
	require.NoError(t, err)
	require.Equal(t, []*planpb.IndexDef{index}, affected)
	require.Empty(t, insertOnly)
}
