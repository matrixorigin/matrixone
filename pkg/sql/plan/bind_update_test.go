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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestIrregularIndexAffectedByUpdate(t *testing.T) {
	tableDef := &TableDef{
		Pkey: &PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
		Indexes: []*IndexDef{
			{
				IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexName:       "idx",
				IndexTableName:  "idx_entries",
				TableExist:      true,
				Parts:           []string{"vec"},
				IncludedColumns: []string{"title"},
			},
		},
	}

	for _, tt := range []struct {
		name string
		col  string
		want bool
	}{
		{name: "indexed part", col: "vec", want: true},
		{name: "included column", col: "title", want: true},
		{name: "unrelated column", col: "id", want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			affected, err := irregularIndexAffectedByUpdate(
				tableDef, tableDef.Indexes[0], map[string]tree.Expr{tt.col: nil})
			require.NoError(t, err)
			require.Equal(t, tt.want, affected)
		})
	}
}

func TestClassifyIrregularIndexesForUpdate(t *testing.T) {
	newTableDef := func(indexes ...*IndexDef) *TableDef {
		return &TableDef{
			Pkey:    &PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
			Indexes: indexes,
		}
	}
	newIndex := func(name, algo, params string, parts ...string) *IndexDef {
		return &IndexDef{
			IndexName:       name,
			IndexTableName:  name + "_table",
			IndexAlgo:       algo,
			IndexAlgoParams: params,
			Parts:           parts,
			TableExist:      true,
		}
	}

	tests := []struct {
		name       string
		tableDef   *TableDef
		updateCols map[string]tree.Expr
		wantInline int
		wantLegacy bool
		wantReject bool
	}{
		{
			name:       "synchronous ivfflat indexed part",
			tableDef:   newTableDef(newIndex("ivf", catalog.MoIndexIvfFlatAlgo.ToString(), "", "vec")),
			updateCols: map[string]tree.Expr{"vec": nil},
			wantInline: 1,
		},
		{
			name: "synchronous ivfflat include column keeps whole hidden group",
			tableDef: newTableDef(
				func() *IndexDef {
					idx := newIndex("ivf", catalog.MoIndexIvfFlatAlgo.ToString(), "", "vec")
					idx.IncludedColumns = []string{"title"}
					return idx
				}(),
			),
			updateCols: map[string]tree.Expr{"title": nil},
			wantInline: 1,
		},
		{
			name:       "unrelated column",
			tableDef:   newTableDef(newIndex("ivf", catalog.MoIndexIvfFlatAlgo.ToString(), "", "vec")),
			updateCols: map[string]tree.Expr{"note": nil},
		},
		{
			name: "implicit on update indexed column",
			tableDef: func() *TableDef {
				tableDef := newTableDef(newIndex("ft", catalog.MOIndexFullTextAlgo.ToString(), "", "updated_at"))
				tableDef.Cols = []*ColDef{
					{Name: "id"},
					{Name: "note"},
					{Name: "updated_at", OnUpdate: &planpb.OnUpdate{}},
				}
				tableDef.Name2ColIndex = map[string]int32{"id": 0, "note": 1, "updated_at": 2}
				return tableDef
			}(),
			updateCols: map[string]tree.Expr{"note": nil},
			wantInline: 1,
		},
		{
			name:       "async ivfflat is cdc only",
			tableDef:   newTableDef(newIndex("ivf", catalog.MoIndexIvfFlatAlgo.ToString(), `{"async":"true"}`, "vec")),
			updateCols: map[string]tree.Expr{"vec": nil},
		},
		{
			name:       "always async hnsw is cdc only",
			tableDef:   newTableDef(newIndex("hnsw", catalog.MoIndexHnswAlgo.ToString(), "", "vec")),
			updateCols: map[string]tree.Expr{"vec": nil},
		},
		{
			name:       "synchronous fulltext",
			tableDef:   newTableDef(newIndex("ft", catalog.MOIndexFullTextAlgo.ToString(), "", "body")),
			updateCols: map[string]tree.Expr{"body": nil},
			wantInline: 1,
		},
		{
			name:       "synchronous fulltext primary key remains rejected",
			tableDef:   newTableDef(newIndex("ft", catalog.MOIndexFullTextAlgo.ToString(), "", "body")),
			updateCols: map[string]tree.Expr{"id": nil},
			wantReject: true,
		},
		{
			name:       "async primary key stays modern",
			tableDef:   newTableDef(newIndex("ft", catalog.MOIndexFullTextAlgo.ToString(), `{"async":"true"}`, "body")),
			updateCols: map[string]tree.Expr{"id": nil},
		},
		{
			name:       "synchronous master uses modern maintenance",
			tableDef:   newTableDef(newIndex("master", catalog.MOIndexMasterAlgo.ToString(), "", "body")),
			updateCols: map[string]tree.Expr{"body": nil},
			wantInline: 1,
		},
		{
			name:       "synchronous master primary key uses old-key maintenance",
			tableDef:   newTableDef(newIndex("master", catalog.MOIndexMasterAlgo.ToString(), "", "body")),
			updateCols: map[string]tree.Expr{"id": nil},
			wantInline: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inline, legacy, err := classifyIrregularIndexesForUpdate(context.Background(), tt.tableDef, tt.updateCols)
			if tt.wantReject {
				require.Error(t, err)
				var routeErr *updatePlannerRouteError
				require.True(t, errors.As(err, &routeErr))
				require.Equal(t, updatePlannerRejected, routeErr.route)
				require.True(t, moerr.IsMoErrCode(routeErr.err, moerr.ErrUnsupportedDML))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantLegacy, legacy)
			require.Len(t, inline, tt.wantInline)
		})
	}
}

func TestPrimaryKeyUpdatedDetectsSingleAndCompositeKeys(t *testing.T) {
	tests := []struct {
		name       string
		tableDef   *TableDef
		updateCols map[string]tree.Expr
		want       bool
	}{
		{
			name: "single primary key updated",
			tableDef: &TableDef{
				Pkey: &PrimaryKeyDef{
					Names:       []string{"id"},
					PkeyColName: "id",
				},
			},
			updateCols: map[string]tree.Expr{"id": nil},
			want:       true,
		},
		{
			name: "composite primary key part updated",
			tableDef: &TableDef{
				Pkey: &PrimaryKeyDef{
					Names:       []string{"tenant_id", "id"},
					PkeyColName: catalog.CPrimaryKeyColName,
				},
			},
			updateCols: map[string]tree.Expr{"id": nil},
			want:       true,
		},
		{
			name: "unrelated column updated",
			tableDef: &TableDef{
				Pkey: &PrimaryKeyDef{
					Names:       []string{"id"},
					PkeyColName: "id",
				},
			},
			updateCols: map[string]tree.Expr{"title": nil},
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, primaryKeyUpdated(tt.tableDef, tt.updateCols))
		})
	}
}
