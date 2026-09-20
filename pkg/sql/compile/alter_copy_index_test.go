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

package compile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestAlterCopyIndexMetadataHandoff(t *testing.T) {
	source := &plan.TableDef{Indexes: []*plan.IndexDef{{IndexName: "final", IndexAlgoParams: `{"session_vars":{"x":1}}`}}}
	option := alterCopyCreateOptions(&plan.AlterTable{TableDef: &plan.TableDef{}, CopyTableDef: source})
	require.Same(t, source, option.AlterCopySourceTable())
	ctx := withAlterCopySourceTable(context.Background(), option)
	require.Same(t, source, ctx.Value(defines.AlterCopySourceTableKey{}))
	nested := withAlterCopySourceTable(ctx, executor.StatementOption{})
	got, ok := nested.Value(defines.AlterCopySourceTableKey{}).(*plan.TableDef)
	require.True(t, ok)
	require.Nil(t, got)
	require.Same(t, source, ctx.Value(defines.AlterCopySourceTableKey{}))
}

func TestAlterCopyIndexRenameForeignKeys(t *testing.T) {
	renamed := map[string]string{"a": "b", "b": "a"}
	source := []*plan.ForeignKeyDef{
		{ForeignTbl: 0, ReferencedIndexName: "A"},
		{ForeignTbl: 10, ReferencedIndexName: "b"},
		{ForeignTbl: 20, ReferencedIndexName: "a"},
		{ForeignTbl: 0, ReferencedIndexName: ""},
		{ForeignTbl: 0, ReferencedIndexName: "PRIMARY"},
		{ForeignTbl: 0, ReferencedIndexName: "other"},
	}
	remapped, _, err := remapAlterCopyForeignKeyState(context.Background(), source, nil, nil, 10, renamed)
	require.NoError(t, err)
	for i, expected := range []string{"b", "a", "a", "", "PRIMARY", "other"} {
		require.Equal(t, expected, remapped[i].ReferencedIndexName)
	}
	require.Equal(t, "A", source[0].ReferencedIndexName)
	require.Equal(t, "b", source[1].ReferencedIndexName)

	keys := []*plan.ForeignKeyDef{
		{ForeignTbl: 10, ReferencedIndexName: "a"},
		{ForeignTbl: 10, ReferencedIndexName: "B"},
		{ForeignTbl: 20, ReferencedIndexName: "a"},
		{ForeignTbl: 10, ReferencedIndexName: ""},
		{ForeignTbl: 10, ReferencedIndexName: "PRIMARY"},
	}
	constraint := &engine.ConstraintDef{Cts: []engine.Constraint{&engine.ForeignKeyDef{Fkeys: keys}}}
	changed, err := rewriteForeignKeyReferencesForAlterCopy(context.Background(), constraint, nil, 10, 11, renamed)
	require.NoError(t, err)
	require.True(t, changed)
	for i, expected := range []string{"b", "a", "a", "", "PRIMARY"} {
		require.Equal(t, expected, keys[i].ReferencedIndexName)
	}
	require.Equal(t, uint64(20), keys[2].ForeignTbl)
	changed, err = rewriteForeignKeyReferencesForAlterCopy(context.Background(), constraint, nil, 10, 11, renamed)
	require.NoError(t, err)
	require.False(t, changed)
}

func TestAlterCopyIndexRenamePublicationError(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	relation := mock_frontend.NewMockRelation(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	c := NewCompile("test", "test", "", "", "", eng, proc, nil, false, nil, time.Now())
	constraint := &engine.ConstraintDef{}
	stub := gostub.Stub(&GetConstraintDef, func(context.Context, engine.Relation) (*engine.ConstraintDef, error) {
		return constraint, nil
	})
	t.Cleanup(stub.Reset)
	failure := errors.New("publication failed")
	relation.EXPECT().UpdateConstraint(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, ct *engine.ConstraintDef) error {
		var published []*plan.ForeignKeyDef
		for _, def := range ct.Cts {
			if fk, ok := def.(*engine.ForeignKeyDef); ok {
				published = append(published, fk.Fkeys...)
			}
		}
		require.Len(t, published, 1)
		require.Equal(t, "new", published[0].ReferencedIndexName)
		return failure
	}).Times(1)
	source := []*plan.ForeignKeyDef{{ForeignTbl: 0, ReferencedIndexName: "old"}}
	err := applyAlterCopyForeignKeyState(c, relation, source, []uint64{30}, nil, 10, 11, map[string]string{"old": "new"})
	require.ErrorIs(t, err, failure)
	require.Equal(t, "old", source[0].ReferencedIndexName)
	// No child lookup may occur after replacement constraint publication fails.
}

func TestAlterCopyIndexRenameCatalogSQL(t *testing.T) {
	require.Empty(t, alterCopyIndexRenameCatalogSQL("db", "t", nil))
	require.Empty(t, alterCopyIndexRenameCatalogSQL("db", "t", map[string]string{"": "a", "PRIMARY": "x", "a": "a"}))
	require.Equal(t,
		"update `mo_catalog`.`mo_foreign_keys` set referenced_index_name = case lower(referenced_index_name) when 'a' then 'b' when 'b' then 'a' else referenced_index_name end where refer_db_name = 'db' and refer_table_name = 't'",
		alterCopyIndexRenameCatalogSQL("db", "t", map[string]string{"a": "b", "b": "a"}))
	require.Equal(t,
		"update `mo_catalog`.`mo_foreign_keys` set referenced_index_name = case lower(referenced_index_name) when 'a''\\\\' then 'b''\\\\' else referenced_index_name end where refer_db_name = 'd''\\\\' and refer_table_name = 't''\\\\'",
		alterCopyIndexRenameCatalogSQL("d'\\", "t'\\", map[string]string{"A'\\": "b'\\"}))
}

func TestAlterCopyIndexRenameIncomingPublication(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "publication error"}[fail], func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			child := mock_frontend.NewMockRelation(ctrl)
			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(30)).Return("db", "child", child, nil).Times(1)
			keys := []*plan.ForeignKeyDef{
				{ForeignTbl: 10, ReferencedIndexName: "a"},
				{ForeignTbl: 10, ReferencedIndexName: "b"},
				{ForeignTbl: 20, ReferencedIndexName: "a"},
			}
			constraint := &engine.ConstraintDef{Cts: []engine.Constraint{&engine.ForeignKeyDef{Fkeys: keys}}}
			stub := gostub.Stub(&GetConstraintDef, func(_ context.Context, got engine.Relation) (*engine.ConstraintDef, error) {
				require.Same(t, child, got)
				return constraint, nil
			})
			t.Cleanup(stub.Reset)
			var failure error
			if fail {
				failure = errors.New("child publication failed")
			}
			child.EXPECT().UpdateConstraint(gomock.Any(), constraint).DoAndReturn(func(context.Context, *engine.ConstraintDef) error {
				require.Equal(t, "b", keys[0].ReferencedIndexName)
				require.Equal(t, "a", keys[1].ReferencedIndexName)
				require.Equal(t, "a", keys[2].ReferencedIndexName)
				require.Equal(t, uint64(11), keys[0].ForeignTbl)
				require.Equal(t, uint64(20), keys[2].ForeignTbl)
				return failure
			}).Times(1)
			c := NewCompile("test", "db", "", "", "", eng, proc, nil, false, nil, time.Now())
			err := reconcileAlterCopyChildForeignKeyReferences(c, nil, []uint64{30, 30}, 10, 11, map[string]string{"a": "b", "b": "a"})
			if fail {
				require.ErrorIs(t, err, failure)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
