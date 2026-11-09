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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

type temporaryDestinationTestContext struct {
	*MockCompilerContext
	tempDef         *TableDef
	tempErr         error
	persistentErr   error
	tempCalls       int
	persistentCalls int
}

func (c *temporaryDestinationTestContext) Resolve(db, name string, snapshot *Snapshot, borrowOutput *TableResolution) (*ObjectRef, *TableDef, error) {
	borrowOutput.Reset()
	if name == "destination" {
		c.persistentCalls++
		return nil, nil, c.persistentErr
	}
	return c.MockCompilerContext.Resolve(db, name, snapshot, nil)
}

func (c *temporaryDestinationTestContext) ResolveTemporaryTable(db, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	c.tempCalls++
	if c.tempErr != nil {
		return nil, nil, c.tempErr
	}
	if c.tempDef == nil {
		return nil, nil, nil
	}
	return &ObjectRef{SchemaName: db, ObjName: name, Obj: int64(c.tempDef.TblId)}, c.tempDef, nil
}

func TestTemporaryRenameDestinationUsesSessionNamespace(t *testing.T) {
	protected := errors.New("persistent destination denied")
	tempDenied := errors.New("temporary index denied")
	for _, tc := range []struct {
		name            string
		sourceTemporary bool
		tempDef         *TableDef
		tempErr         error
		wantErr         error
		wantExists      bool
		wantTempCalls   int
		wantPersistent  int
	}{
		{name: "shadows protected persistent", sourceTemporary: true, wantTempCalls: 1},
		{name: "occupied temporary", sourceTemporary: true, tempDef: &TableDef{Name: "destination", IsTemporary: true}, wantExists: true, wantTempCalls: 1},
		{name: "protected temporary", sourceTemporary: true, tempErr: tempDenied, wantErr: tempDenied, wantTempCalls: 1},
		{name: "persistent source remains guarded", wantErr: protected, wantPersistent: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newAutoIncrementAlterOptimizer(t)
			source := mock.ctxt.tables["auto_incr_t"]
			source.IsTemporary = tc.sourceTemporary
			if tc.sourceTemporary {
				source.TableType = catalog.SystemTemporaryTable
			}
			ctx := &temporaryDestinationTestContext{MockCompilerContext: &mock.ctxt, tempDef: tc.tempDef, tempErr: tc.tempErr, persistentErr: protected}
			stmt, err := mysql.ParseOne(ctx.GetContext(), "alter table constraint_test.auto_incr_t rename to destination", 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false, nil)
			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			case tc.wantExists:
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTableAlreadyExists), "%v", err)
			default:
				require.NoError(t, err)
				require.Equal(t, "destination", p.GetDdl().GetAlterTable().Actions[0].GetAlterName().NewName)
			}
			require.Equal(t, tc.wantTempCalls, ctx.tempCalls)
			require.Equal(t, tc.wantPersistent, ctx.persistentCalls)
		})
	}
}

func TestTemporaryCreateAndPreparedDestinationDependencies(t *testing.T) {
	mock := newAutoIncrementAlterOptimizer(t)
	ctx := &temporaryDestinationTestContext{MockCompilerContext: &mock.ctxt, persistentErr: errors.New("persistent destination denied")}
	stmt, err := mysql.ParseOne(ctx.GetContext(), "create temporary table constraint_test.destination(id int)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	require.Zero(t, ctx.persistentCalls)
	_, err = collectPrepareDdlSchemas(ctx, stmt, p)
	require.NoError(t, err)
	require.Equal(t, 1, ctx.tempCalls)
	require.Zero(t, ctx.persistentCalls)

	mock.ctxt.tables["auto_incr_t"].IsTemporary = true
	mock.ctxt.tables["auto_incr_t"].TableType = catalog.SystemTemporaryTable
	alter, err := mysql.ParseOne(ctx.GetContext(), "alter table constraint_test.auto_incr_t rename to destination", 1)
	require.NoError(t, err)
	defer alter.Free()
	p, err = BuildPlan(ctx, alter, false, nil)
	require.NoError(t, err)
	_, err = collectPrepareDdlSchemas(ctx, alter, p)
	require.NoError(t, err)
	require.Equal(t, 3, ctx.tempCalls) // CREATE dependency, ALTER plan and dependency
	require.Zero(t, ctx.persistentCalls)
}
