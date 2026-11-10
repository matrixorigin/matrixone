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

package compile

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPrivateAlterCopyCreateAuthority(t *testing.T) {
	ctrl := gomock.NewController(t)
	c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
	c.proc.Ctx = defines.AttachAccountId(c.proc.Ctx, 7)
	c.db = "db"
	const sql = "create table db.replica (id int)"
	original := c.proc.Ctx
	create := &plan.CreateTable{Database: "db", TableDef: &plan.TableDef{Name: "replica"}}
	var escaped context.Context

	require.NoError(t, c.withPrivateAlterCopyCreate("db", "replica", sql, func() error {
		var err error
		escaped, err = authorizePrivateAlterCopyCreate(c.proc.Ctx, c.proc.GetTxnOperator(), sql)
		require.NoError(t, err)
		c.proc.Ctx = escaped
		ok, err := c.validatePrivateAlterCopyCreate(create)
		require.NoError(t, err)
		require.True(t, ok)

		for name, bad := range map[string]*plan.CreateTable{
			"nil":                nil,
			"database":           {Database: "other", TableDef: create.TableDef},
			"table":              {Database: "db", TableDef: &plan.TableDef{Name: "other"}},
			"missing definition": {Database: "db"},
			"temporary":          {Database: "db", TableDef: create.TableDef, Temporary: true},
			"replace":            {Database: "db", TableDef: create.TableDef, Replace: true},
			"if not exists":      {Database: "db", TableDef: create.TableDef, IfNotExists: true},
			"ctas":               {Database: "db", TableDef: create.TableDef, CreateAsSelectSql: "select 1"},
		} {
			t.Run(name, func(t *testing.T) {
				_, err := c.validatePrivateAlterCopyCreate(bad)
				require.Error(t, err)
			})
		}
		_, err = authorizePrivateAlterCopyCreate(c.proc.Ctx, mock_frontend.NewMockTxnOperator(ctrl), sql)
		require.Error(t, err)
		_, err = authorizePrivateAlterCopyCreate(c.proc.Ctx, c.proc.GetTxnOperator(), sql)
		require.Error(t, err, "the same CREATE authority must not be reusable")

		child, err := authorizePrivateAlterCopyCreate(c.proc.Ctx, c.proc.GetTxnOperator(), "create table db.other (id int)")
		require.NoError(t, err)
		c.proc.Ctx = child
		ok, err = c.validatePrivateAlterCopyCreate(create)
		require.NoError(t, err)
		require.False(t, ok, "a nested statement cannot inherit its parent's authority")
		c.proc.Ctx = defines.AttachAccountId(escaped, 8)
		_, err = c.validatePrivateAlterCopyCreate(create)
		require.Error(t, err)
		return nil
	}))
	require.Same(t, original, c.proc.Ctx)
	c.proc.Ctx = escaped
	_, err := c.validatePrivateAlterCopyCreate(create)
	require.Error(t, err, "a closed receipt cannot authorize later work")
	_, err = authorizePrivateAlterCopyCreate(escaped, c.proc.GetTxnOperator(), sql)
	require.Error(t, err)
	c.proc.Ctx = original

	want := errors.New("injected")
	require.ErrorIs(t, c.withPrivateAlterCopyCreate("db", "replica", sql, func() error { return want }), want)
	require.Same(t, original, c.proc.Ctx)
	require.PanicsWithValue(t, "injected", func() {
		_ = c.withPrivateAlterCopyCreate("db", "replica", sql, func() error { panic("injected") })
	})
	require.Same(t, original, c.proc.Ctx)
}
