// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package compile

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewExecutionCapability(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	op.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("execution")}).AnyTimes()
	proc.Base.TxnOperator = op
	c := &Compile{proc: proc}
	def := &plan.TableDef{TableType: catalog.SystemMaterializedViewRel, TblId: 99}
	require.Error(t, c.validateMaterializedViewWrite(def))
	proc.Ctx = defines.WithMaterializedViewWrite(proc.Ctx, 7, 99, []byte("binding"))
	require.Error(t, c.validateMaterializedViewWrite(def))
	proc.Ctx = defines.WithMaterializedViewWrite(proc.Ctx, 7, 99, []byte("execution"))
	require.NoError(t, c.validateMaterializedViewWrite(def))
	def.TblId = 100
	require.Error(t, c.validateMaterializedViewWrite(def))
	def.TableType = catalog.SystemOrdinaryRel
	require.NoError(t, c.validateMaterializedViewWrite(def))
}

type materializedViewNestedExecutor struct {
	executor.SQLExecutor
	t  *testing.T
	op client.TxnOperator
}

func (e *materializedViewNestedExecutor) ExecTxn(_ context.Context, f func(executor.TxnExecutor) error, opts executor.Options) error {
	require.True(e.t, opts.DisableIncrStatement(), "nested catalog work must not start a second active statement")
	require.Same(e.t, e.op, opts.Txn())
	return f(executor.NewMemTxnExecutor(func(string) (executor.Result, error) { return executor.Result{}, nil }, e.op))
}
func TestMaterializedViewNestedStatementOwnership(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	proc.Base.TxnOperator = op
	inner := &materializedViewNestedExecutor{t: t, op: op}
	installDropDDLExecutor(t, proc, inner)
	c := &Compile{proc: proc, db: "db"}
	require.NoError(t, c.mvTxn(func(tx executor.TxnExecutor) error {
		require.Same(t, op, tx.Txn())
		r, err := tx.Exec("select 1", executor.StatementOption{})
		if err == nil {
			r.Close()
		}
		return err
	}))
}
