package compile

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type spySQLExecutor struct {
	ctx context.Context
}

func (s *spySQLExecutor) Exec(
	ctx context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	s.ctx = ctx
	return executor.Result{}, nil
}

func (s *spySQLExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	s.ctx = ctx
	return nil
}

func TestCompileRunSqlWithResultAndOptionsUsesTopContextWhenProcCtxNil(t *testing.T) {
	proc := testutil.NewProcess(t)
	topCtx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.ReplaceTopCtx(topCtx)
	proc.Ctx = nil

	spyExec := &spySQLExecutor{}
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)

	c := &Compile{proc: proc, pn: &pbplan.Plan{}}

	_, err := c.runSqlWithResultAndOptions("select 1", NoAccountId, executor.StatementOption{})
	require.NoError(t, err)
	require.NotNil(t, spyExec.ctx)

	accountID, err := defines.GetAccountId(spyExec.ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(catalog.System_Account), accountID)
}

func TestSQLExecutorExecTxnHandlesNilContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	s := &sqlExecutor{}

	err := s.ExecTxn(nil, func(executor.TxnExecutor) error {
		return nil
	}, executor.Options{}.WithTxn(txnOp))
	require.NoError(t, err)
}
