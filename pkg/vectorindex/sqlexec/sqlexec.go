package sqlexec

import (
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// run SQL in batch mode. Result batches will stored in memory and return once all result batches received.
func RunSql(proc *process.Process, sql string) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	//-------------------------------------------------------
	topContext := proc.GetTopContext()
	accountId, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return executor.Result{}, err
	}
	//-------------------------------------------------------

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(accountId)
	return exec.Exec(topContext, sql, opts)
}

// run SQL in WithStreaming() and pass the channel to SQL executor
func RunStreamingSql(proc *process.Process, sql string, stream_chan chan executor.Result, error_chan chan error) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	//-------------------------------------------------------
	topContext := proc.GetTopContext()
	accountId, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return executor.Result{}, err
	}
	//-------------------------------------------------------
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(accountId).
		WithStreaming(stream_chan, error_chan)
	return exec.Exec(topContext, sql, opts)
}
