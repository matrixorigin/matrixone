package frontend

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"time"
)

var _ StmtExecutor = &selectExecutor{}

type selectExecutor struct {
	father *resultSetStmtExecutor
	sel    *tree.Select
}

func (se *selectExecutor) Run(ts uint64) (err error) {
	return se.father.Run(ts)
}

func (se *selectExecutor) GetAst() tree.Statement {
	return se.father.GetAst()
}

func (se *selectExecutor) SetDatabaseName(db string) error {
	return se.father.SetDatabaseName(db)
}

func (se *selectExecutor) GetColumns() ([]interface{}, error) {
	return se.father.GetColumns()
}

func (se *selectExecutor) GetAffectedRows() uint64 {
	return se.father.GetAffectedRows()
}

func (se *selectExecutor) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	return se.father.Compile(requestCtx, u, fill)
}

func (se *selectExecutor) GetUUID() []byte {
	return se.father.GetUUID()
}

func (se *selectExecutor) RecordExecPlan(ctx context.Context) error {
	return se.father.RecordExecPlan(ctx)
}

func (se *selectExecutor) GetLoadTag() bool {
	return se.father.GetLoadTag()
}

func (se *selectExecutor) Prepare(ctx context.Context, ses *Session, proc *process.Process, beginInstant time.Time) error {
	return se.father.Prepare(ctx, ses, proc, beginInstant)
}

func (se *selectExecutor) Close(ctx context.Context, ses *Session) error {
	return se.father.Close(ctx, ses)
}

func (se *selectExecutor) VerifyPrivilege(ctx context.Context, ses *Session) error {
	return se.father.VerifyPrivilege(ctx, ses)
}

func (se *selectExecutor) VerifyTxnRestriction(ctx context.Context, ses *Session) error {
	return se.father.VerifyTxnRestriction(ctx, ses)
}

func (se *selectExecutor) ResponseBefore(ctx context.Context, ses *Session) error {
	return se.father.ResponseBefore(ctx, ses)
}

func (se *selectExecutor) ResponseAfter(ctx context.Context, ses *Session) error {
	return se.father.ResponseAfter(ctx, ses)
}

func (se *selectExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return nil
}

func (se *selectExecutor) RecordPlan(ctx context.Context, ses *Session) error {
	return se.father.RecordPlan(ctx, ses)
}

func (se *selectExecutor) CommitOrRollbackTxn(ctx context.Context, ses *Session) error {
	return se.father.CommitOrRollbackTxn(ctx, ses)
}

func (se *selectExecutor) Execute(ctx context.Context, ses *Session, proc *process.Process, beginInstant time.Time) error {
	return se.father.Execute(ctx, ses, proc, beginInstant)
}
