package v1_3_0

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
)

type MockTxnExecutor struct{}

func (MockTxnExecutor) Use(db string) {
	//TODO implement me
	panic("implement me")
}

func (MockTxnExecutor) LockTable(table string) error {
	//TODO implement me
	panic("implement me")
}

func (MockTxnExecutor) Exec(sql string, options executor.StatementOption) (executor.Result, error) {
	return executor.Result{}, nil
}

func (MockTxnExecutor) Txn() client.TxnOperator {
	//TODO implement me
	panic("implement me")
}

func TestInsertInitDataKey(t *testing.T) {
	txn := &MockTxnExecutor{}
	err := InsertInitDataKey(txn, "01234567890123456789012345678901")
	assert.NoError(t, err)
}
