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

package iscp

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type flushErrorSQLExecutor struct {
	err error
}

func (e flushErrorSQLExecutor) Exec(context.Context, string, executor.Options) (executor.Result, error) {
	return executor.Result{}, e.err
}

func (e flushErrorSQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	return e.err
}

func TestInternalSQLConsumerFlushReturnsError(t *testing.T) {
	expected := errors.New("flush failed")
	consumer := &interalSqlConsumer{
		internalSqlExecutor: flushErrorSQLExecutor{err: expected},
		dataRetriever:       &DataRetrieverImpl{accountID: 7},
		tableInfo:           &plan.TableDef{Name: "source"},
	}

	require.ErrorIs(t, consumer.tryFlushSqlBuf(context.Background(), nil, []byte("insert")), expected)
}
