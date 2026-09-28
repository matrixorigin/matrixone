// Copyright 2022 Matrix Origin
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

package motrace

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

var _ ie.InternalExecutor = &dummySqlExecutor{}

type dummySqlExecutor struct {
	opts ie.SessionOverrideOptions
	ch   chan<- string
}

func (e *dummySqlExecutor) ApplySessionOverride(opts ie.SessionOverrideOptions) {}
func (e *dummySqlExecutor) Query(ctx context.Context, s string, options ie.SessionOverrideOptions) ie.InternalExecResult {
	return nil
}
func (e *dummySqlExecutor) Exec(ctx context.Context, sql string, opts ie.SessionOverrideOptions) error {
	e.ch <- sql
	return nil
}

// copy from /Users/jacksonxie/go/src/github.com/matrixorigin/matrixone/pkg/util/metric/metric_collector_test.go
func newDummyExecutorFactory(sqlch chan string) func() ie.InternalExecutor {
	return func() ie.InternalExecutor {
		return &dummySqlExecutor{
			opts: ie.NewOptsBuilder().Finish(),
			ch:   sqlch,
		}
	}
}

func TestInitSchemaByInnerExecutor(t *testing.T) {
	type args struct {
		ctx context.Context
		ch  chan string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "dummy",
			args: args{
				ctx: context.Background(),
				ch:  make(chan string, 10),
			},
		},
		{
			name: "dummyS3",
			args: args{
				ctx: context.Background(),
				ch:  make(chan string, 10),
			},
		},
	}

	// (1 + 4) * n + 1
	// 1: create database ...
	// 4: create EXTERNAL table
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			wg.Add(1 + len(tables) + len(views))
			err := InitSchemaByInnerExecutor(tt.args.ctx, newDummyExecutorFactory(tt.args.ch))
			require.Equal(t, nil, err)
			go func() {
				wg.Wait()
				wg.Add(1)
				close(tt.args.ch)
			}()
		loop:
			for {
				sql, ok := <-tt.args.ch
				wg.Done()
				if ok {
					t.Logf("exec sql: %s", sql)
					if sql == "create database if not exists system" {
						continue
					}
					idx := strings.Index(sql, "CREATE EXTERNAL TABLE")
					viewIdx := strings.Index(sql, "CREATE VIEW")
					require.Equal(t, -1, idx|viewIdx)
				} else {
					t.Log("exec sql Done.")
					wg.Wait()
					break loop
				}
			}
		})
	}
}

func TestGetSchemaForAccount(t *testing.T) {
	type args struct {
		account string
	}
	tests := []struct {
		name     string
		args     args
		wantPath string
	}{
		{
			name: "with_account_user1",
			args: args{
				account: "user1",
			},
			wantPath: "/user1/*/*/*/*/statement_info/*",
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schemas := GetSchemaForAccount(ctx, tt.args.account)
			//found := false
			//for _, sche := range schemas {
			//	t.Logf("schma: %s", sche)
			//	if strings.Contains(sche, tt.wantPath) {
			//		found = true
			//	}
			//}
			require.Equal(t, 1, len(schemas))
			//require.Equal(t, true, found)
			//found = false
			//if strings.Contains(SingleStatementTable.ToCreateSql(ctx, true), "/*/*/*/*/*/statement_info/*") {
			//	found = true
			//}
			//require.Equal(t, true, found)
		})
	}
}

func TestSchemaBootstrapPhases(t *testing.T) {
	ctx := t.Context()
	var statements []string
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		statements = append(statements, sql)
		return executor.Result{}, nil
	}, nil)
	require.NoError(t, InitSchemaTablesWithTxn(ctx, txn))
	require.Len(t, statements, 1+len(tables))
	for _, sql := range statements {
		require.NotContains(t, sql, "CREATE VIEW")
	}
	statements = nil
	require.NoError(t, InitSchemaWithTxn(ctx, txn))
	require.Len(t, statements, 1+len(tables)+2*len(views))

	for _, tc := range []struct {
		name, kind           string
		lookupErr, createErr error
		wantCreates          int
	}{
		{name: "missing", wantCreates: len(views)},
		{name: "existing release views", kind: catalog.SystemViewRel},
		{name: "wrong object kind", kind: catalog.SystemOrdinaryRel},
		{name: "lookup failure", lookupErr: errors.New("lookup failed")},
		{name: "authoring rejected", createErr: errors.New("protocol version 97"), wantCreates: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := mpool.MustNewZero()
			creates := 0
			txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.HasPrefix(sql, "select relkind") {
					require.Contains(t, sql, "account_id = 0")
					if tc.kind == "" {
						return executor.Result{}, tc.lookupErr
					}
					result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, pool)
					result.NewBatchWithRowCount(1)
					executor.AppendStringRows(result, 0, []string{tc.kind})
					return result.GetResult(), nil
				}
				require.Contains(t, sql, "CREATE VIEW IF NOT EXISTS")
				creates++
				return executor.Result{}, tc.createErr
			}, nil)
			err := InitSchemaViewsWithTxn(ctx, txn)
			switch {
			case tc.lookupErr != nil:
				require.ErrorIs(t, err, tc.lookupErr)
			case tc.createErr != nil:
				require.ErrorIs(t, err, tc.createErr)
			case tc.kind == catalog.SystemOrdinaryRel:
				require.ErrorContains(t, err, "non-view object")
			default:
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantCreates, creates)
			require.Zero(t, pool.CurrNB(), "catalog lookup results must be released")
		})
	}
}
