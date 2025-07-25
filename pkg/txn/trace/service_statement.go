// Copyright 2024 Matrix Origin
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

package trace

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

func (s *service) AddStatement(
	op client.TxnOperator,
	sql string,
	cost time.Duration,
) {
	if !s.Enabled(FeatureTraceStatement) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	txnFilters := s.atomic.txnFilters.Load()
	if skipped := txnFilters.filter(op); skipped {
		return
	}

	statementFilters := s.atomic.statementFilters.Load()
	if skipped := statementFilters.filter(op, sql, cost); skipped {
		return
	}

	sql = truncateSQL(sql)
	s.statementC <- event{
		csv: newStatement(
			op.Txn().ID,
			sql,
			cost,
		),
	}
}

func (s *service) AddStatementFilter(
	method, value string,
) error {
	switch method {
	case statementCostMethod:
		cost := &toml.Duration{}
		if err := cost.UnmarshalText([]byte(value)); err != nil {
			return err
		}
	case statementContainsMethod:
	default:
		return moerr.NewNotSupportedNoCtxf("method %s not support", method)
	}

	ctx, cancel := context.WithTimeoutCause(context.Background(), 30*time.Second, moerr.CauseAddStatementFilter)
	defer cancel()

	now, _ := s.clock.Now()
	err := s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			r, err := txn.Exec(addStatementFilterSQL(method, value), executor.StatementOption{})
			if err != nil {
				return err
			}
			r.Close()
			return nil
		},
		executor.Options{}.
			WithDatabase(DebugDB).
			WithMinCommittedTS(now).
			WithWaitCommittedLogApplied().
			WithDisableTrace())
	return moerr.AttachCause(ctx, err)
}

func (s *service) ClearStatementFilters() error {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 30*time.Second, moerr.CauseClearStatementFilters)
	defer cancel()

	now, _ := s.clock.Now()
	err := s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(DebugDB)
			res, err := txn.Exec(
				fmt.Sprintf("truncate table %s",
					TraceStatementFilterTable),
				executor.StatementOption{})
			if err != nil {
				return err
			}
			res.Close()
			return nil
		},
		executor.Options{}.
			WithDisableTrace().
			WithMinCommittedTS(now).
			WithWaitCommittedLogApplied())
	if err != nil {
		return moerr.AttachCause(ctx, err)
	}

	return s.RefreshTableFilters()
}

func (s *service) RefreshStatementFilters() error {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 30*time.Second, moerr.CauseRefreshStatementFilters)
	defer cancel()

	var filters []StatementFilter
	var methods []string
	var values []string
	now, _ := s.clock.Now()
	err := s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(DebugDB)
			res, err := txn.Exec(
				fmt.Sprintf("select method, value from %s",
					TraceStatementFilterTable),
				executor.StatementOption{}.WithDisableLog())
			if err != nil {
				return err
			}
			defer res.Close()

			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				for i := 0; i < rows; i++ {
					methods = append(methods, cols[0].UnsafeGetStringAt(i))
					values = append(values, cols[1].UnsafeGetStringAt(i))
				}
				return true
			})
			return nil
		},
		executor.Options{}.
			WithDisableTrace().
			WithMinCommittedTS(now).
			WithWaitCommittedLogApplied())
	if err != nil {
		return moerr.AttachCause(ctx, err)
	}

	for i, method := range methods {
		switch method {
		case statementCostMethod:
			cost := &toml.Duration{}
			if err := cost.UnmarshalText([]byte(values[i])); err != nil {
				panic(err)
			}
			filters = append(filters,
				&costFilter{target: cost.Duration})
		case statementContainsMethod:
			filters = append(filters,
				&sqlContainsFilter{
					value: values[i],
				})
		}
	}

	s.atomic.statementFilters.Store(&statementFilters{filters: filters})
	return nil
}

func (s *service) handleStatements(ctx context.Context) {
	s.handleEvent(
		ctx,
		4,
		TraceStatementTable,
		s.statementC,
	)
}

func addStatementFilterSQL(
	method string,
	value string,
) string {
	return fmt.Sprintf("insert into %s (method, value) values ('%s', '%s')",
		TraceStatementFilterTable,
		method,
		value)
}
