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

package metric

var (
	StatementCounterFactory = NewCounterVec(
		CounterOpts{
			Subsystem: "sql",
			Name:      "statement_total",
			Help:      "Counter of executed sql statement",
		},
		[]string{constTenantKey, "type"},
		false,
	)

	TransactionCounterFactory = NewCounterVec(
		CounterOpts{
			Subsystem: "sql",
			Name:      "transaction_total",
			Help:      "Counter of transaction",
		},
		[]string{constTenantKey},
		false,
	)

	TransactionErrorsFactory = NewCounterVec(
		CounterOpts{
			Subsystem: "sql",
			Name:      "transaction_errors",
			Help:      "Counter of errors on execute commit/rollback statement",
		},
		[]string{constTenantKey, "type"},
		false,
	)

	StatementErrorsFactory = NewCounterVec(
		CounterOpts{
			Subsystem: "sql",
			Name:      "statement_errors",
			Help:      "Counter of executed sql statement failed.",
		},
		[]string{constTenantKey, "type"},
		false,
	)
)

type SQLType string

var (
	SQLTypeSelect SQLType = "select"
	SQLTypeInsert SQLType = "insert"
	SQLTypeUpdate SQLType = "delete"
	SQLTypeDelete SQLType = "update"
	SQLTypeOther  SQLType = "other"

	SQLTypeBegin        SQLType = "begin"
	SQLTypeCommit       SQLType = "commit"
	SQLTypeRollback     SQLType = "rollback"
	SQLTypeAutoCommit   SQLType = "auto_commit"
	SQLTypeAutoRollback SQLType = "auto_rollback"
)

func StatementCounter(tenant string, t SQLType) Counter {
	return StatementCounterFactory.WithLabelValues(tenant, string(t))
}

func TransactionCounter(tenant string) Counter {
	return TransactionCounterFactory.WithLabelValues(tenant)
}

func TransactionErrorsCounter(account string, t SQLType) Counter {
	return TransactionErrorsFactory.WithLabelValues(account, string(t))
}

func StatementErrorsCounter(account string, t SQLType) Counter {
	return StatementErrorsFactory.WithLabelValues(account, string(t))
}
