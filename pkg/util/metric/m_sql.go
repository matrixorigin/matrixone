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
		[]string{"type", "internal"},
	)
	statementCounters = []Counter{
		StatementCounterFactory.WithLabelValues("select", "0"),
		StatementCounterFactory.WithLabelValues("insert", "0"),
		StatementCounterFactory.WithLabelValues("delete", "0"),
		StatementCounterFactory.WithLabelValues("update", "0"),
		StatementCounterFactory.WithLabelValues("other", "0"),
	}
	internalStatementCounters = []Counter{
		StatementCounterFactory.WithLabelValues("select", "1"),
		StatementCounterFactory.WithLabelValues("insert", "1"),
		StatementCounterFactory.WithLabelValues("delete", "1"),
		StatementCounterFactory.WithLabelValues("update", "1"),
		StatementCounterFactory.WithLabelValues("other", "1"),
	}
)

type SQLType int

const (
	SQLTypeSelect SQLType = iota
	SQLTypeInsert
	SQLTypeUpdate
	SQLTypeDelete
	SQLTypeOther
)

func StatementCounter(t SQLType, isInternal bool) Counter {
	if isInternal {
		return internalStatementCounters[t]
	} else {
		return statementCounters[t]
	}
}
