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
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

var (
	statementCostMethod     = "cost"
	statementContainsMethod = "contains"
)

type statementFilters struct {
	filters []StatementFilter
}

func (f *statementFilters) isEmpty() bool {
	return f == nil || len(f.filters) == 0
}

// filter return true means the txn need to be skipped.
// Return true cases:
// 1. any filter skipped
// 2. filters is empty
func (f *statementFilters) filter(
	op client.TxnOperator,
	sql string,
	cost time.Duration,
) bool {
	if f.isEmpty() {
		return true
	}

	for _, v := range f.filters {
		if v.Filter(op, sql, cost) {
			return true
		}
	}
	return false
}

type costFilter struct {
	target time.Duration
}

func (f *costFilter) Filter(
	op client.TxnOperator,
	sql string,
	cost time.Duration,
) bool {
	match := cost > f.target
	return !match
}

type sqlContainsFilter struct {
	value string
}

func (f *sqlContainsFilter) Filter(
	op client.TxnOperator,
	sql string,
	cost time.Duration,
) bool {
	match := strings.Contains(sql, f.value)
	return !match
}
