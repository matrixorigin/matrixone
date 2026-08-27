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

package foreignext

import (
	"fmt"
	"hash/crc32"
	"strings"
)

// WrapPushdownQuery renders the text MO sends to a SQL remote when the table
// opted into predicate pushdown ('recheck' = 'false'): the user's query
// becomes a derived table and the deparsed conjuncts become its WHERE clause.
//
//	select * from (
//	<query>
//	) `__mo_subq_<hash>` where <filter>
//
// The projection stays `*` deliberately.  A foreign scan maps the remote
// result onto the declared columns BY POSITION -- field i feeds column i, the
// remote's column names are never read (docs/cn/esql_sql_exttab.md) -- so
// naming the columns here would impose a name contract on the projection that
// the verbatim path never had, and break every table whose query projects
// differently-named expressions.
//
// The WHERE clause cannot avoid naming columns: it is the one new requirement
// pushdown adds (the query must expose the declared names), and one of the
// reasons pushdown is opt-in rather than automatic.
func WrapPushdownQuery(query, filter string) string {
	if strings.TrimSpace(filter) == "" {
		return query
	}
	inner := trimQueryTail(query)
	// The newlines are load-bearing: a query ending in a `-- ...` line comment
	// would otherwise swallow the closing paren and the whole WHERE clause.
	return fmt.Sprintf("%s\n%s\n) `%s` where %s", pushdownPrefix, inner, PushdownAlias(inner), filter)
}

// pushdownPrefix opens every wrapper WrapPushdownQuery renders.
const pushdownPrefix = "select * from ("

// PushdownAlias derives the derived-table alias for a query text.  It is
// content-derived so one scan always renders the same SQL -- readable remote
// logs, reproducible tests -- while staying distinctive enough that it cannot
// be confused with a user identifier.  Uniqueness against names *inside* the
// query is not required: the alias lives in the wrapper's scope, which the
// inner query cannot see.
func PushdownAlias(query string) string {
	return fmt.Sprintf("__mo_subq_%08x", crc32.ChecksumIEEE([]byte(query)))
}

// trimQueryTail strips trailing whitespace and statement terminators.  A
// user's `select ... ;` is accepted on the verbatim path, but a semicolon
// inside a derived table is a syntax error.
func trimQueryTail(query string) string {
	return strings.TrimRight(strings.TrimSpace(query), "; \t\r\n")
}

// WrapPushdownProbe renders the zero-row form of a query, used to ask a source
// what it calls the columns of that query's result.  It is the same derived
// table the pushed-down query will use, so a text that cannot be wrapped fails
// here -- before MO has committed to a wrapped query -- and MO falls back to
// the verbatim text.
func WrapPushdownProbe(query string) string {
	inner := trimQueryTail(query)
	return fmt.Sprintf("%s\n%s\n) `%s` limit 0", pushdownPrefix, inner, PushdownAlias(inner))
}
