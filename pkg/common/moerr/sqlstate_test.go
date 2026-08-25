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

package moerr

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSqlStateMatchesMySQL: an error that reports a MySQL error number must
// report the SQLSTATE MySQL pairs with that number. Clients route on SQLSTATE
// -- JDBC picks the SQLException subclass from its class, so an integrity
// violation sent as HY000 does not surface as
// SQLIntegrityConstraintViolationException -- and MO already carries MySQL's
// own table in mysql_error_define.go, so the two must not drift.
//
// HY000 stays correct wherever MySQL itself uses it, and for a code MySQL does
// not define.
func TestSqlStateMatchesMySQL(t *testing.T) {
	for code, item := range errorMsgRefer {
		mysqlRef, ok := MysqlErrorMsgRefer[item.mysqlCode]
		if !ok {
			continue // not a MySQL error number: HY000 is the right default
		}
		if len(mysqlRef.SqlStates) == 0 {
			continue
		}
		want := mysqlRef.SqlStates[0]
		// SQLSTATE class 01 is the WARNING class. MySQL attaches it to a
		// warning, never to an ERR packet, and a client that sees class 01
		// reads it as "succeeded, with a warning". MO delivers these as
		// errors, so copying the warning SQLSTATE would misreport them;
		// HY000 is the honest answer here.
		if strings.HasPrefix(want, "01") {
			continue
		}
		require.NotEmpty(t, item.sqlStates, "error %d has no SQLSTATE", code)
		require.Equal(t, want, item.sqlStates[0],
			"error code %d (MySQL %d): SQLSTATE %q, but MySQL uses %q",
			code, item.mysqlCode, item.sqlStates[0], want)
	}
}

// TestDuplicateEntrySqlState pins the case that motivated the sweep: a
// duplicate key is an integrity constraint violation (SQLSTATE class 23), not
// a general error.
func TestDuplicateEntrySqlState(t *testing.T) {
	err := NewDuplicateEntryNoCtx("1", "a")
	require.Equal(t, ER_DUP_ENTRY, err.MySQLCode())
	require.Equal(t, "23000", err.SqlState())
}
