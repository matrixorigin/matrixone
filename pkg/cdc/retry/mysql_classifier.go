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

package retry

import (
	"database/sql/driver"
	"errors"

	gomysql "github.com/go-sql-driver/mysql"
)

// MySQLErrorClassifier recognises transient MySQL errors that are worth retrying.
type MySQLErrorClassifier struct{}

var mysqlRetryableErrorCodes = map[uint16]struct{}{
	// Lock wait timeout exceeded; try restarting transaction
	1205: {},
	// Deadlock found when trying to get lock; try restarting transaction
	1213: {},
	// Server closed the connection
	2006: {},
	// Lost connection to MySQL server during query
	2013: {},
	// Can't connect to MySQL server on host (network issues)
	2003: {},
	// Not enough privilege or connection handshake issues that can be transient
	1043: {},
}

// IsRetryable implements ErrorClassifier.
func (MySQLErrorClassifier) IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, driver.ErrBadConn) {
		return true
	}

	var mysqlErr *gomysql.MySQLError
	if errors.As(err, &mysqlErr) {
		if _, ok := mysqlRetryableErrorCodes[mysqlErr.Number]; ok {
			return true
		}
	}

	return false
}
