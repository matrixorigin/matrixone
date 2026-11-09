// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	mysql "github.com/matrixorigin/mysql"
	"github.com/stretchr/testify/require"
)

func TestMySQLErrorClassifierBothDrivers(t *testing.T) {
	for _, transport := range []struct {
		name string
		make func(uint16) error
	}{
		{"production", func(code uint16) error { return &mysql.MySQLError{Number: code} }},
		{"upstream", func(code uint16) error { return &gomysql.MySQLError{Number: code} }},
	} {
		for _, tc := range []struct {
			code      uint16
			retryable bool
		}{
			{1213, true}, {1205, true}, {2013, true}, {1045, false}, {1064, false},
			// MO transaction codes belong to range replay, not this SQL retry policy.
			{20701, false},
		} {
			for _, wrapped := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%d/wrapped=%t", transport.name, tc.code, wrapped), func(t *testing.T) {
					cause := transport.make(tc.code)
					if wrapped {
						cause = fmt.Errorf("target execution: %w", cause)
					}
					code, ok := MySQLErrorCode(cause)
					require.True(t, ok)
					require.Equal(t, tc.code, code)
					require.Equal(t, tc.retryable, (MySQLErrorClassifier{}).IsRetryable(cause))
				})
			}
		}
	}
	for _, err := range []error{nil, errors.New("Error 1213: deadlock"), driver.ErrBadConn, fmt.Errorf("wrapped: %w", driver.ErrBadConn)} {
		code, ok := MySQLErrorCode(err)
		require.False(t, ok)
		require.Zero(t, code)
		require.Equal(t, errors.Is(err, driver.ErrBadConn), (MySQLErrorClassifier{}).IsRetryable(err))
	}
}
