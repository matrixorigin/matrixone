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

package taskservice

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
)

// runSQLTaskMetadataTransaction uses an owned connection so BEGIN, every
// statement, and COMMIT receive the same cancellation budget. A failed COMMIT
// may have reached the server: the caller must reconcile its exact identities.
// Connections whose transactional state is uncertain are never pooled again.
func runSQLTaskMetadataTransaction(
	ctx context.Context,
	db *sql.DB,
	work func(context.Context, *sql.Conn) error,
) (commitAttempted bool, err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return false, err
	}
	discard := true
	defer func() { closeSQLTaskMetadataConn(conn, discard) }()

	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return false, err
	}
	if err := work(ctx, conn); err != nil {
		if ctx.Err() == nil {
			if _, rollbackErr := conn.ExecContext(ctx, "ROLLBACK"); rollbackErr == nil {
				discard = false
			} else {
				err = errors.Join(err, rollbackErr)
			}
		}
		return false, err
	}
	commitAttempted = true
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return true, err
	}
	discard = false
	return true, nil
}

func closeSQLTaskMetadataConn(conn *sql.Conn, discard bool) {
	if discard {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	_ = conn.Close()
}
