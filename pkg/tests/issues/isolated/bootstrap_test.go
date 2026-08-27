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

package isolated

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// StartTestCluster returns after the CN service is listening, while the
// asynchronous system bootstrap can still be creating the task tables. Account
// DDL initializes a complete tenant catalog and competes with that bootstrap for
// HAKeeper/logtail work. Wait for the bootstrap marker tables before issuing it.
func waitSystemBootstrap(ctx context.Context, db *sql.DB) error {
	want := map[string]struct{}{
		"sys_async_task":  {},
		"sys_cron_task":   {},
		"sys_daemon_task": {},
		"sql_task":        {},
		"sql_task_run":    {},
	}

	for {
		rows, err := db.QueryContext(ctx, "show tables from mo_task")
		if err == nil {
			err = func() error {
				defer rows.Close()
				for rows.Next() {
					var name string
					if err := rows.Scan(&name); err != nil {
						return err
					}
					delete(want, strings.ToLower(name))
				}
				return rows.Err()
			}()
			if err == nil && len(want) == 0 {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
