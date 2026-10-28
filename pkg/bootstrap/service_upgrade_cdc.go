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

package bootstrap

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// A stopped 4.2 deployment cannot establish the target identity required by
// the current CDC executor. Reject all pre-4.0.7 business tasks, including
// paused tasks, before changing any catalog; never guess an identity from an
// optional marker or a task's state. Limit the diagnostic rather than keeping
// an unbounded result in the upgrade coordinator.
func rejectLegacyCDCTasks(tx executor.TxnExecutor) error {
	const maxListedTasks = 32
	const query = "select cast(account_id as varchar(20)), cast(task_id as varchar(36)), task_name, coalesce(state, '') " +
		"from mo_catalog.mo_cdc_task order by account_id, task_id limit 33"
	res, err := tx.Exec(query, executor.StatementOption{}.WithAccountID(catalog.System_Account).WithMaxResultBytes(CatalogUpgradePageBytes))
	if err != nil {
		return fmt.Errorf("cannot verify legacy CDC tasks before upgrade: %w", err)
	}
	defer res.Close()
	var tasks []string
	more := false
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			if len(tasks) == maxListedTasks {
				more = true
				return false
			}
			fields := make([]string, 4)
			for col := range fields {
				if !cols[col].IsNull(uint64(i)) {
					fields[col] = cols[col].GetStringAt(i)
				}
			}
			tasks = append(tasks, fmt.Sprintf("account=%s task_id=%s name=%s state=%s", fields[0], fields[1], strconv.Quote(fields[2]), strconv.Quote(fields[3])))
		}
		return true
	})
	if len(tasks) == 0 {
		return nil
	}
	suffix := ""
	if more {
		suffix = "; more tasks exist; list all with SELECT account_id, task_id, task_name, state FROM mo_catalog.mo_cdc_task"
	}
	return &terminalUpgradeError{fmt.Errorf("legacy CDC tasks must be exported and removed using the source version before upgrade: %s%s; if new logservice has started, restore the pre-upgrade backup before restarting the source version; stop all old CNs, then recreate CDC with a clean target after upgrade", strings.Join(tasks, "; "), suffix)}
}
