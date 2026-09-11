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

import "sync/atomic"

type sqlTaskRefreshHook struct {
	refreshed func(serviceID string, taskIDs []uint64)
	catchUp   func(serviceID string, taskID uint64, started bool)
}

var sqlTaskRefreshHookForTest atomic.Pointer[sqlTaskRefreshHook]

// SetSQLTaskRefreshHookForTest observes successful scheduler reconciliation,
// after removed cron jobs and their cron-managed callbacks have stopped. The
// catchUp observer separately brackets stopper-managed overdue executions. The
// callback receives an owned snapshot and must not block or call the scheduler.
// Tests installing this process-wide observer must serialize its lifetime.
func SetSQLTaskRefreshHookForTest(hook func(string, []uint64), catchUp func(string, uint64, bool)) func() {
	previous := sqlTaskRefreshHookForTest.Load()
	if hook == nil && catchUp == nil {
		sqlTaskRefreshHookForTest.Store(nil)
	} else {
		value := sqlTaskRefreshHook{refreshed: hook, catchUp: catchUp}
		sqlTaskRefreshHookForTest.Store(&value)
	}
	return func() { sqlTaskRefreshHookForTest.Store(previous) }
}

func notifySQLTaskRefreshForTest(s *taskService) {
	hook := sqlTaskRefreshHookForTest.Load()
	if hook == nil || hook.refreshed == nil {
		return
	}
	ids := make([]uint64, 0, len(s.sqlCrons.jobs))
	for id := range s.sqlCrons.jobs {
		ids = append(ids, id)
	}
	hook.refreshed(s.rt.ServiceUUID(), ids)
}

// Capture the observer before enqueueing, so even a late callback pairs with
// the same registration when the test has already restored its observer.
func observeSQLTaskCatchUpForTest(s *taskService, taskID uint64) func() {
	hook := sqlTaskRefreshHookForTest.Load()
	if hook == nil || hook.catchUp == nil {
		return nil
	}
	serviceID := s.rt.ServiceUUID()
	hook.catchUp(serviceID, taskID, true)
	return func() { hook.catchUp(serviceID, taskID, false) }
}
