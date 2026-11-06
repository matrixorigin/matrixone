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
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"go.uber.org/zap"
)

type sqlTaskFairFetchContextKey struct{}

func querySQLTaskAsync(ctx context.Context, store TaskStorage, conditions ...Condition) ([]task.AsyncTask, error) {
	tasks, err, _ := querySQLTaskPage(ctx, store, conditions...)
	return tasks, err
}

// A short page proves exhaustion only for built-in stores: a decorator can
// filter rows after the underlying query has applied LIMIT.
func querySQLTaskPage(ctx context.Context, store TaskStorage, conditions ...Condition) ([]task.AsyncTask, error, bool) {
	if holder, ok := store.(*refreshableTaskStorage); ok {
		var tasks []task.AsyncTask
		var trustShortPage bool
		err := holder.withSQLTaskFetchStore(ctx, func(inner TaskStorage) error {
			var err error
			tasks, err, trustShortPage = querySQLTaskPage(ctx, inner, conditions...)
			return err
		})
		return tasks, err, trustShortPage
	}
	tasks, err := store.QueryAsyncTask(ctx, conditions...)
	switch store.(type) {
	case *mysqlTaskStorage, *memTaskStorage:
		return tasks, err, true
	default:
		return tasks, err, false
	}
}

func (r *taskRunner) querySQLTaskAsync(ctx context.Context, owner *taskService, conditions ...Condition) ([]task.AsyncTask, error, bool) {
	ctx = context.WithValue(ctx, sqlTaskFairFetchContextKey{}, true)
	if service, ok := r.service.(*taskService); ok && service == owner {
		return querySQLTaskPage(ctx, owner.store, conditions...)
	}
	// Decorators may filter or otherwise specialize the public query path.
	tasks, err := r.service.QueryAsyncTask(ctx, conditions...)
	return tasks, err, false
}

func scanAsyncTaskIDs(ctx context.Context, store TaskStorage, after, high uint64, descending bool, limit int) ([]uint64, error) {
	return store.ScanAsyncTaskIDs(ctx, after, high, descending, min(max(limit, 1), 32))
}

func (m *mysqlTaskStorage) ScanAsyncTaskIDs(ctx context.Context, after, high uint64, descending bool, limit int) ([]uint64, error) {
	limit = min(max(limit, 1), 32)
	order := "asc"
	if descending {
		order = "desc"
	}
	query := fmt.Sprintf("select task_id from sys_async_task where task_id>? and task_id<=? order by task_id %s limit %d", order, limit)
	rows, err := m.db.QueryContext(ctx, query, after, high)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]uint64, 0, limit)
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *memTaskStorage) ScanAsyncTaskIDs(ctx context.Context, after, high uint64, descending bool, limit int) ([]uint64, error) {
	limit = min(max(limit, 1), 32)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.RLock()
	defer s.RUnlock()
	ids := make([]uint64, 0, limit)
	for id := range s.asyncTasks {
		if id <= after || id > high {
			continue
		}
		at := sort.Search(len(ids), func(i int) bool {
			if descending {
				return ids[i] <= id
			}
			return ids[i] >= id
		})
		if at >= limit {
			continue
		}
		ids = append(ids, 0)
		copy(ids[at+1:], ids[at:])
		ids[at] = id
		if len(ids) > limit {
			ids = ids[:limit]
		}
	}
	return ids, nil
}

func (r *sqlTaskRecovery) hasPending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started
}

// A bounded raw scan can inspect a task before its full metadata is decoded.
func taskMatchesFetchLane(value task.AsyncTask, runner string, sqlLane bool) bool {
	return value.Status == task.TaskStatus_Running && value.TaskRunner == runner &&
		(value.Metadata.Executor == task.TaskCode_SQLTask) == sqlLane
}

type sqlTaskFetchCursor struct {
	after, high uint64
	ready       bool
	initialDone bool
}

type sqlTaskFetchState struct {
	active, waveReady bool
	observed          uint64
	waveHigh          uint64
	nextSQL, fresh    bool
	other, old, new   sqlTaskFetchCursor
	last              *sqlTaskFetchCursor
}

// One recent failing parent is enough to drain an already queued burst without
// delaying unrelated parents. The marker expires and never owns a firing.
type sqlTaskDeferredParent struct {
	id         uint64
	parent     string
	retryAt    time.Time
	hardRetry  time.Time
	oldChecked atomic.Bool
}

func (r *taskRunner) recordDeferredSQLTaskParent(parent string) {
	suffix, ok := strings.CutPrefix(parent, "sql-task:")
	id, err := strconv.ParseUint(suffix, 10, 64)
	if !ok || err != nil || id == 0 || strconv.FormatUint(id, 10) != suffix {
		r.sqlDeferred.Add(1)
		return
	}
	interval := min(max(r.options.fetchInterval, 250*time.Millisecond), time.Duration(math.MaxInt64/8))
	now := time.Now()
	r.sqlBackoff.Store(&sqlTaskDeferredParent{id: id, parent: "sql-task:" + suffix,
		retryAt: now.Add(4 * interval), hardRetry: now.Add(8 * interval)})
	r.sqlDeferred.Add(1)
}

func (r *taskRunner) deferredSQLTaskParent() *sqlTaskDeferredParent {
	for {
		parent := r.sqlBackoff.Load()
		if parent == nil {
			return nil
		}
		now := time.Now()
		if now.Before(parent.retryAt) || !parent.oldChecked.Load() && now.Before(parent.hardRetry) {
			return parent
		}
		if r.sqlBackoff.CompareAndSwap(parent, nil) {
			r.sqlDeferred.Add(1) // revisit rows skipped while this marker was active
			return nil
		}
	}
}

// A full wait queue must not make the cursor skip an unqueued task.
func (s *sqlTaskFetchState) rewind(id uint64) {
	if s.last == nil || id == 0 || id-1 >= s.last.after {
		return
	}
	s.last.after, s.last.ready = id-1, true
	if s.last != &s.new && id <= s.waveHigh {
		s.last.initialDone = false
	}
}

func (r *taskRunner) useSQLTaskFairFetch(service *taskService) bool {
	s := &r.sqlFetch
	s.last = nil
	parent := r.deferredSQLTaskParent()
	current := r.sqlDeferred.Load()
	pending := service.sqlRecovery.hasPending()
	if s.active && s.waveReady && s.other.initialDone && s.old.initialDone {
		if current == s.observed && !pending && parent == nil {
			s.active, s.observed = false, current
		} else {
			*s = sqlTaskFetchState{active: true, observed: current}
		}
	}
	if !s.active && (current != s.observed || pending || parent != nil) {
		*s = sqlTaskFetchState{active: true, observed: current}
	}
	return s.active
}

func (r *taskRunner) doSQLTaskFairFetch(ctx context.Context, service *taskService) ([]task.AsyncTask, error) {
	s := &r.sqlFetch
	available := cap(r.waitTasksC) - len(r.waitTasksC)
	if available <= 0 {
		return nil, nil
	}
	limit := min(max(r.options.queryLimit, 1), available, 32)
	if !s.waveReady {
		high, err := r.lastAsyncTaskID(ctx, service)
		if err != nil {
			return nil, err
		}
		s.waveReady, s.waveHigh = true, high
		s.other = sqlTaskFetchCursor{high: high, ready: true}
		s.old = sqlTaskFetchCursor{high: high, ready: true}
		s.new.after = high
	}

	sqlLane := s.nextSQL
	s.nextSQL = !s.nextSQL // rotate even when this attempt is empty or fails
	cursor := &s.other
	if sqlLane {
		cursor = &s.old
		if s.fresh || cursor.initialDone {
			cursor = &s.new
		}
		s.fresh = !s.fresh
	}
	if !cursor.ready {
		high, err := r.lastAsyncTaskID(ctx, service)
		if err != nil {
			return nil, err
		}
		cursor.high, cursor.ready = high, true
	}
	if cursor.after >= cursor.high {
		r.finishSQLTaskFetchCursor(cursor)
		return nil, nil
	}

	// Keep half the existing fetch budget for a primary-key fallback when a
	// selective query stalls or encounters malformed task metadata.
	fastBudget := max(r.options.fetchTimeout/2, time.Millisecond)
	fastCtx, cancel := context.WithTimeout(ctx, fastBudget)
	conditions := []Condition{WithTaskIDCond(GT, cursor.after), WithTaskStatusCond(task.TaskStatus_Running),
		WithTaskRunnerCond(EQ, r.runnerID), withSQLTaskLane(sqlLane), WithLimitCond(limit)}
	var excluded *sqlTaskDeferredParent
	if sqlLane {
		conditions = append(conditions, withSQLTaskUnclaimed())
		if excluded = r.deferredSQLTaskParent(); excluded != nil {
			conditions = append(conditions, withSQLTaskParentExcluded(excluded.id))
		}
	}
	tasks, err, trustShortPage := r.querySQLTaskAsync(fastCtx, service, conditions...)
	cancel()
	// A service decorator can refine query conditions. Check the returned
	// identity and order before moving the cursor or dispatching any row.
	fallback := err != nil || len(tasks) > limit || !trustShortPage && len(tasks) < limit
	if !fallback {
		lastID := cursor.after
		for _, value := range tasks {
			if value.ID <= lastID || !taskMatchesFetchLane(value, r.runnerID, sqlLane) ||
				(excluded != nil && value.ParentTaskID == excluded.parent) {
				fallback = true
				break
			}
			lastID = value.ID
		}
	}
	if fallback {
		tasks, err = r.fetchSQLTaskRawPage(ctx, service, cursor, sqlLane, limit, excluded)
		if err != nil {
			return nil, err
		}
	} else {
		if excluded != nil && cursor == &s.old {
			excluded.oldChecked.Store(true)
		}
		selected := tasks[:0]
		for _, value := range tasks {
			if value.ID > cursor.high {
				break
			}
			cursor.after = value.ID
			selected = append(selected, value)
		}
		if len(selected) < limit || cursor.after >= cursor.high {
			r.finishSQLTaskFetchCursor(cursor)
		}
		tasks = selected
	}
	if len(tasks) != 0 {
		s.last = cursor
	}
	return tasks, nil
}

func (r *taskRunner) lastAsyncTaskID(ctx context.Context, service *taskService) (uint64, error) {
	ids, err := scanAsyncTaskIDs(ctx, service.store, 0, math.MaxInt64, true, 1)
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	return ids[0], nil
}

func (r *taskRunner) finishSQLTaskFetchCursor(cursor *sqlTaskFetchCursor) {
	cursor.after, cursor.ready = cursor.high, false
	if cursor != &r.sqlFetch.new && cursor.high <= r.sqlFetch.waveHigh {
		cursor.initialDone = true
	}
}

func (r *taskRunner) fetchSQLTaskRawPage(ctx context.Context, service *taskService, cursor *sqlTaskFetchCursor, sqlLane bool, limit int, excluded *sqlTaskDeferredParent) ([]task.AsyncTask, error) {
	ids, err := scanAsyncTaskIDs(ctx, service.store, cursor.after, cursor.high, false, 32)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		r.finishSQLTaskFetchCursor(cursor)
		return nil, nil
	}
	values := make([]task.AsyncTask, 0, limit)
	for _, id := range ids {
		conditions := []Condition{WithTaskIDCond(EQ, id), WithTaskStatusCond(task.TaskStatus_Running),
			WithTaskRunnerCond(EQ, r.runnerID), withSQLTaskLane(sqlLane), WithLimitCond(1)}
		if sqlLane {
			conditions = append(conditions, withSQLTaskUnclaimed())
		}
		tasks, queryErr, _ := r.querySQLTaskAsync(ctx, service, conditions...)
		if ctx.Err() != nil {
			if len(values) != 0 {
				return values, nil
			}
			return nil, ctx.Err()
		}
		cursor.after = id
		if queryErr != nil {
			r.sqlDeferred.Add(1)
			sqlTaskFetchFailed.Warn(zap.Uint64("task-id", id), zap.Error(queryErr))
			continue
		}
		if len(tasks) == 1 && tasks[0].ID == id && taskMatchesFetchLane(tasks[0], r.runnerID, sqlLane) &&
			(excluded == nil || tasks[0].ParentTaskID != excluded.parent) {
			values = append(values, tasks[0])
			if len(values) == limit {
				break
			}
		}
	}
	if cursor.after >= cursor.high || len(ids) < 32 && cursor.after == ids[len(ids)-1] {
		r.finishSQLTaskFetchCursor(cursor)
	}
	return values, nil
}
