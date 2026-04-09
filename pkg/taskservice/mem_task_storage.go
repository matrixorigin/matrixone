// Copyright 2022 Matrix Origin
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
	"sort"
	"sync"

	"github.com/mohae/deepcopy"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

// used for testing
type memTaskStorage struct {
	sync.RWMutex

	id                uint64
	asyncTasks        map[uint64]task.AsyncTask
	asyncTaskIndexes  map[string]uint64
	cronTasks         map[uint64]task.CronTask
	cronTaskIndexes   map[string]uint64
	sqlTasks          map[uint64]SQLTask
	sqlTaskIndexes    map[string]uint64
	sqlTaskRuns       map[uint64]SQLTaskRun
	daemonTasks       map[uint64]task.DaemonTask
	daemonTaskIndexes map[string]uint64

	// Used for testing. Make some changes to the data before updating.
	preUpdate     func()
	preUpdateCron func() error
}

func NewMemTaskStorage() TaskStorage {
	return &memTaskStorage{
		asyncTasks:        make(map[uint64]task.AsyncTask),
		asyncTaskIndexes:  make(map[string]uint64),
		cronTasks:         make(map[uint64]task.CronTask),
		cronTaskIndexes:   make(map[string]uint64),
		sqlTasks:          make(map[uint64]SQLTask),
		sqlTaskIndexes:    make(map[string]uint64),
		sqlTaskRuns:       make(map[uint64]SQLTaskRun),
		daemonTasks:       make(map[uint64]task.DaemonTask),
		daemonTaskIndexes: make(map[string]uint64),
		preUpdateCron:     func() error { return nil },
		preUpdate:         func() {},
	}
}

func (s *memTaskStorage) Close() error {
	return nil
}

func (s *memTaskStorage) PingContext(ctx context.Context) error {
	return nil
}

func (s *memTaskStorage) AddAsyncTask(ctx context.Context, tasks ...task.AsyncTask) (int, error) {
	s.Lock()
	defer s.Unlock()

	n := 0
	for _, v := range tasks {
		if _, ok := s.asyncTaskIndexes[v.Metadata.ID]; ok {
			continue
		}

		v.ID = s.nextIDLocked()
		s.asyncTasks[v.ID] = v
		s.asyncTaskIndexes[v.Metadata.ID] = v.ID
		n++
	}
	return n, nil
}

func (s *memTaskStorage) UpdateAsyncTask(ctx context.Context, tasks []task.AsyncTask, conds ...Condition) (int, error) {
	if s.preUpdate != nil {
		s.preUpdate()
	}

	c := newConditions(conds...)
	s.Lock()
	defer s.Unlock()

	n := 0
	for _, task := range tasks {
		if v, ok := s.asyncTasks[task.ID]; ok && s.filterAsyncTask(c, v) {
			n++
			s.asyncTasks[task.ID] = task
		}
	}
	return n, nil
}

func (s *memTaskStorage) DeleteAsyncTask(ctx context.Context, conds ...Condition) (int, error) {
	c := newConditions(conds...)

	s.Lock()
	defer s.Unlock()

	var removeTasks []task.AsyncTask
	for _, task := range s.asyncTasks {
		if v, ok := s.asyncTasks[task.ID]; ok && s.filterAsyncTask(c, v) {
			removeTasks = append(removeTasks, task)
		}
	}

	for _, task := range removeTasks {
		delete(s.asyncTasks, task.ID)
		delete(s.asyncTaskIndexes, task.Metadata.ID)
	}
	return len(removeTasks), nil
}

func (s *memTaskStorage) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	s.RLock()
	defer s.RUnlock()

	c := newConditions(conds...)

	sortedTasks := make([]task.AsyncTask, 0, len(s.asyncTasks))
	for _, task := range s.asyncTasks {
		sortedTasks = append(sortedTasks, task)
	}
	sort.Slice(sortedTasks, func(i, j int) bool { return sortedTasks[i].ID < sortedTasks[j].ID })

	var result []task.AsyncTask
	for _, task := range sortedTasks {
		if s.filterAsyncTask(c, task) {
			result = append(result, task)
		}
		if cond, e := (*c)[CondLimit]; e && cond.eval(len(result)) {
			break
		}
	}
	return result, nil
}

func (s *memTaskStorage) AddCronTask(ctx context.Context, tasks ...task.CronTask) (int, error) {
	s.Lock()
	defer s.Unlock()

	n := 0
	for _, v := range tasks {
		if _, ok := s.cronTaskIndexes[v.Metadata.ID]; ok {
			continue
		}

		v.ID = s.nextIDLocked()
		s.cronTasks[v.ID] = v
		s.cronTaskIndexes[v.Metadata.ID] = v.ID
		n++
	}
	return n, nil
}

func (s *memTaskStorage) QueryCronTask(context.Context, ...Condition) ([]task.CronTask, error) {
	s.Lock()
	defer s.Unlock()

	tasks := make([]task.CronTask, 0, len(s.cronTasks))
	for _, v := range s.cronTasks {
		tasks = append(tasks, v)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	return tasks, nil
}

func (s *memTaskStorage) UpdateCronTask(ctx context.Context, cron task.CronTask, value task.AsyncTask) (int, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.preUpdateCron(); err != nil {
		return 0, err
	}

	if _, ok := s.asyncTaskIndexes[value.Metadata.ID]; ok {
		return 0, nil
	}
	if v, ok := s.cronTasks[cron.ID]; !ok || v.TriggerTimes != cron.TriggerTimes-1 {
		return 0, nil
	}

	value.ID = s.nextIDLocked()
	s.asyncTasks[value.ID] = value
	s.asyncTaskIndexes[value.Metadata.ID] = value.ID
	s.cronTasks[cron.ID] = cron
	return 2, nil
}

func (s *memTaskStorage) AddDaemonTask(ctx context.Context, tasks ...task.DaemonTask) (int, error) {
	s.Lock()
	defer s.Unlock()

	n := 0
	for _, v := range tasks {
		if _, ok := s.daemonTaskIndexes[v.Metadata.ID]; ok {
			continue
		}

		s.daemonTasks[v.ID] = v
		s.daemonTaskIndexes[v.Metadata.ID] = v.ID
		n++
	}
	return n, nil
}

func (s *memTaskStorage) AddSQLTask(ctx context.Context, tasks ...SQLTask) (int, error) {
	s.Lock()
	defer s.Unlock()

	n := 0
	for _, v := range tasks {
		if _, ok := s.sqlTaskIndexes[sqlTaskIndexKey(v.AccountID, v.TaskName)]; ok {
			continue
		}
		v.TaskID = s.nextIDLocked()
		s.sqlTasks[v.TaskID] = v
		s.sqlTaskIndexes[sqlTaskIndexKey(v.AccountID, v.TaskName)] = v.TaskID
		n++
	}
	return n, nil
}

func (s *memTaskStorage) UpdateSQLTask(ctx context.Context, tasks []SQLTask, conds ...Condition) (int, error) {
	c := newConditions(conds...)

	s.Lock()
	defer s.Unlock()

	n := 0
	for _, t := range tasks {
		if v, ok := s.sqlTasks[t.TaskID]; ok && s.filterSQLTask(c, v) {
			if oldKey, newKey := sqlTaskIndexKey(v.AccountID, v.TaskName), sqlTaskIndexKey(t.AccountID, t.TaskName); oldKey != newKey {
				delete(s.sqlTaskIndexes, oldKey)
				s.sqlTaskIndexes[newKey] = t.TaskID
			}
			s.sqlTasks[t.TaskID] = t
			n++
		}
	}
	return n, nil
}

func (s *memTaskStorage) DeleteSQLTask(ctx context.Context, conds ...Condition) (int, error) {
	c := newConditions(conds...)

	s.Lock()
	defer s.Unlock()

	var removeTasks []SQLTask
	for _, t := range s.sqlTasks {
		if v, ok := s.sqlTasks[t.TaskID]; ok && s.filterSQLTask(c, v) {
			removeTasks = append(removeTasks, t)
		}
	}

	for _, t := range removeTasks {
		delete(s.sqlTasks, t.TaskID)
		delete(s.sqlTaskIndexes, sqlTaskIndexKey(t.AccountID, t.TaskName))
	}
	return len(removeTasks), nil
}

func (s *memTaskStorage) QuerySQLTask(ctx context.Context, conds ...Condition) ([]SQLTask, error) {
	s.RLock()
	defer s.RUnlock()

	c := newConditions(conds...)

	sortedTasks := make([]SQLTask, 0, len(s.sqlTasks))
	for _, t := range s.sqlTasks {
		sortedTasks = append(sortedTasks, t)
	}
	sort.Slice(sortedTasks, func(i, j int) bool { return sortedTasks[i].TaskID < sortedTasks[j].TaskID })

	var result []SQLTask
	for _, t := range sortedTasks {
		if s.filterSQLTask(c, t) {
			result = append(result, t)
		}
		if cond, ok := (*c)[CondLimit]; ok && cond.eval(len(result)) {
			break
		}
	}
	return result, nil
}

func (s *memTaskStorage) AddSQLTaskRun(ctx context.Context, runs ...SQLTaskRun) (int, error) {
	s.Lock()
	defer s.Unlock()

	n := 0
	for _, run := range runs {
		run.RunID = s.nextIDLocked()
		s.sqlTaskRuns[run.RunID] = run
		n++
	}
	return n, nil
}

func (s *memTaskStorage) UpdateSQLTaskRun(ctx context.Context, runs []SQLTaskRun, conds ...Condition) (int, error) {
	c := newConditions(conds...)

	s.Lock()
	defer s.Unlock()

	n := 0
	for _, run := range runs {
		if v, ok := s.sqlTaskRuns[run.RunID]; ok && s.filterSQLTaskRun(c, v) {
			s.sqlTaskRuns[run.RunID] = run
			n++
		}
	}
	return n, nil
}

func (s *memTaskStorage) QuerySQLTaskRun(ctx context.Context, conds ...Condition) ([]SQLTaskRun, error) {
	s.RLock()
	defer s.RUnlock()

	c := newConditions(conds...)

	sortedRuns := make([]SQLTaskRun, 0, len(s.sqlTaskRuns))
	for _, run := range s.sqlTaskRuns {
		sortedRuns = append(sortedRuns, run)
	}
	sort.Slice(sortedRuns, func(i, j int) bool { return sortedRuns[i].RunID < sortedRuns[j].RunID })

	var result []SQLTaskRun
	for _, run := range sortedRuns {
		if s.filterSQLTaskRun(c, run) {
			result = append(result, run)
		}
		if cond, ok := (*c)[CondLimit]; ok && cond.eval(len(result)) {
			break
		}
	}
	return result, nil
}

func (s *memTaskStorage) AcquireSQLTaskRun(ctx context.Context, sqlTask SQLTask, run SQLTaskRun) (uint64, error) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.sqlTasks[sqlTask.TaskID]; !ok {
		return 0, ErrSQLTaskNotFound
	}
	for _, existing := range s.sqlTaskRuns {
		if existing.TaskID == sqlTask.TaskID && existing.Status == SQLTaskStatusRunning {
			return 0, ErrSQLTaskOverlap
		}
	}

	run.RunID = s.nextIDLocked()
	s.sqlTaskRuns[run.RunID] = run
	return run.RunID, nil
}

func (s *memTaskStorage) CompleteSQLTaskRun(ctx context.Context, run SQLTaskRun) (int, error) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.sqlTaskRuns[run.RunID]; !ok {
		return 0, nil
	}
	s.sqlTaskRuns[run.RunID] = run
	return 1, nil
}

func (s *memTaskStorage) TriggerSQLTask(ctx context.Context, sqlTask SQLTask, asyncTask task.AsyncTask) (int, error) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.asyncTaskIndexes[asyncTask.Metadata.ID]; ok {
		return 0, nil
	}
	current, ok := s.sqlTasks[sqlTask.TaskID]
	if !ok || current.TriggerCount != sqlTask.TriggerCount-1 {
		return 0, nil
	}

	asyncTask.ID = s.nextIDLocked()
	s.asyncTasks[asyncTask.ID] = asyncTask
	s.asyncTaskIndexes[asyncTask.Metadata.ID] = asyncTask.ID
	s.sqlTasks[sqlTask.TaskID] = sqlTask
	return 2, nil
}

func (s *memTaskStorage) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, conds ...Condition) (int, error) {
	if s.preUpdate != nil {
		s.preUpdate()
	}

	c := newConditions(conds...)

	s.Lock()
	defer s.Unlock()

	n := 0
	for _, t := range tasks {
		if v, ok := s.daemonTasks[t.ID]; ok && s.filterDaemonTask(c, v) {
			n++
			s.daemonTasks[t.ID] = t
		}
	}
	return n, nil
}

func (s *memTaskStorage) DeleteDaemonTask(ctx context.Context, conds ...Condition) (int, error) {
	c := newConditions(conds...)

	s.Lock()
	defer s.Unlock()

	var removeTasks []task.DaemonTask
	for _, task := range s.daemonTasks {
		if v, ok := s.daemonTasks[task.ID]; ok && s.filterDaemonTask(c, v) {
			removeTasks = append(removeTasks, task)
		}
	}

	for _, task := range removeTasks {
		delete(s.daemonTasks, task.ID)
		delete(s.daemonTaskIndexes, task.Metadata.ID)
	}
	return len(removeTasks), nil
}

func (s *memTaskStorage) QueryDaemonTask(ctx context.Context, conds ...Condition) ([]task.DaemonTask, error) {
	s.RLock()
	defer s.RUnlock()

	c := newConditions(conds...)

	sortedTasks := make([]task.DaemonTask, 0, len(s.daemonTasks))
	for _, t := range s.daemonTasks {
		sortedTasks = append(sortedTasks, deepcopy.Copy(t).(task.DaemonTask))
	}
	sort.Slice(sortedTasks, func(i, j int) bool { return sortedTasks[i].ID < sortedTasks[j].ID })

	var result []task.DaemonTask
	for _, task := range sortedTasks {
		if s.filterDaemonTask(c, task) {
			result = append(result, task)
		}
		if cond, e := (*c)[CondLimit]; e && cond.eval(len(result)) {
			break
		}
	}
	return result, nil
}

func (s *memTaskStorage) HeartbeatDaemonTask(ctx context.Context, tasks []task.DaemonTask) (int, error) {
	if s.preUpdate != nil {
		s.preUpdate()
	}

	s.Lock()
	defer s.Unlock()

	n := 0
	for _, t := range tasks {
		if _, ok := s.daemonTasks[t.ID]; ok {
			n++
			s.daemonTasks[t.ID] = t
		}
	}
	return n, nil
}

func (s *memTaskStorage) nextIDLocked() uint64 {
	s.id++
	return s.id
}

func (s *memTaskStorage) filterAsyncTask(c *conditions, task task.AsyncTask) bool {
	ok := true

	if cond, e := (*c)[CondTaskID]; e {
		ok = cond.eval(task.ID)
	}
	if !ok {
		return false
	}

	if cond, e := (*c)[CondTaskRunner]; e {
		ok = cond.eval(task.TaskRunner)
	}
	if !ok {
		return false
	}

	if cond, e := (*c)[CondTaskStatus]; e {
		ok = cond.eval(task.Status)
	}
	if !ok {
		return false
	}

	if cond, e := (*c)[CondTaskEpoch]; e {
		ok = cond.eval(task.Epoch)
	}
	if !ok {
		return false
	}

	if cond, e := (*c)[CondTaskParentTaskID]; e {
		ok = cond.eval(task.ParentTaskID)
	}
	if !ok {
		return false
	}

	if cond, e := (*c)[CondTaskMetadataId]; e {
		if metadataCond, ok2 := cond.(*taskMetadataIDCond); ok2 {
			ok = compare(metadataCond.op, task.Metadata.ID, metadataCond.taskMetadataID)
		}
	}
	return ok
}

func (s *memTaskStorage) filterDaemonTask(c *conditions, task task.DaemonTask) bool {
	ok := true

	if cond, e := (*c)[CondTaskID]; e {
		ok = cond.eval(task.ID)
	}
	if !ok {
		return false
	}

	if cond, e := (*c)[CondTaskRunner]; e {
		ok = cond.eval(task.TaskRunner)
	}
	if !ok {
		return false
	}

	if cond, e := (*c)[CondTaskStatus]; e {
		ok = cond.eval(task.TaskStatus)
	}
	if !ok {
		return false
	}

	if cond, e := (*c)[CondTaskType]; e {
		ok = cond.eval(task.TaskType)
	}

	if cond, e := (*c)[CondAccountID]; e {
		ok = cond.eval(task.AccountID)
	}

	if cond, e := (*c)[CondAccount]; e {
		ok = cond.eval(task.Account)
	}

	if cond, e := (*c)[CondLastHeartbeat]; e {
		ok = cond.eval(task.LastHeartbeat.UnixNano())
	}
	return ok
}

func (s *memTaskStorage) filterSQLTask(c *conditions, task SQLTask) bool {
	for code, cond := range *c {
		switch code {
		case CondTaskID:
			if !cond.eval(task.TaskID) {
				return false
			}
		case CondCdcTaskName:
			if nameCond, ok := cond.(*taskNameCond); ok && !compare(nameCond.op, task.TaskName, nameCond.taskName) {
				return false
			}
		case CondAccountID:
			if !cond.eval(task.AccountID) {
				return false
			}
		case CondSQLTaskEnabled:
			if !cond.eval(task.Enabled) {
				return false
			}
		}
	}
	return true
}

func (s *memTaskStorage) filterSQLTaskRun(c *conditions, run SQLTaskRun) bool {
	for code, cond := range *c {
		switch code {
		case CondSQLTaskRunID:
			if !cond.eval(run.RunID) {
				return false
			}
		case CondTaskID:
			if !cond.eval(run.TaskID) {
				return false
			}
		case CondCdcTaskName:
			if nameCond, ok := cond.(*taskNameCond); ok && !compare(nameCond.op, run.TaskName, nameCond.taskName) {
				return false
			}
		case CondAccountID:
			if !cond.eval(run.AccountID) {
				return false
			}
		case CondSQLTaskRunStatus:
			if !cond.eval(run.Status) {
				return false
			}
		case CondSQLTaskTriggerType:
			if !cond.eval(run.TriggerType) {
				return false
			}
		}
	}
	return true
}

func sqlTaskIndexKey(accountID uint32, taskName string) string {
	return fmt.Sprintf("%d/%s", accountID, taskName)
}

func (s *memTaskStorage) UpdateCDCTask(ctx context.Context, targetStatus task.TaskStatus, f func(context.Context, task.TaskStatus, map[CDCTaskKey]struct{}, SqlExecutor) (int, error), condition ...Condition) (int, error) {
	return 0, nil
}

func (s *memTaskStorage) AddCDCTask(ctx context.Context, dt task.DaemonTask, callback func(context.Context, SqlExecutor) (int, error)) (int, error) {
	return 0, nil
}
