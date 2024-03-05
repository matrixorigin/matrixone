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
	"sort"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/mohae/deepcopy"
)

// used for testing
type memTaskStorage struct {
	sync.RWMutex

	id                uint64
	asyncTasks        map[uint64]task.AsyncTask
	asyncTaskIndexes  map[string]uint64
	cronTasks         map[uint64]task.CronTask
	cronTaskIndexes   map[string]uint64
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
		daemonTasks:       make(map[uint64]task.DaemonTask),
		daemonTaskIndexes: make(map[string]uint64),
		preUpdateCron:     func() error { return nil },
		preUpdate:         func() {},
	}
}

func (s *memTaskStorage) Close() error {
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

	c := newConditions()
	for _, cond := range conds {
		cond(c)
	}

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
	c := newConditions()
	for _, cond := range conds {
		cond(c)
	}

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

	c := newConditions()
	for _, cond := range conds {
		cond(c)
	}

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

func (s *memTaskStorage) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, conds ...Condition) (int, error) {
	if s.preUpdate != nil {
		s.preUpdate()
	}

	c := newConditions()
	for _, cond := range conds {
		cond(c)
	}

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
	c := newConditions()
	for _, cond := range conds {
		cond(c)
	}

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

	c := newConditions()
	for _, cond := range conds {
		cond(c)
	}

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
