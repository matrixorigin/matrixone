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
)

// used for testing
type memTaskStorage struct {
	sync.RWMutex

	id              uint64
	tasks           map[uint64]task.Task
	taskIndexes     map[string]uint64
	cronTasks       map[uint64]task.CronTask
	cronTaskIndexes map[string]uint64

	// Used for testing. Make some changes to the data before updating.
	preUpdate     func()
	preUpdateCron func() error
}

func NewMemTaskStorage() TaskStorage {
	return &memTaskStorage{
		tasks:           make(map[uint64]task.Task),
		taskIndexes:     make(map[string]uint64),
		cronTasks:       make(map[uint64]task.CronTask),
		cronTaskIndexes: make(map[string]uint64),
		preUpdateCron:   func() error { return nil },
		preUpdate:       func() {},
	}
}

func (s *memTaskStorage) Close() error {
	return nil
}

func (s *memTaskStorage) Add(ctx context.Context, tasks ...task.Task) (int, error) {
	s.Lock()
	defer s.Unlock()

	n := 0
	for _, v := range tasks {
		if _, ok := s.taskIndexes[v.Metadata.ID]; ok {
			continue
		}

		v.ID = s.nextIDLocked()
		s.tasks[v.ID] = v
		s.taskIndexes[v.Metadata.ID] = v.ID
		n++
	}
	return n, nil
}

func (s *memTaskStorage) Update(ctx context.Context, tasks []task.Task, conds ...Condition) (int, error) {
	if s.preUpdate != nil {
		s.preUpdate()
	}

	c := conditions{}
	for _, cond := range conds {
		cond(&c)
	}

	s.Lock()
	defer s.Unlock()

	n := 0
	for _, task := range tasks {
		if v, ok := s.tasks[task.ID]; ok && s.filter(c, v) {
			n++
			s.tasks[task.ID] = task
		}
	}
	return n, nil
}

func (s *memTaskStorage) Delete(ctx context.Context, conds ...Condition) (int, error) {
	c := conditions{}
	for _, cond := range conds {
		cond(&c)
	}

	s.Lock()
	defer s.Unlock()

	var removeTasks []task.Task
	for _, task := range s.tasks {
		if v, ok := s.tasks[task.ID]; ok && s.filter(c, v) {
			removeTasks = append(removeTasks, task)
		}
	}

	for _, task := range removeTasks {
		delete(s.tasks, task.ID)
		delete(s.taskIndexes, task.Metadata.ID)
	}
	return len(removeTasks), nil
}

func (s *memTaskStorage) Query(ctx context.Context, conds ...Condition) ([]task.Task, error) {
	s.RLock()
	defer s.RUnlock()

	c := conditions{}
	for _, cond := range conds {
		cond(&c)
	}

	sortedTasks := make([]task.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		sortedTasks = append(sortedTasks, task)
	}
	sort.Slice(sortedTasks, func(i, j int) bool { return sortedTasks[i].ID < sortedTasks[j].ID })

	var result []task.Task
	for _, task := range sortedTasks {
		if s.filter(c, task) {
			result = append(result, task)
		}
		if c.limit > 0 && c.limit <= len(result) {
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

func (s *memTaskStorage) QueryCronTask(context.Context) ([]task.CronTask, error) {
	s.Lock()
	defer s.Unlock()

	tasks := make([]task.CronTask, 0, len(s.cronTasks))
	for _, v := range s.cronTasks {
		tasks = append(tasks, v)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	return tasks, nil
}

func (s *memTaskStorage) UpdateCronTask(ctx context.Context, cron task.CronTask, value task.Task) (int, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.preUpdateCron(); err != nil {
		return 0, err
	}

	if _, ok := s.taskIndexes[value.Metadata.ID]; ok {
		return 0, nil
	}
	if v, ok := s.cronTasks[cron.ID]; !ok || v.TriggerTimes != cron.TriggerTimes-1 {
		return 0, nil
	}

	value.ID = s.nextIDLocked()
	s.tasks[value.ID] = value
	s.taskIndexes[value.Metadata.ID] = value.ID
	s.cronTasks[cron.ID] = cron
	return 2, nil
}

func (s *memTaskStorage) nextIDLocked() uint64 {
	s.id++
	return s.id
}

func (s *memTaskStorage) filter(c conditions, task task.Task) bool {
	ok := true

	if c.hasTaskIDCond {
		switch c.taskIDOp {
		case EQ:
			ok = task.ID == c.taskID
		case GT:
			ok = task.ID > c.taskID
		case GE:
			ok = task.ID >= c.taskID
		case LE:
			ok = task.ID <= c.taskID
		case LT:
			ok = task.ID < c.taskID
		}
	}

	if ok && c.hasTaskRunnerCond {
		switch c.taskRunnerOp {
		case EQ:
			ok = task.TaskRunner == c.taskRunner
		}
	}

	if ok && c.hasTaskStatusCond {
		switch c.taskStatusOp {
		case EQ:
			ok = task.Status == c.taskStatus
		case GT:
			ok = task.Status > c.taskStatus
		case GE:
			ok = task.Status >= c.taskStatus
		case LE:
			ok = task.Status <= c.taskStatus
		case LT:
			ok = task.Status < c.taskStatus
		}
	}

	if ok && c.hasTaskEpochCond {
		switch c.taskEpochOp {
		case EQ:
			ok = task.Epoch == c.taskEpoch
		case GT:
			ok = task.Epoch > c.taskEpoch
		case GE:
			ok = task.Epoch >= c.taskEpoch
		case LE:
			ok = task.Epoch <= c.taskEpoch
		case LT:
			ok = task.Epoch < c.taskEpoch
		}
	}

	if ok && c.hasTaskParentIDCond {
		switch c.taskParentTaskIDOp {
		case EQ:
			ok = task.ParentTaskID == c.taskParentTaskID
		}
	}
	return ok
}
