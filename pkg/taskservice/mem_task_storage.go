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
	}
	return n, nil
}

func (s *memTaskStorage) Update(ctx context.Context, tasks []task.Task, conds ...Condition) (int, error) {
	c := conditions{}
	for _, cond := range conds {
		cond(&c)
	}

	s.Lock()
	defer s.Unlock()

	n := 0
	for _, task := range tasks {
		if s.filter(c, s.tasks[task.ID]) {
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
		if s.filter(c, task) {
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
	c := conditions{}
	for _, cond := range conds {
		cond(&c)
	}

	var sortedTasks []task.Task
	for _, task := range s.tasks {
		sortedTasks = append(sortedTasks, task)
	}
	sort.Slice(sortedTasks, func(i, j int) bool { return sortedTasks[i].ID < sortedTasks[j].ID })

	var result []task.Task
	for _, task := range sortedTasks {
		if s.filter(c, task) {
			result = append(result, task)
		}
		if c.limit >= len(result) {
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
	}
	return n, nil
}

func (s *memTaskStorage) UpdateCronTask(ctx context.Context, cron task.CronTask, value task.Task) (int, error) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.taskIndexes[value.Metadata.ID]; ok {
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
	if c.taskID > 0 {
		switch c.taskIDOp {
		case EQ:
			return task.ID == c.taskID
		case GT:
			return task.ID > c.taskID
		}
	}

	if c.taskRunner != "" {
		switch c.taskRunnerOp {
		case EQ:
			return task.TaskRunner == c.taskRunner
		}
	}
	return true
}
