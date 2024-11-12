// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	taskpb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func NewTaskServiceGetter(getter taskservice.Getter) *CNMergeScheduler {
	return &CNMergeScheduler{
		getter: getter,
		activeObjects: struct {
			sync.Mutex
			o map[objectio.ObjectId]activeEntry
		}{o: make(map[objectio.ObjectId]activeEntry)},
	}
}

type CNMergeScheduler struct {
	getter taskservice.Getter

	activeObjects struct {
		sync.Mutex
		o map[objectio.ObjectId]activeEntry
	}
}

func (s *CNMergeScheduler) sendMergeTask(ctx context.Context, task *api.MergeTaskEntry) error {
	ts, ok := s.getter()
	if !ok {
		return taskservice.ErrNotReady
	}
	taskIDPrefix := "Merge:" + task.TableName
	asyncTask, err := ts.QueryAsyncTask(ctx,
		taskservice.WithTaskMetadataId(taskservice.LIKE, taskIDPrefix+"%"),
		taskservice.WithTaskStatusCond(taskpb.TaskStatus_Created, taskpb.TaskStatus_Running))
	if err != nil {
		return err
	}
	if len(asyncTask) != 0 {
		return moerr.NewInternalError(ctx, fmt.Sprintf("table %q is merging", task.TableName))
	}
	b, err := task.Marshal()
	if err != nil {
		return err
	}
	return ts.CreateAsyncTask(ctx,
		taskpb.TaskMetadata{
			ID:       taskIDPrefix + ":" + strconv.FormatInt(time.Now().Unix(), 10),
			Executor: taskpb.TaskCode_MergeObject,
			Context:  b,
			Options:  taskpb.TaskOptions{Resource: &taskpb.Resource{Memory: task.EstimatedMemUsage}},
		})
}

func (s *CNMergeScheduler) addActiveObjects(entries []*catalog.ObjectEntry) {
	s.activeObjects.Lock()
	for _, entry := range entries {
		s.activeObjects.o[*entry.ID()] = activeEntry{
			entry.GetTable().ID,
			time.Now(),
		}
	}
	s.activeObjects.Unlock()
}

func (s *CNMergeScheduler) checkOverlapOnCNActive(entries []*catalog.ObjectEntry) bool {
	s.activeObjects.Lock()
	defer s.activeObjects.Unlock()
	for _, entry := range entries {
		if _, ok := s.activeObjects.o[*entry.ID()]; ok {
			return true
		}
	}
	return false
}

func (s *CNMergeScheduler) activeObjsString() string {
	s.activeObjects.Lock()
	defer s.activeObjects.Unlock()

	b := &bytes.Buffer{}
	now := time.Now()
	for k, v := range s.activeObjects.o {
		b.WriteString(fmt.Sprintf(" id: %v, table: %v, insertAt: %s ago\n",
			k.String(), v.tid, now.Sub(v.insertAt).String()))
	}
	return b.String()
}

func (s *CNMergeScheduler) removeActiveObject(ids []objectio.ObjectId) {
	s.activeObjects.Lock()
	defer s.activeObjects.Unlock()
	for _, id := range ids {
		delete(s.activeObjects.o, id)
	}
}

func (s *CNMergeScheduler) prune(id uint64, ago time.Duration) {
	s.activeObjects.Lock()
	defer s.activeObjects.Unlock()
	now := time.Now()
	if ago == 0 {
		for k, v := range s.activeObjects.o {
			if v.tid == id {
				delete(s.activeObjects.o, k)
			}
		}
		return
	}

	if id == 0 && ago > 1*time.Second {
		for k, v := range s.activeObjects.o {
			if now.Sub(v.insertAt) > ago {
				delete(s.activeObjects.o, k)
			}
		}
		return
	}
	for k, v := range s.activeObjects.o {
		if v.tid == id && now.Sub(v.insertAt) > ago {
			delete(s.activeObjects.o, k)
		}
	}
}

type activeEntry struct {
	tid      uint64
	insertAt time.Time
}
