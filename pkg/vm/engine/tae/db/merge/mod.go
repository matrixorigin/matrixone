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
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	taskpb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var StopMerge atomic.Bool

type CNMergeScheduler interface {
	SendMergeTask(ctx context.Context, task *api.MergeTaskEntry) error
}

func NewTaskServiceGetter(getter taskservice.Getter) CNMergeScheduler {
	return &taskServiceGetter{
		Getter: getter,
	}
}

type taskServiceGetter struct {
	taskservice.Getter
}

func (tsg *taskServiceGetter) SendMergeTask(ctx context.Context, task *api.MergeTaskEntry) error {
	ts, ok := tsg.Getter()
	if !ok {
		return taskservice.ErrNotReady
	}
	asyncTask, err := ts.QueryAsyncTask(ctx,
		taskservice.WithTaskMetadataId(taskservice.LIKE, "%"+task.TableName+"%"),
		taskservice.WithTaskStatusCond(taskpb.TaskStatus_Created, taskpb.TaskStatus_Running))
	if err != nil {
		return err
	}
	if len(asyncTask) != 0 {
		return moerr.NewInternalError(context.TODO(), fmt.Sprintf("table %s is merging", task.TableName))
	}
	b, err := task.Marshal()
	if err != nil {
		return err
	}
	return ts.CreateAsyncTask(ctx,
		taskpb.TaskMetadata{
			ID:       "Merge:" + task.TableName + ":" + strconv.FormatInt(time.Now().Unix(), 10),
			Executor: taskpb.TaskCode_MergeTablet,
			Context:  b,
			Options:  taskpb.TaskOptions{Resource: &taskpb.Resource{Memory: task.EstimatedMemUsage}},
		})
}

type TaskHostKind int

const (
	TaskHostCN TaskHostKind = iota
	TaskHostDN
)

var ActiveCNObj ActiveCNObjMap = ActiveCNObjMap{
	o: make(map[objectio.ObjectId]struct{}),
}

type ActiveCNObjMap struct {
	sync.Mutex
	o map[objectio.ObjectId]struct{}
}

func (e *ActiveCNObjMap) AddActiveCNObj(entries []*catalog.ObjectEntry) {
	e.Lock()
	for _, entry := range entries {
		e.o[entry.ID] = struct{}{}
	}
	e.Unlock()
}

func (e *ActiveCNObjMap) RemoveActiveCNObj(ids []objectio.ObjectId) {
	e.Lock()
	defer e.Unlock()
	for _, id := range ids {
		delete(e.o, id)
	}
}

func (e *ActiveCNObjMap) CheckOverlapOnCNActive(entries []*catalog.ObjectEntry) bool {
	e.Lock()
	defer e.Unlock()
	for _, entry := range entries {
		if _, ok := e.o[entry.ID]; ok {
			return true
		}
	}
	return false
}

const (
	constMergeMinBlks       = 5
	constMergeExpansionRate = 6
	constMaxMemCap          = 4 * constMergeExpansionRate * common.Const1GBytes // max orginal memory for a object
	constSmallMergeGap      = 3 * time.Minute
)

type Policy interface {
	OnObject(obj *catalog.ObjectEntry)
	Revise(cpu, mem int64) ([]*catalog.ObjectEntry, TaskHostKind)
	ResetForTable(*catalog.TableEntry)
	SetConfig(*catalog.TableEntry, func() txnif.AsyncTxn, any)
	GetConfig(*catalog.TableEntry) any
}
