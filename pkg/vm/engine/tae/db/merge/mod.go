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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	taskpb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var StopMerge atomic.Bool
var DisableDeltaLocMerge atomic.Bool

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

type TaskHostKind int

const (
	TaskHostCN TaskHostKind = iota
	TaskHostDN
)

type activeEntry struct {
	tid      uint64
	insertAt time.Time
}

var ActiveCNObj = ActiveCNObjMap{
	o: make(map[objectio.ObjectId]activeEntry),
}

type ActiveCNObjMap struct {
	sync.Mutex
	o map[objectio.ObjectId]activeEntry
}

func (e *ActiveCNObjMap) Prune(id uint64, ago time.Duration) {
	e.Lock()
	defer e.Unlock()
	now := time.Now()
	if ago == 0 {
		for k, v := range e.o {
			if v.tid == id {
				delete(e.o, k)
			}
		}
		return
	}

	if id == 0 && ago > 1*time.Second {
		for k, v := range e.o {
			if now.Sub(v.insertAt) > ago {
				delete(e.o, k)
			}
		}
		return
	}
	for k, v := range e.o {
		if v.tid == id && now.Sub(v.insertAt) > ago {
			delete(e.o, k)
		}
	}
}

func (e *ActiveCNObjMap) String() string {
	e.Lock()
	defer e.Unlock()

	b := &bytes.Buffer{}
	now := time.Now()
	for k, v := range e.o {
		b.WriteString(fmt.Sprintf(" id: %v, table: %v, insertAt: %s ago\n",
			k.String(), v.tid, now.Sub(v.insertAt).String()))
	}
	return b.String()
}

func (e *ActiveCNObjMap) AddActiveCNObj(entries []*catalog.ObjectEntry) {
	e.Lock()
	for _, entry := range entries {
		e.o[*entry.ID()] = activeEntry{
			entry.GetTable().ID,
			time.Now(),
		}
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
		if _, ok := e.o[*entry.ID()]; ok {
			return true
		}
	}
	return false
}

func CleanUpUselessFiles(entry *api.MergeCommitEntry, fs fileservice.FileService) {
	if entry == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, filepath := range entry.BookingLoc {
		_ = fs.Delete(ctx, filepath)
	}
	if len(entry.CreatedObjs) != 0 {
		for _, obj := range entry.CreatedObjs {
			if len(obj) == 0 {
				continue
			}
			s := objectio.ObjectStats(obj)
			_ = fs.Delete(ctx, s.ObjectName().String())
		}
	}
}

const (
	constMaxMemCap     = 12 * common.Const1GBytes // max original memory for an object
	constSmallMergeGap = 3 * time.Minute
)

type Policy interface {
	OnObject(obj *catalog.ObjectEntry, force bool)
	Revise(cpu, mem int64) ([]*catalog.ObjectEntry, TaskHostKind)
	ResetForTable(*catalog.TableEntry)
	SetConfig(*catalog.TableEntry, func() txnif.AsyncTxn, any)
	GetConfig(*catalog.TableEntry) any
}

func NewUpdatePolicyReq(c *BasicPolicyConfig) *api.AlterTableReq {
	return &api.AlterTableReq{
		Kind: api.AlterKind_UpdatePolicy,
		Operation: &api.AlterTableReq_UpdatePolicy{
			UpdatePolicy: &api.AlterTablePolicy{
				MinOsizeQuailifed: c.ObjectMinOsize,
				MaxObjOnerun:      uint32(c.MergeMaxOneRun),
				MaxOsizeMergedObj: c.MaxOsizeMergedObj,
				MinCnMergeSize:    c.MinCNMergeSize,
				Hints:             c.MergeHints,
			},
		},
	}
}
