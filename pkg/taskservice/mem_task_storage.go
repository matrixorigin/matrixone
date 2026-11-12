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
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/mohae/deepcopy"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

// used for testing
type memTaskStorage struct {
	sync.RWMutex

	id                uint64
	asyncTasks        map[uint64]task.AsyncTask
	asyncTaskIndexes  map[string]uint64
	sqlTaskClaims     map[uint64]uint64
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
		sqlTaskClaims:     make(map[uint64]uint64),
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
		delete(s.sqlTaskClaims, task.ID)
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
	slices.SortFunc(sortedTasks, func(a, b task.AsyncTask) int {
		if _, desc := (*c)[CondOrderByDesc]; desc {
			return cmp.Compare(b.ID, a.ID)
		}
		return cmp.Compare(a.ID, b.ID)
	})

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
	slices.SortFunc(tasks, func(a, b task.CronTask) int { return cmp.Compare(a.ID, b.ID) })
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
		if v.ScheduleGeneration == 0 {
			v.ScheduleGeneration = 1
		}
		v.TaskID = s.nextIDLocked()
		s.sqlTasks[v.TaskID] = v
		s.sqlTaskIndexes[sqlTaskIndexKey(v.AccountID, v.TaskName)] = v.TaskID
		n++
	}
	return n, nil
}

func (s *memTaskStorage) AlterSQLTask(ctx context.Context, observed, updated SQLTask, action SQLTaskAlterAction) (int, error) {
	if observed.TaskID != updated.TaskID || observed.AccountID != updated.AccountID {
		return 0, moerr.NewInvalidInputNoCtx("sql task identity changed during alter")
	}
	if action > SQLTaskSetTimeout {
		return 0, moerr.NewInvalidInputNoCtx("unknown sql task alter action")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.Lock()
	defer s.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	current, ok := s.sqlTasks[observed.TaskID]
	if !ok || current.AccountID != observed.AccountID {
		return 0, nil
	}
	switch action {
	case SQLTaskSuspend, SQLTaskSetSchedule:
		if current.ScheduleGeneration != observed.ScheduleGeneration {
			return 0, nil
		}
		sequence, err := nextSQLTaskInvalidationSequence(current)
		if err != nil {
			return 0, err
		}
		current.TriggerCount, current.ScheduleGeneration = sequence, sequence
		if action == SQLTaskSuspend {
			current.Enabled = false
		} else {
			current.CronExpr, current.Timezone, current.NextFireTime = updated.CronExpr, updated.Timezone, updated.NextFireTime
		}
	case SQLTaskResume:
		if current.ScheduleGeneration != observed.ScheduleGeneration {
			return 0, nil
		}
		current.Enabled, current.NextFireTime = true, updated.NextFireTime
	case SQLTaskSetGate:
		current.GateCondition = updated.GateCondition
	case SQLTaskSetRetry:
		current.RetryLimit = updated.RetryLimit
	case SQLTaskSetTimeout:
		current.TimeoutSeconds = updated.TimeoutSeconds
	default:
		return 0, moerr.NewInvalidInputNoCtx("unknown sql task alter action")
	}
	current.UpdatedAt = updated.UpdatedAt
	s.sqlTasks[current.TaskID] = current
	return 1, nil
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
		parentTaskID := fmt.Sprintf("sql-task:%d", t.TaskID)
		for asyncTaskID, asyncTask := range s.asyncTasks {
			if asyncTask.ParentTaskID == parentTaskID {
				delete(s.asyncTasks, asyncTaskID)
				delete(s.asyncTaskIndexes, asyncTask.Metadata.ID)
				delete(s.sqlTaskClaims, asyncTaskID)
			}
		}
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
	slices.SortFunc(sortedTasks, func(a, b SQLTask) int { return cmp.Compare(a.TaskID, b.TaskID) })

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

func (s *memTaskStorage) QuerySQLTaskRun(ctx context.Context, conds ...Condition) ([]SQLTaskRun, error) {
	s.RLock()
	defer s.RUnlock()

	c := newConditions(conds...)

	sortedRuns := make([]SQLTaskRun, 0, len(s.sqlTaskRuns))
	for _, run := range s.sqlTaskRuns {
		sortedRuns = append(sortedRuns, run)
	}
	slices.SortFunc(sortedRuns, func(a, b SQLTaskRun) int { return cmp.Compare(b.RunID, a.RunID) })

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

func (s *memTaskStorage) QueryLatestSQLTaskRun(ctx context.Context, accountID uint32, taskIDs []uint64) ([]SQLTaskRun, error) {
	s.RLock()
	defer s.RUnlock()

	if len(taskIDs) == 0 {
		return nil, nil
	}

	taskIDSet := make(map[uint64]struct{}, len(taskIDs))
	for _, taskID := range taskIDs {
		taskIDSet[taskID] = struct{}{}
	}

	latestByTask := make(map[uint64]SQLTaskRun, len(taskIDs))
	for _, run := range s.sqlTaskRuns {
		if run.AccountID != accountID {
			continue
		}
		if _, ok := taskIDSet[run.TaskID]; !ok {
			continue
		}
		prev, exists := latestByTask[run.TaskID]
		if !exists || run.RunID > prev.RunID {
			latestByTask[run.TaskID] = run
		}
	}

	result := make([]SQLTaskRun, 0, len(latestByTask))
	for _, run := range latestByTask {
		result = append(result, run)
	}
	slices.SortFunc(result, func(a, b SQLTaskRun) int {
		if c := cmp.Compare(a.TaskID, b.TaskID); c != 0 {
			return c
		}
		return cmp.Compare(b.RunID, a.RunID)
	})
	return result, nil
}

func (s *memTaskStorage) AcquireSQLTaskRun(ctx context.Context, sqlTask SQLTask, run SQLTaskRun, admission *SQLTaskRunAdmission) (uint64, error) {
	if err := validateSQLTaskAdmission(sqlTask, run, admission); err != nil {
		return 0, err
	}
	s.Lock()
	defer s.Unlock()

	current, ok := s.sqlTasks[sqlTask.TaskID]
	if !ok || current.AccountID != sqlTask.AccountID || current.TaskName != sqlTask.TaskName {
		return 0, ErrSQLTaskNotFound
	}
	if admission.AsyncTaskID != 0 {
		async, ok := s.asyncTasks[admission.AsyncTaskID]
		if !ok {
			return 0, ErrSQLTaskStaleFiring
		}
		if err := validateSQLTaskClaim(current, run, async, s.sqlTaskClaims[async.ID], admission); err != nil {
			return 0, err
		}
	}
	if !admission.Reserve(current.AccountID, current.TaskName) {
		return 0, ErrSQLTaskRecoveryCapacity
	}
	for _, existing := range s.sqlTaskRuns {
		if existing.AccountID == current.AccountID && existing.TaskName == current.TaskName && existing.Status == SQLTaskStatusRunning {
			return 0, ErrSQLTaskOverlap
		}
	}

	run.TaskID, run.AccountID, run.TaskName = current.TaskID, current.AccountID, current.TaskName
	run.RunID = s.nextIDLocked()
	s.sqlTaskRuns[run.RunID] = run
	if admission.AsyncTaskID != 0 {
		s.sqlTaskClaims[admission.AsyncTaskID] = run.RunID
	}
	return run.RunID, nil
}

func (s *memTaskStorage) PublishSQLTaskReceipt(ctx context.Context, request SQLTaskReceiptRequest) (bool, error) {
	receipt := &request
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.Lock()
	defer s.Unlock()
	if receipt.Kind == SQLTaskReceiptRetryAdmission {
		previous, previousExists := s.sqlTaskRuns[receipt.Run.RunID]
		_, nextExists := s.sqlTaskRuns[receipt.NextRun.RunID]
		resolved := *receipt
		resolved.Kind = SQLTaskReceiptTerminal
		if nextExists {
			if !previousExists || !sameSQLTaskReceiptResult(previous, receipt.Run) {
				return false, errSQLTaskReceiptConflict
			}
			resolved.Run = receipt.NextRun
		}
		receipt = &resolved
	}

	if receipt.Kind != SQLTaskReceiptTerminal && receipt.Kind != SQLTaskReceiptUnknown && receipt.Kind != SQLTaskReceiptAdmission {
		return false, ErrNotReady
	}
	frozen := receipt.Run
	existing, exists := s.sqlTaskRuns[frozen.RunID]
	if !exists {
		if receipt.Kind == SQLTaskReceiptAdmission {
			if async, ok := s.asyncTasks[receipt.Claim.ID]; ok && async.Metadata.Executor == task.TaskCode_SQLTask && async.Status == task.TaskStatus_Running &&
				async.TaskRunner == receipt.Claim.Runner && async.Epoch == receipt.Claim.Epoch && s.sqlTaskClaims[async.ID] == 0 {
				async.Status = task.TaskStatus_Completed
				result := sqlTaskReceiptAsyncResult(receipt)
				async.ExecuteResult, async.CompletedAt = &result, receipt.EndAt
				s.asyncTasks[async.ID] = async
			}
			return true, nil
		}
		return false, errSQLTaskReceiptConflict
	}
	if existing.TaskID != frozen.TaskID || existing.AccountID != frozen.AccountID || existing.TaskName != frozen.TaskName {
		return false, errSQLTaskReceiptConflict
	}
	if existing.Status == SQLTaskStatusUnknown {
		return true, nil
	}
	if existing.Status != SQLTaskStatusRunning {
		if receipt.Kind == SQLTaskReceiptUnknown || !sameSQLTaskReceiptResult(existing, frozen) {
			return false, errSQLTaskReceiptConflict
		}
	} else if existing.RunnerCN != frozen.RunnerCN || existing.AttemptNumber != frozen.AttemptNumber {
		return false, errSQLTaskReceiptConflict
	} else if receipt.Kind == SQLTaskReceiptUnknown {
		existing.ErrorCode, existing.ErrorMessage, existing.GateResult = frozen.ErrorCode, frozen.ErrorMessage, frozen.GateResult
		s.sqlTaskRuns[frozen.RunID] = existing
	} else {
		s.sqlTaskRuns[frozen.RunID] = applySQLTaskRunOutcome(existing, frozen)
	}
	if receipt.Claim.ID != 0 {
		async, exists := s.asyncTasks[receipt.Claim.ID]
		if exists && async.Status == task.TaskStatus_Running && s.sqlTaskClaims[async.ID] == frozen.RunID {
			async.Status = task.TaskStatus_Completed
			result := sqlTaskReceiptAsyncResult(receipt)
			async.ExecuteResult = &result
			async.CompletedAt = receipt.EndAt
			s.asyncTasks[async.ID] = async
		}
	}
	return true, nil
}

// Finish the old attempt and acquire the new one under the same store lock.
func (s *memTaskStorage) TransitionSQLTaskRun(ctx context.Context, retry SQLTaskRetryRequest) (SQLTaskRetryTransition, error) {
	retry = snapshotSQLTaskRetry(retry)
	if err := ctx.Err(); err != nil {
		return SQLTaskRetryTransition{}, err
	}
	s.Lock()
	defer s.Unlock()
	old, ok := s.sqlTaskRuns[retry.Previous.RunID]
	if !ok || old.Status != SQLTaskStatusRunning || old.TaskID != retry.Previous.TaskID ||
		old.AccountID != retry.Previous.AccountID || old.RunnerCN != retry.Previous.RunnerCN ||
		old.AttemptNumber != retry.Previous.AttemptNumber {
		return SQLTaskRetryTransition{}, errSQLTaskReceiptConflict
	}
	definition, exists := s.sqlTasks[old.TaskID]
	var async task.AsyncTask
	var pointer uint64
	if retry.Claim != nil {
		async, pointer = s.asyncTasks[retry.Claim.ID], s.sqlTaskClaims[retry.Claim.ID]
	}
	allowed := canRetrySQLTask(definition, exists, retry, async, pointer)
	old = applySQLTaskRunOutcome(old, retry.Previous)
	s.sqlTaskRuns[old.RunID] = old
	if !allowed {
		if retry.Claim != nil && async.ID != 0 && async.Status == task.TaskStatus_Running && pointer == old.RunID {
			result := sqlTaskReceiptAsyncResult(&SQLTaskReceiptRequest{Kind: SQLTaskReceiptTerminal, Run: old})
			async.Status, async.ExecuteResult, async.CompletedAt = task.TaskStatus_Completed, &result, old.FinishedAt.UnixMilli()
			s.asyncTasks[async.ID] = async
		}
		return SQLTaskRetryTransition{CommitAttempted: true}, nil
	}
	next := retry.Next
	next.RunID = s.nextIDLocked()
	s.sqlTaskRuns[next.RunID] = next
	if retry.Claim != nil {
		s.sqlTaskClaims[retry.Claim.ID] = next.RunID
	}
	return SQLTaskRetryTransition{RunID: next.RunID, Continued: true, CommitAttempted: true}, nil
}

func (s *memTaskStorage) ReconcileSQLTaskRetry(ctx context.Context, retry SQLTaskRetryRequest, nextID uint64) (SQLTaskRetryResolution, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	retry = snapshotSQLTaskRetry(retry)
	s.RLock()
	defer s.RUnlock()
	previous, previousExists := s.sqlTaskRuns[retry.Previous.RunID]
	next, nextExists := s.sqlTaskRuns[nextID]
	var async task.AsyncTask
	var pointer uint64
	if retry.Claim != nil {
		async, pointer = s.asyncTasks[retry.Claim.ID], s.sqlTaskClaims[retry.Claim.ID]
	}
	return resolveSQLTaskRetry(retry, nextID, previous, previousExists, next, nextExists, async, pointer)
}

func (s *memTaskStorage) TriggerSQLTask(ctx context.Context, sqlTask SQLTask, asyncTask task.AsyncTask) (int, error) {
	if err := validateSQLTaskTriggerSequence(sqlTask.TriggerCount); err != nil {
		return 0, err
	}
	s.Lock()
	defer s.Unlock()

	if _, ok := s.asyncTaskIndexes[asyncTask.Metadata.ID]; ok {
		return 0, nil
	}
	current, ok := s.sqlTasks[sqlTask.TaskID]
	if !ok || !current.Enabled || current.ScheduleGeneration != sqlTask.ScheduleGeneration ||
		current.TriggerCount != sqlTask.TriggerCount-1 {
		return 0, nil
	}

	asyncTask.ID = s.nextIDLocked()
	s.asyncTasks[asyncTask.ID] = asyncTask
	s.asyncTaskIndexes[asyncTask.Metadata.ID] = asyncTask.ID
	current.TriggerCount = sqlTask.TriggerCount
	current.NextFireTime = sqlTask.NextFireTime
	current.UpdatedAt = sqlTask.UpdatedAt
	s.sqlTasks[sqlTask.TaskID] = current
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

func (s *memTaskStorage) UpdateDaemonTaskError(ctx context.Context, claim task.DaemonTask, release bool) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.preUpdate != nil {
		s.preUpdate()
	}
	s.Lock()
	defer s.Unlock()
	current, ok := s.daemonTasks[claim.ID]
	if !ok || current.TaskStatus != task.TaskStatus_Running || !sameDaemonClaim(current, claim) {
		return 0, nil
	}
	current.Details = cloneDaemonTaskDetails(claim.Details)
	current.UpdateAt = claim.UpdateAt
	if release {
		current.TaskStatus = task.TaskStatus_RestartRequested
		current.TaskRunner = ""
		current.LastHeartbeat = time.Time{}
	}
	s.daemonTasks[claim.ID] = current
	return 1, nil
}

func (s *memTaskStorage) UpdateDaemonTaskStatus(
	ctx context.Context,
	taskID uint64,
	status task.TaskStatus,
	updateAt time.Time,
	endAt time.Time,
	conds ...Condition,
) (int, error) {
	if s.preUpdate != nil {
		s.preUpdate()
	}

	c := newConditions(conds...)

	s.Lock()
	defer s.Unlock()

	t, ok := s.daemonTasks[taskID]
	if !ok || !s.filterDaemonTask(c, t) {
		return 0, nil
	}
	t.TaskStatus = status
	t.UpdateAt = updateAt
	t.EndAt = endAt
	s.daemonTasks[taskID] = t
	return 1, nil
}

func (s *memTaskStorage) RequestDaemonTaskStatus(
	ctx context.Context, observed task.DaemonTask, status task.TaskStatus,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.preUpdate != nil {
		s.preUpdate()
	}
	s.Lock()
	defer s.Unlock()
	t, ok := s.daemonTasks[observed.ID]
	if !ok || t.AccountID != observed.AccountID || t.TaskStatus != observed.TaskStatus || !sameDaemonClaim(t, observed) {
		return 0, nil
	}
	t.TaskStatus = status
	s.daemonTasks[observed.ID] = t
	return 1, nil
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
	slices.SortFunc(sortedTasks, func(a, b task.DaemonTask) int { return cmp.Compare(a.ID, b.ID) })

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
		if current, ok := s.daemonTasks[t.ID]; ok {
			if current.TaskRunner != t.TaskRunner ||
				!current.LastRun.Equal(t.LastRun) {
				continue
			}
			n++
			current.LastHeartbeat = t.LastHeartbeat
			s.daemonTasks[t.ID] = current
		}
	}
	return n, nil
}

func (s *memTaskStorage) ValidateDaemonTask(
	ctx context.Context,
	t task.DaemonTask,
) (bool, error) {
	s.RLock()
	defer s.RUnlock()
	current, ok := s.daemonTasks[t.ID]
	return ok &&
		daemonTaskStatusAuthorizesEffect(t.TaskStatus, current.TaskStatus) &&
		current.TaskRunner == t.TaskRunner &&
		current.LastRun.Equal(t.LastRun), nil
}

func (s *memTaskStorage) nextIDLocked() uint64 {
	s.id++
	return s.id
}

func (s *memTaskStorage) filterAsyncTask(c *conditions, task task.AsyncTask) bool {
	ok := true
	if cond, exists := (*c)[condSQLTaskUnclaimed]; exists && !cond.eval(sqlTaskAsyncOwnership{task: task, runID: s.sqlTaskClaims[task.ID]}) {
		return false
	}
	if cond, exists := (*c)[condSQLTaskLane]; exists && !cond.eval(task.Metadata.Executor) {
		return false
	}
	if cond, exists := (*c)[condSQLTaskParentExcluded]; exists && !cond.eval(task.ParentTaskID) {
		return false
	}
	if cond, exists := (*c)[CondTaskExecutor]; exists && !cond.eval(task.Metadata.Executor) {
		return false
	}

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

	if cond, e := (*c)[CondTaskExecutor]; e {
		ok = cond.eval(task.Metadata.Executor)
	}
	if !ok {
		return false
	}

	if cond, e := (*c)[CondTaskType]; e && !cond.eval(task.TaskType.String()) {
		return false
	}
	if cond, e := (*c)[CondAccountID]; e && !cond.eval(task.AccountID) {
		return false
	}
	if cond, e := (*c)[CondAccount]; e && !cond.eval(task.Account) {
		return false
	}
	if cond, e := (*c)[CondLastHeartbeat]; e && !cond.eval(task.LastHeartbeat.UnixNano()) {
		return false
	}
	if cond, e := (*c)[CondLastRun]; e {
		ok = cond.eval(task.LastRun)
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
		case CondSQLTaskRunner:
			if !cond.eval(run.RunnerCN) {
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
