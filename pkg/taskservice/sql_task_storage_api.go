// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package taskservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

// SQLTaskClaim is the identity of one scheduled async allocation.
type SQLTaskClaim struct {
	ID           uint64
	Epoch        uint32
	Runner       string
	ParentTaskID string
	MetadataID   string
}

// SQLTaskReceiptKind distinguishes a known terminal result from an uncertain
// business commit or an admission whose COMMIT acknowledgement was lost.
type SQLTaskReceiptKind uint8

const (
	// SQLTaskReceiptTerminal persists the known result and its exact async completion.
	SQLTaskReceiptTerminal SQLTaskReceiptKind = iota
	// SQLTaskReceiptUnknown preserves a RUNNING barrier with the unknown outcome.
	SQLTaskReceiptUnknown
	// SQLTaskReceiptAdmission reconciles an uncertain initial admission; no SQL started.
	SQLTaskReceiptAdmission
	// SQLTaskReceiptRetryAdmission reconciles an uncertain retry admission; the new attempt did not start.
	SQLTaskReceiptRetryAdmission
)

// SQLTaskReceiptRequest contains only durable metadata, not SQL or recovery
// worker state. Kind determines which exact identities must be reconciled.
type SQLTaskReceiptRequest struct {
	Kind    SQLTaskReceiptKind
	Run     SQLTaskRun
	NextRun SQLTaskRun
	Claim   SQLTaskClaim
	EndAt   int64 // async completion time in Unix milliseconds
}

// SQLTaskRetryRequest is the completed attempt and proposed next attempt.
// Claim is nil for a manually executed task.
type SQLTaskRetryRequest struct {
	Previous SQLTaskRun
	Next     SQLTaskRun
	Claim    *SQLTaskClaim
}

func (c sqlTaskAsyncClaim) request() SQLTaskClaim {
	return SQLTaskClaim{ID: c.id, Epoch: c.epoch, Runner: c.runner, ParentTaskID: c.parent, MetadataID: c.metadata}
}

func (c SQLTaskClaim) internal() sqlTaskAsyncClaim {
	return sqlTaskAsyncClaim{id: c.ID, epoch: c.Epoch, runner: c.Runner, parent: c.ParentTaskID, metadata: c.MetadataID}
}

func (r *sqlTaskReceipt) request() SQLTaskReceiptRequest {
	return SQLTaskReceiptRequest{Kind: SQLTaskReceiptKind(r.kind), Run: r.run, NextRun: r.nextRun, Claim: r.claim.request(), EndAt: r.endAt}
}

func (r SQLTaskReceiptRequest) internal() sqlTaskReceipt {
	return sqlTaskReceipt{kind: sqlTaskReceiptKind(r.Kind), run: r.Run, nextRun: r.NextRun, claim: r.Claim.internal(), endAt: r.EndAt}
}

func (r sqlTaskRetry) request() SQLTaskRetryRequest {
	request := SQLTaskRetryRequest{Previous: r.previous, Next: r.next}
	if r.claim != nil {
		claim := r.claim.request()
		request.Claim = &claim
	}
	return request
}

func (r SQLTaskRetryRequest) internal() sqlTaskRetry {
	retry := sqlTaskRetry{previous: r.Previous, next: r.Next}
	if r.Claim != nil {
		claim := r.Claim.internal()
		retry.claim = &claim
	}
	return retry
}

func publishSQLTaskReceiptRequest(ctx context.Context, store sqlTaskReceiptStore, request SQLTaskReceiptRequest) (bool, error) {
	receipt := request.internal()
	return store.publishSQLTaskReceipt(ctx, &receipt)
}

func (m *mysqlTaskStorage) PublishSQLTaskReceipt(ctx context.Context, request SQLTaskReceiptRequest) (bool, error) {
	return publishSQLTaskReceiptRequest(ctx, m, request)
}

func (s *memTaskStorage) PublishSQLTaskReceipt(ctx context.Context, request SQLTaskReceiptRequest) (bool, error) {
	return publishSQLTaskReceiptRequest(ctx, s, request)
}

func (m *mysqlTaskStorage) TransitionSQLTaskRun(ctx context.Context, request SQLTaskRetryRequest) (uint64, bool, error) {
	return m.transitionSQLTaskRun(ctx, request.internal())
}

func (s *memTaskStorage) TransitionSQLTaskRun(ctx context.Context, request SQLTaskRetryRequest) (uint64, bool, error) {
	return s.transitionSQLTaskRun(ctx, request.internal())
}

func (m *mysqlTaskStorage) ForceRecoverSQLTaskRun(ctx context.Context, account uint32, name string, runID uint64, actor string) (bool, error) {
	return m.forceRecoverSQLTaskRun(ctx, account, name, runID, actor)
}

func (s *memTaskStorage) ForceRecoverSQLTaskRun(ctx context.Context, account uint32, name string, runID uint64, actor string) (bool, error) {
	return s.forceRecoverSQLTaskRun(ctx, account, name, runID, actor)
}

func (m *mysqlTaskStorage) FinishSQLTaskUnclaimed(ctx context.Context, claim SQLTaskClaim, result task.ExecuteResult, endAt int64) error {
	return m.finishSQLTaskUnclaimed(ctx, claim.internal(), result, endAt)
}

func (s *memTaskStorage) FinishSQLTaskUnclaimed(ctx context.Context, claim SQLTaskClaim, result task.ExecuteResult, endAt int64) error {
	return s.finishSQLTaskUnclaimed(ctx, claim.internal(), result, endAt)
}

func (m *mysqlTaskStorage) DeleteStaleSQLTaskBatch(ctx context.Context, taskID uint64) (int, error) {
	return m.deleteStaleSQLTaskBatch(ctx, taskID)
}

func (s *memTaskStorage) DeleteStaleSQLTaskBatch(ctx context.Context, taskID uint64) (int, error) {
	return s.deleteStaleSQLTaskBatch(ctx, taskID)
}

func (s *refreshableTaskStorage) PublishSQLTaskReceipt(ctx context.Context, request SQLTaskReceiptRequest) (bool, error) {
	var resolved bool
	err := s.withSQLTaskMetadataStore(ctx, func(store TaskStorage) error {
		var err error
		resolved, err = store.PublishSQLTaskReceipt(ctx, request)
		return err
	})
	return resolved, err
}

func (s *refreshableTaskStorage) TransitionSQLTaskRun(ctx context.Context, request SQLTaskRetryRequest) (uint64, bool, error) {
	var id uint64
	var continued bool
	err := s.withSQLTaskMetadataStore(ctx, func(store TaskStorage) error {
		var err error
		id, continued, err = store.TransitionSQLTaskRun(ctx, request)
		return err
	})
	return id, continued, err
}

func (s *refreshableTaskStorage) ForceRecoverSQLTaskRun(ctx context.Context, account uint32, name string, runID uint64, actor string) (bool, error) {
	var changed bool
	err := s.withSQLTaskMetadataStore(ctx, func(store TaskStorage) error {
		var err error
		changed, err = store.ForceRecoverSQLTaskRun(ctx, account, name, runID, actor)
		return err
	})
	return changed, err
}

func (s *refreshableTaskStorage) FinishSQLTaskUnclaimed(ctx context.Context, claim SQLTaskClaim, result task.ExecuteResult, endAt int64) error {
	return s.withSQLTaskMetadataStore(ctx, func(store TaskStorage) error {
		return store.FinishSQLTaskUnclaimed(ctx, claim, result, endAt)
	})
}

func (s *refreshableTaskStorage) DeleteStaleSQLTaskBatch(ctx context.Context, taskID uint64) (int, error) {
	var deleted int
	err := s.withSQLTaskMetadataStore(ctx, func(store TaskStorage) error {
		var err error
		deleted, err = store.DeleteStaleSQLTaskBatch(ctx, taskID)
		return err
	})
	return deleted, err
}

func (s *refreshableTaskStorage) ScanAsyncTaskIDs(ctx context.Context, after, high uint64, descending bool, limit int) ([]uint64, error) {
	var ids []uint64
	err := s.withSQLTaskFetchStore(ctx, func(store TaskStorage) error {
		var err error
		ids, err = store.ScanAsyncTaskIDs(ctx, after, high, descending, limit)
		return err
	})
	return ids, err
}
