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

// SQLTaskRetryTransition distinguishes a failed transaction from an
// uncertain COMMIT, including transitions that did not create a successor.
type SQLTaskRetryTransition struct {
	RunID           uint64
	Continued       bool
	CommitAttempted bool
}

type SQLTaskRetryResolution uint8

const (
	SQLTaskRetryRolledBack SQLTaskRetryResolution = iota
	SQLTaskRetryAdmitted
	SQLTaskRetryStopped
)

// snapshotSQLTaskRetry keeps the caller's claim independent of a storage
// decorator, and the storage transaction independent of the decorator.
func snapshotSQLTaskRetry(request SQLTaskRetryRequest) SQLTaskRetryRequest {
	if request.Claim != nil {
		claim := *request.Claim
		request.Claim = &claim
	}
	return request
}

func (m *mysqlTaskStorage) ForceRecoverSQLTaskRun(ctx context.Context, account uint32, name string, runID uint64, actor string) (bool, error) {
	return m.forceRecoverSQLTaskRun(ctx, account, name, runID, actor)
}

func (s *memTaskStorage) ForceRecoverSQLTaskRun(ctx context.Context, account uint32, name string, runID uint64, actor string) (bool, error) {
	return s.forceRecoverSQLTaskRun(ctx, account, name, runID, actor)
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

func (s *refreshableTaskStorage) TransitionSQLTaskRun(ctx context.Context, request SQLTaskRetryRequest) (SQLTaskRetryTransition, error) {
	var result SQLTaskRetryTransition
	err := s.withSQLTaskMetadataStore(ctx, func(store TaskStorage) error {
		var err error
		result, err = store.TransitionSQLTaskRun(ctx, request)
		return err
	})
	return result, err
}

func (s *refreshableTaskStorage) ReconcileSQLTaskRetry(ctx context.Context, request SQLTaskRetryRequest, nextID uint64) (SQLTaskRetryResolution, error) {
	var result SQLTaskRetryResolution
	err := s.withSQLTaskMetadataStore(ctx, func(store TaskStorage) error {
		var err error
		result, err = store.ReconcileSQLTaskRetry(ctx, request, nextID)
		return err
	})
	return result, err
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

func (s *refreshableTaskStorage) ScanAsyncTaskPage(ctx context.Context, after, high uint64, descending bool, limit int) ([]AsyncTaskScanRow, error) {
	var page []AsyncTaskScanRow
	err := s.withSQLTaskFetchStore(ctx, func(store TaskStorage) error {
		var err error
		page, err = store.ScanAsyncTaskPage(ctx, after, high, descending, limit)
		return err
	})
	return page, err
}
