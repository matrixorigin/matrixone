// Copyright 2026 Matrix Origin
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

package icebergwrite

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	icebergwritecore "github.com/matrixorigin/matrixone/pkg/iceberg/write"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

func (w *IcebergWrite) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
}

func (w *IcebergWrite) OpType() vm.OpType {
	return vm.IcebergWrite
}

func (w *IcebergWrite) Prepare(proc *process.Process) error {
	if w.OpAnalyzer == nil {
		w.OpAnalyzer = process.NewAnalyzer(w.GetIdx(), w.IsFirst, w.IsLast, opName)
	} else {
		w.OpAnalyzer.Reset()
	}
	if w.Coordinator == nil {
		w.Request.ParallelID = w.GetParalleID()
		w.Request.MaxParallel = w.GetMaxParallel()
		if w.Factory != nil {
			w.refreshExecutionRequest(proc)
			w.factoryInvoked = true
			coordinator, err := w.Factory.NewCoordinator(proc.Ctx, w.Request)
			if err != nil {
				return err
			}
			w.Coordinator = coordinator
			w.coordinatorFromFactory = true
		}
		if w.Coordinator == nil {
			w.Coordinator = unsupportedCoordinator{operation: normalizedOperation(w.Request.Operation)}
		}
	}
	if !w.ctr.opened {
		if err := w.Coordinator.Begin(proc.Ctx, w.Request); err != nil {
			recoveryCtx, cancel := icebergwritecore.NewRecoveryContext(proc.Ctx)
			abortErr := w.Coordinator.Abort(recoveryCtx, err)
			cancel()
			w.discardFactoryCoordinator()
			return errors.Join(err, abortErr)
		}
		w.ctr.opened = true
	}
	return nil
}

func (w *IcebergWrite) Call(proc *process.Process) (vm.CallResult, error) {
	result, err := vm.ChildrenCall(w.GetChildren(0), proc, w.OpAnalyzer)
	if err != nil {
		return result, err
	}
	if result.Batch == nil {
		if w.ctr.opened && !w.ctr.finished {
			if err := w.Coordinator.Commit(proc.Ctx); err != nil {
				return result, err
			}
			w.ctr.finished = true
		}
		return result, nil
	}
	terminal := result.Status == vm.ExecStop || result.Batch.Last()
	if !result.Batch.IsEmpty() {
		if err := w.appendBatch(proc, result.Batch); err != nil {
			return result, err
		}
	}
	if terminal {
		if w.ctr.opened && !w.ctr.finished {
			if err := w.Coordinator.Commit(proc.Ctx); err != nil {
				return result, err
			}
			w.ctr.finished = true
		}
		return vm.CancelResult, nil
	}
	if result.Batch.IsEmpty() {
		return result, nil
	}
	result.Batch = batch.EmptyBatch
	result.Status = vm.ExecNext
	return result, nil
}

func (w *IcebergWrite) appendBatch(proc *process.Process, bat *batch.Batch) error {
	if coordinator, ok := w.Coordinator.(ProcessAwareCoordinator); ok {
		return coordinator.AppendWithProcess(proc, bat)
	}
	return w.Coordinator.Append(proc.Ctx, bat)
}

func (w *IcebergWrite) Reset(proc *process.Process, pipelineFailed bool, err error) {
	w.abortOpen(proc, err)
	w.discardFactoryCoordinator()
	w.input = vm.CallResult{}
	w.ctr = container{}
}

func (w *IcebergWrite) Free(proc *process.Process, pipelineFailed bool, err error) {
	w.abortOpen(proc, err)
	w.ReleaseObjectIORef()
	w.input = vm.CallResult{}
	w.Coordinator = nil
	w.coordinatorFromFactory = false
}

func (w *IcebergWrite) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (w *IcebergWrite) abortOpen(proc *process.Process, cause error) {
	if w == nil || w.Coordinator == nil || !w.ctr.opened || w.ctr.finished {
		return
	}
	parent := context.Background()
	if proc != nil && proc.Ctx != nil {
		parent = proc.Ctx
	}
	recoveryCtx, cancel := icebergwritecore.NewRecoveryContext(parent)
	defer cancel()
	if err := w.Coordinator.Abort(recoveryCtx, cause); err != nil {
		// Reset/Free cannot return an error through the operator contract. Keep
		// the abort bounded and observable instead of silently losing a cleanup
		// failure or skipping it because the query context was cancelled.
		logutil.Warn("Iceberg write coordinator abort failed",
			zap.String("operation", w.Request.Operation),
			zap.String("catalog", w.Request.CatalogName),
			zap.String("namespace", w.Request.Namespace),
			zap.String("table", w.Request.Table),
			zap.Error(err))
	}
	w.ctr.finished = true
}

func (w *IcebergWrite) discardFactoryCoordinator() {
	if w == nil || !w.coordinatorFromFactory {
		return
	}
	w.Coordinator = nil
	w.coordinatorFromFactory = false
}

func (w *IcebergWrite) refreshExecutionRequest(proc *process.Process) {
	if w == nil || proc == nil {
		return
	}
	compiledStatementID := w.Request.StatementID
	compiledIdempotencyKey := w.Request.IdempotencyKey
	// Never let a missing execution identity fall back to the PREPARE-time key:
	// that would make two EXECUTEs look like one idempotent commit. Runtime
	// factory validation may reject an empty identity, which is safer than
	// silently reusing a stale one.
	w.Request.StatementID = ""
	w.Request.IdempotencyKey = ""
	if sessionInfo := proc.GetSessionInfo(); sessionInfo != nil && sessionInfo.TimeZone != nil {
		w.Request.TimeZone = sessionInfo.TimeZone
	}
	if proc.Ctx != nil {
		if accountID, ok := proc.Ctx.Value(defines.TenantIDKey{}).(uint32); ok {
			w.Request.AccountID = accountID
		}
		if roleID, ok := proc.Ctx.Value(defines.RoleIDKey{}).(uint32); ok {
			w.Request.RoleID = uint64(roleID)
		}
		if userID, ok := proc.Ctx.Value(defines.UserIDKey{}).(uint32); ok {
			w.Request.UserID = uint64(userID)
		}
	}
	statementID := ""
	if profile := proc.GetStmtProfile(); profile != nil {
		if id := strings.TrimSpace(profile.GetStmtId().String()); id != "" && strings.Trim(id, "0-") != "" {
			statementID = id
		}
	}
	if statementID == "" {
		statementID = strings.TrimSpace(proc.QueryId())
	}
	if statementID == "" {
		// The first invocation may be a directly compiled, non-cached pipeline
		// without a statement profile (notably embedded/operator callers). Its
		// compiled identity belongs to this execution and remains a valid one-time
		// fallback. Once the factory has been invoked, Reset means a new execution;
		// never resurrect that old identity on subsequent invocations.
		if !w.factoryInvoked {
			w.Request.StatementID = compiledStatementID
			w.Request.IdempotencyKey = compiledIdempotencyKey
		}
		return
	}
	w.Request.StatementID = statementID
	w.Request.IdempotencyKey = statementID
}

type unsupportedCoordinator struct {
	operation string
}

type ProcessAwareCoordinator interface {
	AppendWithProcess(proc *process.Process, bat *batch.Batch) error
}

func (unsupportedCoordinator) Begin(ctx context.Context, req AppendRequest) error {
	return nil
}

func (c unsupportedCoordinator) Append(ctx context.Context, bat *batch.Batch) error {
	switch c.operation {
	case OperationOverwrite:
		return moerr.NewNotSupported(ctx, "Iceberg OVERWRITE writer data path is not implemented in this phase")
	case OperationMerge:
		return moerr.NewNotSupported(ctx, "Iceberg MERGE writer data path is not implemented in this phase")
	case OperationUpdate:
		return moerr.NewNotSupported(ctx, "Iceberg UPDATE writer data path is not implemented in this phase")
	case OperationDelete:
		return moerr.NewNotSupported(ctx, "Iceberg DELETE writer data path is not implemented in this phase")
	default:
		return moerr.NewNotSupported(ctx, "Iceberg append writer data path is not implemented in this phase")
	}
}

func (c unsupportedCoordinator) Commit(ctx context.Context) error {
	switch c.operation {
	case OperationOverwrite:
		return moerr.NewNotSupported(ctx, "Iceberg OVERWRITE writer commit path is not implemented in this phase")
	case OperationMerge:
		return moerr.NewNotSupported(ctx, "Iceberg MERGE writer commit path is not implemented in this phase")
	case OperationUpdate:
		return moerr.NewNotSupported(ctx, "Iceberg UPDATE writer commit path is not implemented in this phase")
	case OperationDelete:
		return moerr.NewNotSupported(ctx, "Iceberg DELETE writer commit path is not implemented in this phase")
	default:
		return moerr.NewNotSupported(ctx, "Iceberg append writer commit path is not implemented in this phase")
	}
}

func (unsupportedCoordinator) Abort(ctx context.Context, cause error) error {
	return nil
}

func normalizedOperation(operation string) string {
	switch operation {
	case OperationOverwrite:
		return OperationOverwrite
	case OperationMerge:
		return OperationMerge
	case OperationUpdate:
		return OperationUpdate
	case OperationDelete:
		return OperationDelete
	default:
		return OperationAppend
	}
}
