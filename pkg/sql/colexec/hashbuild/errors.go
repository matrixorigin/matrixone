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

package hashbuild

import (
	"context"
	"errors"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// TerminalBudgetError converts only an unrecoverable capacity admission at an
// operator's public Call boundary. Spill and other recovery paths must keep the
// typed admission error until they have exhausted every recovery option.
func TerminalBudgetError(ctx context.Context, err error) error {
	return terminalBudgetError(ctx, "hash build", true, err)
}

// TerminalBudgetErrorForOperator applies the same client-facing conversion to
// operators that share the execution-resource admission contract with
// HashBuild. The operator name keeps the resulting resource-exhausted message
// actionable without exposing the internal admission sentinel.
func TerminalBudgetErrorForOperator(
	ctx context.Context,
	operator string,
	err error,
) error {
	return terminalBudgetError(ctx, operator, false, err)
}

func terminalBudgetError(
	ctx context.Context,
	operator string,
	normalizePhysical bool,
	err error,
) error {
	if err == nil {
		return err
	}
	// YieldError is scheduler control flow, not an execution failure. Keep it
	// discoverable by the pipeline continuation even when an operator reaches
	// this resource-error boundary while waiting on a child or mailbox.
	if _, ok := vm.AsYieldError(err); ok {
		return err
	}
	// A joined lifecycle/accounting failure is not a capacity rejection. Keep
	// it intact so cancellation and cleanup bugs cannot masquerade as OOM.
	if errors.Is(err, process.ErrExecutionResourceClosed) ||
		errors.Is(err, process.ErrExecutionResourceInvalid) ||
		errors.Is(err, process.ErrExecutionMemoryCeilingMissing) {
		return err
	}

	var budgetErr *process.ExecutionResourceError
	if !errors.As(err, &budgetErr) || budgetErr.Kind != process.ExecutionResourceErrorAdmission {
		switch {
		case mpool.AllocationFailureReasonOf(err) ==
			mpool.AllocationFailureCapacity &&
			!mpool.IsMPoolCapacityFailure(err):
			if !normalizePhysical {
				return err
			}
			return moerr.NewResourceExhaustedf(ctx,
				"%s memory budget exceeded; reduce query width or concurrency, or increase processLimitationSize",
				operator)
		case errors.Is(err, process.ErrExecutionResourceAdmission):
			return moerr.NewResourceExhaustedf(ctx,
				"%s resource budget exceeded; inspect execution-resource metrics and resource limits",
				operator)
		default:
			return err
		}
	}
	if budgetErr.Component == 0 {
		reason := terminalBudgetReason(budgetErr.Message)
		if reason != "" {
			return moerr.NewResourceExhaustedf(ctx, "%s", reason)
		}
		return moerr.NewResourceExhaustedf(
			ctx,
			"%s resource budget exceeded; inspect execution-resource metrics and resource limits",
			operator,
		)
	}

	reason := terminalBudgetReason(budgetErr.Message)
	var resource, action string
	switch budgetErr.Component {
	case process.ExecutionResourceComponentMemory:
		resource = "memory"
		if operator == "hash build" {
			action = "reduce join build width or query concurrency, increase processLimitationSize, or lower join_spill_mem for an eligible shuffle join; automatic spill can still exhaust recovery headroom for wide or skewed partitions"
		} else {
			action = "reduce query width or concurrency, or increase processLimitationSize"
		}
	case process.ExecutionResourceComponentSpillDisk:
		resource = "spill disk"
		action = "free spill storage or increase processLimitationSpillSize"
	case process.ExecutionResourceComponentSpillFD:
		resource = "spill file descriptor"
		action = "reduce concurrent spill work or raise the CN open-file limit"
	default:
		if reason != "" {
			return moerr.NewResourceExhaustedf(ctx, "%s", reason)
		}
		return moerr.NewResourceExhaustedf(
			ctx,
			"%s resource budget exceeded; inspect execution-resource metrics and resource limits",
			operator,
		)
	}
	if reason != "" {
		return moerr.NewResourceExhaustedf(
			ctx,
			"%s %s budget exceeded (requested=%d, used=%d, limit=%d); %s; %s",
			operator,
			resource,
			budgetErr.Requested,
			budgetErr.Used,
			budgetErr.Cap,
			action,
			reason,
		)
	}
	return moerr.NewResourceExhaustedf(ctx,
		"%s %s budget exceeded (requested=%d, used=%d, limit=%d); %s",
		operator, resource, budgetErr.Requested, budgetErr.Used, budgetErr.Cap, action)
}

func terminalBudgetReason(message string) string {
	message = strings.TrimSpace(message)
	sentinel := process.ErrExecutionResourceAdmission.Error()
	if message == sentinel || strings.HasPrefix(message, sentinel+": requested=") {
		return ""
	}
	reason := strings.TrimSpace(strings.ReplaceAll(
		message,
		sentinel,
		"",
	))
	reason = strings.TrimSpace(strings.TrimSuffix(reason, ":"))
	return reason
}
