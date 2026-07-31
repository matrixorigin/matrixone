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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// TerminalBudgetError converts only an unrecoverable capacity admission at an
// operator's public Call boundary. Spill and other recovery paths must keep the
// typed admission error until they have exhausted every recovery option.
func TerminalBudgetError(ctx context.Context, err error) error {
	if err == nil || !errors.Is(err, process.ErrHashBuildBudgetAdmission) {
		return err
	}
	// A joined lifecycle/accounting failure is not a capacity rejection. Keep
	// it intact so cancellation and cleanup bugs cannot masquerade as OOM.
	if errors.Is(err, process.ErrHashBuildBudgetClosed) ||
		errors.Is(err, process.ErrHashBuildBudgetInvalid) ||
		errors.Is(err, process.ErrHashBuildCeilingMissing) {
		return err
	}

	var budgetErr *process.HashBuildBudgetError
	if !errors.As(err, &budgetErr) || budgetErr.Kind != process.HashBuildBudgetErrorAdmission {
		return moerr.NewResourceExhaustedf(
			ctx,
			"hash build resource budget exceeded; inspect hash-build budget metrics and resource limits",
		)
	}

	reason := terminalBudgetReason(budgetErr.Message)
	var resource, action string
	switch budgetErr.Component {
	case process.HashBuildBudgetComponentMemory:
		resource = "memory"
		action = "reduce join build width or query concurrency, increase processLimitationSize, or lower join_spill_mem for an eligible shuffle join; automatic spill can still exhaust recovery headroom for wide or skewed partitions"
	case process.HashBuildBudgetComponentSpillDisk:
		resource = "spill disk"
		action = "free spill storage or increase processLimitationSpillSize"
	case process.HashBuildBudgetComponentSpillFD:
		resource = "spill file descriptor"
		action = "reduce concurrent spill work or raise the CN open-file limit"
	default:
		if reason != "" {
			return moerr.NewResourceExhaustedf(ctx, "%s", reason)
		}
		return moerr.NewResourceExhaustedf(
			ctx,
			"hash build resource budget exceeded; inspect hash-build budget metrics and resource limits",
		)
	}
	if reason != "" {
		return moerr.NewResourceExhaustedf(
			ctx,
			"hash build %s budget exceeded (requested=%d, used=%d, limit=%d); %s; %s",
			resource,
			budgetErr.Requested,
			budgetErr.Used,
			budgetErr.Cap,
			action,
			reason,
		)
	}
	return moerr.NewResourceExhaustedf(ctx,
		"hash build %s budget exceeded (requested=%d, used=%d, limit=%d); %s",
		resource, budgetErr.Requested, budgetErr.Used, budgetErr.Cap, action)
}

func terminalBudgetReason(message string) string {
	message = strings.TrimSpace(message)
	sentinel := process.ErrHashBuildBudgetAdmission.Error()
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
