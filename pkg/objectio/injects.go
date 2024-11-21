// Copyright 2021 - 2022 Matrix Origin
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

package objectio

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

const (
	FJ_CommitDelete  = "fj/commit/delete"
	FJ_CommitSlowLog = "fj/commit/slowlog"
	FJ_TransferSlow  = "fj/transfer/slow"
	FJ_FlushTimeout  = "fj/flush/timeout"

	FJ_TraceRanges         = "fj/trace/ranges"
	FJ_TracePartitionState = "fj/trace/partitionstate"

	FJ_Debug19524 = "fj/debug/19524"
	FJ_Debug19787 = "fj/debug/19787"

	FJ_LogReader    = "fj/log/reader"
	FJ_LogWorkspace = "fj/log/workspace"
)

func Debug19524Injected() bool {
	_, _, injected := fault.TriggerFault(FJ_Debug19524)
	return injected
}

func Debug19787Injected() bool {
	_, _, injected := fault.TriggerFault(FJ_Debug19787)
	return injected
}

func RangesInjected(name string) bool {
	_, sarg, injected := fault.TriggerFault(FJ_TraceRanges)
	if !injected {
		return false
	}
	return sarg == name
}

func InjectRanges(
	ctx context.Context,
	name string,
) (rmFault func(), err error) {
	rmFault = func() {}
	if err = fault.AddFaultPoint(
		ctx,
		FJ_TraceRanges,
		":::",
		"echo",
		0,
		name,
	); err != nil {
		return
	}
	rmFault = func() {
		fault.RemoveFaultPoint(ctx, FJ_TraceRanges)
	}
	return
}

func PartitionStateInjected(name string) bool {
	_, sarg, injected := fault.TriggerFault(FJ_TracePartitionState)
	if !injected {
		return false
	}
	return sarg == name
}
