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

	FJ_LogReader    = "fj/log/reader"
	FJ_LogWorkspace = "fj/log/workspace"
)

const (
	FJ_C_AllNames = "_%_all_"
)

func LogWorkspaceInjected(name string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_LogWorkspace)
	if !injected {
		return false, 0
	}
	if sarg == name || sarg == FJ_C_AllNames {
		return true, int(iarg)
	}
	return false, 0
}

// `name` is the table name
// return injected, logLevel
func LogReaderInjected(name string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_LogReader)
	if !injected {
		return false, 0
	}
	if sarg != name {
		return false, 0
	}
	return true, int(iarg)
}

// inject log reader and partition state
// `name` is the table name
func InjectLog1(
	name string,
	level int,
) (rmFault func(), err error) {
	rmFault = func() {}
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_LogReader,
		":::",
		"echo",
		int64(level),
		name,
	); err != nil {
		return
	}
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_TracePartitionState,
		":::",
		"echo",
		0,
		name,
	); err != nil {
		fault.RemoveFaultPoint(context.Background(), FJ_LogReader)
		return
	}

	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_LogWorkspace,
		":::",
		"echo",
		int64(level),
		name,
	); err != nil {
		fault.RemoveFaultPoint(context.Background(), FJ_LogReader)
		fault.RemoveFaultPoint(context.Background(), FJ_TracePartitionState)
		return
	}

	rmFault = func() {
		fault.RemoveFaultPoint(context.Background(), FJ_LogWorkspace)
		fault.RemoveFaultPoint(context.Background(), FJ_TracePartitionState)
		fault.RemoveFaultPoint(context.Background(), FJ_LogReader)
	}
	return
}

func Debug19524Injected() bool {
	_, _, injected := fault.TriggerFault(FJ_Debug19524)
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
