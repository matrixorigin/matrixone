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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

const (
	FJ_EmptyDB  = ""
	FJ_EmptyTBL = ""
)

const (
	FJ_CommitDelete  = "fj/commit/delete"
	FJ_CommitSlowLog = "fj/commit/slowlog"
	FJ_TransferSlow  = "fj/transfer/slow"
	FJ_FlushTimeout  = "fj/flush/timeout"

	FJ_PrefetchThreshold = "fj/prefetch/threshold"

	FJ_LogRanges         = "fj/log/ranges"
	FJ_LogPartitionState = "fj/log/partitionstate"

	FJ_CNRecvErr        = "fj/cn/recv/err"
	FJ_CNSubSysErr      = "fj/cn/recv/subsyserr"
	FJ_CNReplayCacheErr = "fj/cn/recv/rcacheerr"

	FJ_LogReader    = "fj/log/reader"
	FJ_LogWorkspace = "fj/log/workspace"
)

const (
	FJ_LogLevel0 = iota
	FJ_LogLevel1
	FJ_LogLevel2
	FJ_LogLevel3
)

// ParseFJIntArgs parses the injected fault integer argument
// and returns the log level and function id
// logLevel = iarg % 10 and funcId = iarg / 10
// iarg quick mapping:
// 0->3: logLevel = 0->3, database name equal
// 10->13: logLevel = 0->3, database name contains
// 20->23: logLevel = 0->3, table name equal
// 30->33: logLevel = 0->3, table name contains
// 40->43: logLevel = 0->3, database name and table name contains
func ParseLoggingIntArgs(iarg int) (logLevel int, funcId int) {
	logLevel = iarg % 10
	if logLevel >= FJ_LogLevel3 {
		logLevel = FJ_LogLevel3
	}
	funcId = iarg / 10
	return
}

func ParseLoggingSArgs(sarg string, funcId int, args ...string) bool {
	if len(args) == 0 {
		return false
	}
	switch funcId {
	case 0: // equal args[0]
		return sarg == args[0] || args[0] == ""
	case 1: // contains args[0]
		return strings.Contains(sarg, args[0])
	case 2: // equal args[1]
		if len(args) < 2 {
			return false
		}
		return sarg == args[1] || args[1] == ""
	case 3: // contains args[1]
		if len(args) < 2 {
			return false
		}
		return strings.Contains(sarg, args[1])
	case 4: // contains args[0] and args[1]
		params := strings.Split(sarg, ".")
		if len(params) != 2 {
			return false
		}
		return strings.Contains(params[0], args[0]) && strings.Contains(params[1], args[1])
	default:
		return false
	}
}

func makeInjectIntArg(level, funcId int) int {
	return level + funcId*10
}

func MakeInjectTableLoggingIntArg(level int, isEqual bool) int {
	// 0 means equal database name
	// 1 means contains database name
	// 2 means equal table name
	// 3 means contains table name
	// 4 means contains database name and table name
	if isEqual {
		return makeInjectIntArg(level, 2)
	}
	return makeInjectIntArg(level, 3)
}

func MakeInjectDBLoggingIntArg(level int, isEqual bool) int {
	if isEqual {
		return makeInjectIntArg(level, 0)
	}
	return makeInjectIntArg(level, 1)
}

func MakeInjectDBAndTableLoggingIntArg(level int) int {
	return makeInjectIntArg(level, 4)
}

func checkLoggingArgs(
	iarg int, sarg string, inputArgs ...string,
) (bool, int) {
	level, funcId := ParseLoggingIntArgs(iarg)
	ok := ParseLoggingSArgs(sarg, funcId, inputArgs...)
	if !ok {
		return false, 0
	}
	return ok, level
}

func LogWorkspaceInjected(args ...string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_LogWorkspace)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(int(iarg), sarg, args...)
}

// `name` is the table name
// return injected, logLevel
func LogReaderInjected(args ...string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_LogReader)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(int(iarg), sarg, args...)
}

func InjectLogReader(
	databaseName string,
	tableName string,
	level int,
) (rmFault func(), err error) {
	return InjectLogging(
		FJ_LogReader,
		databaseName,
		tableName,
		level,
		true,
	)
}

func InjectLogWorkspace(
	databaseName string,
	tableName string,
	level int,
) (rmFault func(), err error) {
	return InjectLogging(
		FJ_LogWorkspace,
		databaseName,
		tableName,
		level,
		true,
	)
}

func InjectLogPartitionState(
	databaseName string,
	tableName string,
	level int,
) (rmFault func(), err error) {
	return InjectLogging(
		FJ_LogPartitionState,
		databaseName,
		tableName,
		level,
		false,
	)
}

func InjectLogging(
	key string,
	databaseName string,
	tableName string,
	level int,
	isEqual bool,
) (rmFault func(), err error) {
	var (
		iarg int64
		sarg string
	)
	if databaseName != FJ_EmptyDB && tableName != FJ_EmptyTBL {
		iarg = int64(MakeInjectDBAndTableLoggingIntArg(level))
		sarg = databaseName + "." + tableName
	} else if databaseName != "" {
		iarg = int64(MakeInjectDBLoggingIntArg(level, isEqual))
		sarg = databaseName
	} else if tableName != "" {
		iarg = int64(MakeInjectTableLoggingIntArg(level, isEqual))
		sarg = tableName
	} else {
		return func() {}, nil
	}
	if err = fault.AddFaultPoint(
		context.Background(),
		key,
		":::",
		"echo",
		iarg,
		sarg,
		false,
	); err != nil {
		return
	}
	rmFault = func() {
		fault.RemoveFaultPoint(context.Background(), key)
	}
	return
}

func CNRecvErrInjected() (bool, int) {
	p, _, injected := fault.TriggerFault(FJ_CNRecvErr)
	return injected, int(p)
}

func CNSubSysErrInjected() (bool, int) {
	p, _, injected := fault.TriggerFault(FJ_CNSubSysErr)
	return injected, int(p)
}

func CNReplayCacheErrInjected() (bool, int) {
	p, _, injected := fault.TriggerFault(FJ_CNReplayCacheErr)
	return injected, int(p)
}

func RangesLogInjected(dbName, tableName string) (bool, int) {
	_, sarg, injected := fault.TriggerFault(FJ_LogRanges)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(0, sarg, dbName, tableName)
}

func InjectPrefetchThreshold(threshold int) (rmFault func(), err error) {
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_PrefetchThreshold,
		":::",
		"echo",
		int64(threshold),
		"",
		false,
	); err != nil {
		return
	}
	rmFault = func() {
		fault.RemoveFaultPoint(context.Background(), FJ_PrefetchThreshold)
	}
	return
}

// bool: injected or not
// int: threshold. 1 means 1 millisecond
func PrefetchMetaThresholdInjected() (bool, int) {
	iarg, _, injected := fault.TriggerFault(FJ_PrefetchThreshold)
	if !injected {
		return false, 0
	}
	return true, int(iarg)
}

func InjectLogRanges(
	ctx context.Context,
	databaseName string,
	tableName string,
) (rmFault func(), err error) {
	return InjectLogging(
		FJ_LogRanges,
		databaseName,
		tableName,
		0,
		true,
	)
}

func PartitionStateInjected(dbName, tableName string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_LogPartitionState)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(int(iarg), sarg, dbName, tableName)
}
