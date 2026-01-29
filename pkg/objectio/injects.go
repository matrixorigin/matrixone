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
	FJ_CommitWait    = "fj/commit/wait"
	FJ_TransferSlow  = "fj/transfer/slow"
	FJ_FlushTimeout  = "fj/flush/timeout"
	FJ_FlushEntry    = "fj/flush/entry"

	FJ_CheckpointSave = "fj/checkpoint/save"
	FJ_GCKPWait1      = "fj/gckp/wait1"

	FJ_TraceRanges         = "fj/trace/ranges"
	FJ_TracePartitionState = "fj/trace/partitionstate"
	FJ_PrefetchThreshold   = "fj/prefetch/threshold"

	FJ_Debug19524 = "fj/debug/19524"

	FJ_CNRecvErr        = "fj/cn/recv/err"
	FJ_CNSubSysErr      = "fj/cn/recv/subsyserr"
	FJ_CNReplayCacheErr = "fj/cn/recv/rcacheerr"
	FJ_CNGCDumpTable    = "fj/cn/gc/dumptable"

	FJ_LogReader    = "fj/log/reader"
	FJ_LogWorkspace = "fj/log/workspace"

	FJ_CronJobsOpen = "fj/cronjobs/open"
	FJ_CDCRecordTxn = "fj/cdc/recordtxn"

	FJ_CDCExecutor  = "fj/cdc/executor"
	FJ_CDCScanTable = "fj/cdc/scantable"

	FJ_WALReplayFailed = "fj/wal/replay/failed"

	FJ_CDCHandleSlow             = "fj/cdc/handleslow"
	FJ_CDCHandleErr              = "fj/cdc/handleerr"
	FJ_CDCScanTableErr           = "fj/cdc/scantableerr"
	FJ_CDCAddExecErr             = "fj/cdc/addexecerr"
	FJ_CDCAddExecConsumeTruncate = "fj/cdc/addexecconsumetruncate"

	FJ_CNFlushSmallObjs      = "fj/cn/flush_small_objs"
	FJ_CNSubscribeTableFail  = "fj/cn/subscribe_table_fail"
	FJ_CNWorkspaceForceFlush = "fj/cn/workspace_force_flush"

	FJ_CNCLONEFailed    = "fj/cn/clone_fails"
	FJ_CNNeedRetryError = "fj/cn/need_retry_error"
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

func LogCNFlushSmallObjsInjected(args ...string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_CNFlushSmallObjs)
	if !injected {
		return false, 0
	}

	ok, level := checkLoggingArgs(int(iarg), sarg, args...)
	return ok, level
}

func CNWorkspaceForceFlushInjected() bool {
	_, _, injected := fault.TriggerFault(FJ_CNWorkspaceForceFlush)
	return injected
}

func LogCNNeedRetryErrorInjected(args ...string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_CNNeedRetryError)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(int(iarg), sarg, args...)
}

func LogCNCloneFailedInjected(args ...string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_CNCLONEFailed)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(int(iarg), sarg, args...)
}

func LogCNSubscribeTableFailInjected(args ...string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_CNSubscribeTableFail)
	if !injected {
		return false, 0
	}

	ok, level := checkLoggingArgs(int(iarg), sarg, args...)
	return ok, level
}

func InjectLogPartitionState(
	databaseName string,
	tableName string,
	level int,
) (rmFault func(), err error) {
	return InjectLogging(
		FJ_TracePartitionState,
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

func SimpleInject(key string) (rmFault func(), err error) {
	if err = fault.AddFaultPoint(
		context.Background(),
		key,
		":::",
		"echo",
		0,
		"",
		false,
	); err != nil {
		return
	}
	rmFault = func() {
		fault.RemoveFaultPoint(context.Background(), key)
	}
	return
}

func SimpleInjected(key string) bool {
	_, _, injected := fault.TriggerFault(key)
	return injected
}

// inject log reader and partition state
// `name` is the table name
func InjectLog1(
	tableName string,
	level int,
) (rmFault func(), err error) {
	iarg := int64(MakeInjectTableLoggingIntArg(level, true))
	rmFault = func() {}
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_LogReader,
		":::",
		"echo",
		iarg,
		tableName,
		false,
	); err != nil {
		return
	}
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_TracePartitionState,
		":::",
		"echo",
		iarg,
		tableName,
		false,
	); err != nil {
		fault.RemoveFaultPoint(context.Background(), FJ_LogReader)
		return
	}

	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_LogWorkspace,
		":::",
		"echo",
		iarg,
		tableName,
		false,
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

func CheckpointSaveInjected() (string, bool) {
	_, sarg, injected := fault.TriggerFault(FJ_CheckpointSave)
	return sarg, injected
}

func PrintFlushEntryInjected() (string, bool) {
	_, sarg, injected := fault.TriggerFault(FJ_FlushEntry)
	return sarg, injected
}

func CommitWaitInjected() (string, bool) {
	_, sarg, injected := fault.TriggerFault(FJ_CommitWait)
	return sarg, injected
}

func GCDumpTableInjected() (string, bool) {
	_, sarg, injected := fault.TriggerFault(FJ_CNGCDumpTable)
	return sarg, injected
}

func WaitInjected(key string) {
	fault.TriggerFault(key)
}

func NotifyInjected(key string) {
	fault.TriggerFault(key)
}

func ISCPExecutorInjected() (string, bool) {
	_, sarg, injected := fault.TriggerFault(FJ_CDCExecutor)
	return sarg, injected
}

func CDCScanTableInjected() (string, bool) {
	_, sarg, injected := fault.TriggerFault(FJ_CDCScanTable)
	return sarg, injected
}

func InjectWait(key string) (rmFault func(), err error) {
	if err = fault.AddFaultPoint(
		context.Background(),
		key,
		":::",
		"wait",
		0,
		"",
		false,
	); err != nil {
		return
	}
	rmFault = func() {
		fault.RemoveFaultPoint(context.Background(), key)
	}
	return
}

func InjectNotify(key, target string) (rmFault func(), err error) {
	if err = fault.AddFaultPoint(
		context.Background(),
		key,
		":::",
		"notify",
		0,
		target,
		false,
	); err != nil {
		return
	}
	rmFault = func() {
		fault.RemoveFaultPoint(context.Background(), key)
	}
	return
}

func InjectCheckpointSave(msg string) (rmFault func() (bool, error), err error) {
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_CheckpointSave,
		":::",
		"echo",
		0,
		msg,
		false,
	); err != nil {
		return
	}
	rmFault = func() (ok bool, err error) {
		return fault.RemoveFaultPoint(
			context.Background(), FJ_CheckpointSave,
		)
	}
	return
}

func InjectPrintFlushEntry(msg string) (rmFault func() (bool, error), err error) {
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_FlushEntry,
		":::",
		"echo",
		0,
		msg,
		false,
	); err != nil {
		return
	}
	rmFault = func() (ok bool, err error) {
		return fault.RemoveFaultPoint(context.Background(), FJ_FlushEntry)
	}
	return
}
func InjectCommitWait(msg string) (rmFault func() (bool, error), err error) {
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_CommitWait,
		":::",
		"echo",
		0,
		msg,
		false,
	); err != nil {
		return
	}
	rmFault = func() (ok bool, err error) {
		return fault.RemoveFaultPoint(context.Background(), FJ_CommitWait)
	}
	return
}

func InjectGCDumpTable(msg string) (rmFault func() (bool, error), err error) {
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_CNGCDumpTable,
		":::",
		"echo",
		0,
		msg,
		false,
	); err != nil {
		return
	}
	rmFault = func() (ok bool, err error) {
		return fault.RemoveFaultPoint(context.Background(), FJ_CNGCDumpTable)
	}
	return
}

func InjectCDCExecutor(msg string) (rmFault func() (bool, error), err error) {
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_CDCExecutor,
		":::",
		"echo",
		0,
		msg,
		false,
	); err != nil {
		return
	}
	rmFault = func() (ok bool, err error) {
		return fault.RemoveFaultPoint(context.Background(), FJ_CDCExecutor)
	}
	return
}

func InjectCDCScanTable(msg string) (rmFault func() (bool, error), err error) {
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_CDCScanTable,
		":::",
		"echo",
		0,
		msg,
		false,
	); err != nil {
		return
	}
	rmFault = func() (ok bool, err error) {
		return fault.RemoveFaultPoint(context.Background(), FJ_CDCScanTable)
	}
	return
}

func Debug19524Injected() bool {
	_, _, injected := fault.TriggerFault(FJ_Debug19524)
	return injected
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
	_, sarg, injected := fault.TriggerFault(FJ_TraceRanges)
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
	tableName string,
) (rmFault func(), err error) {
	rmFault = func() {}
	if err = fault.AddFaultPoint(
		ctx,
		FJ_TraceRanges,
		":::",
		"echo",
		int64(MakeInjectTableLoggingIntArg(0, true)),
		tableName,
		false,
	); err != nil {
		return
	}
	rmFault = func() {
		fault.RemoveFaultPoint(ctx, FJ_TraceRanges)
	}
	return
}

func PartitionStateInjected(dbName, tableName string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_TracePartitionState)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(int(iarg), sarg, dbName, tableName)
}

func InjectCDCRecordTxn(
	databaseName string,
	tableName string,
	level int,
) (rmFault func(), err error) {
	return InjectLogging(
		FJ_CDCRecordTxn,
		databaseName,
		tableName,
		level,
		false,
	)
}

func CDCRecordTxnInjected(dbName, tableName string) (bool, int) {
	// for debug
	if strings.Contains(tableName, "bmsql") {
		return true, 0
	}

	iarg, sarg, injected := fault.TriggerFault(FJ_CDCRecordTxn)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(int(iarg), sarg, dbName, tableName)
}

func CDCHandleSlowInjected() (sleepSeconds int64, injected bool) {
	iarg, _, injected := fault.TriggerFault(FJ_CDCHandleSlow)
	return iarg, injected
}

func CDCHandleErrInjected() bool {
	_, _, injected := fault.TriggerFault(FJ_CDCHandleErr)
	return injected
}

func CDCScanTableErrInjected() bool {
	_, _, injected := fault.TriggerFault(FJ_CDCScanTableErr)
	return injected
}

func CDCAddExecErrInjected() bool {
	_, _, injected := fault.TriggerFault(FJ_CDCAddExecErr)
	return injected
}

func CDCAddExecConsumeTruncateInjected() bool {
	_, _, injected := fault.TriggerFault(FJ_CDCAddExecConsumeTruncate)
	return injected
}

func WALReplayFailedExecutorInjected() (string, bool) {
	_, sarg, injected := fault.TriggerFault(FJ_WALReplayFailed)
	return sarg, injected
}

func InjectWALReplayFailed(msg string) (rmFault func() (bool, error), err error) {
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_WALReplayFailed,
		":::",
		"echo",
		0,
		msg,
		false,
	); err != nil {
		return
	}
	rmFault = func() (ok bool, err error) {
		return fault.RemoveFaultPoint(context.Background(), FJ_WALReplayFailed)
	}
	return
}
