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

package ctl

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type serviceType string

var (
	tn serviceType = "DN"
	cn serviceType = "CN"

	PingMethod          = "PING"
	FlushMethod         = "FLUSH"
	TaskMethod          = "TASK"
	UseSnapshotMethod   = "USESNAPSHOT"
	GetSnapshotMethod   = "GETSNAPSHOT"
	CheckpointMethod    = "CHECKPOINT"
	ForceGCMethod       = "FORCEGC"
	InspectMethod       = "INSPECT"
	LabelMethod         = "LABEL"
	SyncCommitMethod    = "SYNCCOMMIT"
	AddFaultPointMethod = "ADDFAULTPOINT"
	BackupMethod        = "BACKUP"
	TraceSpanMethod     = "TRACESPAN"
	CoreDumpMethod      = "COREDUMP"
	PrintLogMethod      = "PRINTLOG"

	GetProtocolVersionMethod = "GETPROTOCOLVERSION"
	SetProtocolVersionMethod = "SETPROTOCOLVERSION"

	RemoveRemoteLockTable = strings.ToUpper("RemoveRemoteLockTable")
	GetLatestBind         = strings.ToUpper("GetLatestBind")
	UnsubscribeTable      = "UNSUBSCRIBE_TABLE"

	HandleTxnTrace = strings.ToUpper("txn-trace")
)

var (
	supportedServiceTypes = map[serviceType]struct{}{
		tn: {},
		cn: {},
	}
)

var (
	// register all supported debug command here
	supportedCmds = map[string]handleFunc{
		PingMethod:          handlePing(),
		FlushMethod:         handleFlush(),
		TaskMethod:          handleTask,
		UseSnapshotMethod:   handleUseSnapshotTS,
		GetSnapshotMethod:   handleGetSnapshotTS,
		CheckpointMethod:    handleCheckpoint(),
		ForceGCMethod:       handleCNGC,
		InspectMethod:       handleInspectTN(),
		LabelMethod:         handleSetLabel,
		SyncCommitMethod:    handleSyncCommit,
		AddFaultPointMethod: handleAddFaultPoint(),
		BackupMethod:        handleBackup(),
		PrintLogMethod:      handlePrintLog(),
		TraceSpanMethod:     handleTraceSpan,
		CoreDumpMethod:      handleCoreDump,

		GetProtocolVersionMethod: handleGetProtocolVersion,
		SetProtocolVersionMethod: handleSetProtocolVersion,
		RemoveRemoteLockTable:    handleRemoveRemoteLockTable,
		GetLatestBind:            handleGetLatestBind,
		UnsubscribeTable:         handleUnsubscribeTable,
		HandleTxnTrace:           handleTxnTrace,
	}
)

type requestSender = func(context.Context, *process.Process, []txn.CNOpRequest) ([]txn.CNOpResponse, error)

type handleFunc func(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error)

// Result ctl result
type Result struct {
	Method string `json:"method"`
	Data   any    `json:"result"`
}
