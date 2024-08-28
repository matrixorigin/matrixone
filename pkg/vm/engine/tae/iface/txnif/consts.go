// Copyright 2021 Matrix Origin
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

package txnif

import "github.com/matrixorigin/matrixone/pkg/container/types"

var UncommitTS types.TS

func init() {
	UncommitTS = types.MaxTs()
}

type TxnState int32

const (
	TxnStateActive TxnState = iota
	TxnStatePreparing
	//TxnStatePrepared only for 2PC
	TxnStatePrepared
	//TxnStateCommittingFinished only for 2PC txn runs on coordinator
	TxnStateCommittingFinished
	TxnStateRollbacking
	//TxnStateCommitted , TxnStateRollbacked, and TxnStateUnknown are final states.
	TxnStateCommitted
	TxnStateRollbacked
	TxnStateUnknown
)

type TxnStatus int32

const (
// TxnStatusActive TxnStatus = iota
// TxnStatusPrepared
// TxnStatusCommittingFinished
// TxnStatusCommitted
// TxnStatusRollbacked
)

type DedupPolicy uint8

func (p DedupPolicy) SkipWorkSpace() bool {
	return p&DedupPolicy_SkipWorkspace != 0
}
func (p DedupPolicy) SkipOldCommit() bool {
	return p&DedupPolicy_SkipOldCommitted != 0
}
func (p DedupPolicy) SkipNewCommit() bool {
	return p&DedupPolicy_SkipNewCommitted != 0
}

const (
	// Do not dedup all uncommitted data and tombstones
	DedupPolicy_SkipWorkspace DedupPolicy = 1 << iota
	// Do not dedup committed data and tombstones before the snapshot ts
	DedupPolicy_SkipOldCommitted
	// Do not dedup committed data and tombstones after the snapshot ts
	DedupPolicy_SkipNewCommitted
)

const (
	// Do dedup all data and tombstones
	DedupPolicy_CheckAll DedupPolicy = 0x00

	// Dedup only uncommitted in-memory data and tombstones. For peristed
	// data and tombstones, skip the deduplication.
	// Skip the workspace data and tombstones internal deduplication
	// Skip the committed data and tombstones after the snapshot ts
	DedupPolicy_CheckIncremental = DedupPolicy_SkipWorkspace | DedupPolicy_SkipOldCommitted

	// Disable deduplication
	DedupPolicy_SkipAll = DedupPolicy_SkipOldCommitted | DedupPolicy_SkipNewCommitted | DedupPolicy_SkipWorkspace
)

func TxnStrState(state TxnState) string {
	switch state {
	case TxnStateActive:
		return "Active"
	case TxnStatePreparing:
		return "Preparing"
	case TxnStatePrepared:
		return "Prepared"
	case TxnStateCommittingFinished:
		return "CommittingFinished"
	case TxnStateRollbacking:
		return "Rollbacking"
	case TxnStateCommitted:
		return "Committed"
	case TxnStateRollbacked:
		return "Rollbacked"
	case TxnStateUnknown:
		return "Unknown"
	}
	panic("state not support")
}

const (
	FreezePhase         = "Phase_Freeze"
	RollbackPhase       = "Phase_Rollback"
	PrePreparePhase     = "Phase_PrePrepare"
	PrepareCommitPhase  = "Phase_PrepareCommit"
	PreApplyCommitPhase = "Phase_PreApplyCommit"
	ApplyCommitPhase    = "Phase_ApplyCommit"
)

const (
	TraceStart = iota
	TracePreparing
	TracePrepareWalWait
	TracePrepareWal
	TracePreapredWait
	TracePrepared
)
