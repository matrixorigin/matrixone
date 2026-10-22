// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package hashjoin

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
)

// InputEnvelope is the fixed allocation table for one skew build/probe slot:
// decoded frame + complete compact replacement + hash resize/Sels/two iterator
// peaks + reader lookahead + sidecar page. One output slot is charged separately.
func (hashJoin *HashJoin) InputEnvelope(unit int64) (int64, bool) {
	if unit <= 0 || len(hashJoin.EqConds) != 2 || len(hashJoin.EqConds[0]) == 0 || len(hashJoin.EqConds[1]) == 0 {
		return 0, false
	}
	envelope := int64(0)
	for side, schema := range [][]types.Type{hashJoin.LeftTypes, hashJoin.RightTypes} {
		rowWidth := int64(0)
		for _, typ := range schema {
			var ok bool
			rowWidth, ok = checkedEnvelopeAdd(rowWidth, int64(typ.TypeSize()))
			if !ok {
				return 0, false
			}
		}
		rows := int(min(int64(8192), max(int64(1), unit/max(rowWidth, 1))))
		keys, ok := hashbuild.SpillKeyMemoryBound(hashJoin.EqConds[side], unit, rows)
		if !ok {
			return 0, false
		}
		hash, ok := hashmap.BuildMemoryBound(uint64(rows), keys.StringKeys, keys.EncodedUnit, true)
		if !ok {
			return 0, false
		}
		extra, ok := checkedEnvelopeAdd(hash, keys.ExpressionBytes)
		if !ok {
			return 0, false
		}
		extra, ok = checkedEnvelopeAdd(extra, spillIOBufferSize+4096)
		if !ok || unit > (math.MaxInt64-extra)/2 {
			return 0, false
		}
		envelope = max(envelope, unit*2+extra)
	}
	if hashJoin.NonEqCond != nil {
		predicate, ok := hashbuild.SpillKeyMemoryBound([]*plan.Expr{hashJoin.NonEqCond}, unit, 1)
		if !ok {
			return 0, false
		}
		// Non-equality evaluation materializes one complete row from each side in
		// joinBats, in addition to the decoded frame and compact build chunk.
		extra, ok := checkedEnvelopeAdd(unit, unit)
		if !ok {
			return 0, false
		}
		extra, ok = checkedEnvelopeAdd(extra, predicate.ExpressionBytes)
		if !ok {
			return 0, false
		}
		envelope, ok = checkedEnvelopeAdd(envelope, extra)
		if !ok {
			return 0, false
		}
	}
	return envelope, true
}
func checkedEnvelopeAdd(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}

func acquireSpillWorkspace(proc *process.Process) (*rscthrottler.WorkspaceLease, error) {
	if proc.Base.ImportMemory == nil {
		return nil, nil
	}
	return proc.Base.ImportMemory.AcquireIO(proc.Ctx)
}

func releaseSpillWorkspace(lease *rscthrottler.WorkspaceLease) {
	if lease != nil {
		lease.Release()
	}
}

// SealSpillScope binds the real CN-local board after decoding and freezes only
// this scope's actual copies. Other RPC scopes seal their own IDs independently.
func (hashJoin *HashJoin) SealSpillScope(proc *process.Process, ids []int32) error {
	if hashJoin.IsShuffle || len(hashJoin.SpillExpectedScopes) == 0 {
		return nil
	}
	group, err := proc.GetMessageBoard().BroadcastSpillGroup(hashJoin.JoinMapTag, hashJoin.SpillExpectedScopes)
	if err != nil {
		return err
	}
	hashJoin.SpillGroup, hashJoin.SpillEpoch = group, group.Epoch()
	hashJoin.SpillParticipantID = uint64(hashJoin.SpillScopeID)<<32 | uint64(uint32(hashJoin.ParallelID))
	return group.SealScope(hashJoin.SpillEpoch, hashJoin.SpillScopeID, ids)
}

// ArmSpillParticipant is called only for actual execution graphs, before any
// reader, scheduler, lease or expression initialization can fail. Templates may
// seal topology but never acquire this claim. Prepare also arms direct users.
func (hashJoin *HashJoin) ArmSpillParticipant(proc *process.Process) error {
	if hashJoin.IsShuffle || len(hashJoin.SpillExpectedScopes) == 0 {
		return nil
	}
	state := hashJoin.ctr.spillParticipant
	if state != spillParticipantNone && state != spillParticipantRetired {
		return nil
	}
	// Retired handles belong to the previous execution. Establish this actual's
	// responsibility before binding/sealing can fail, without retaining authority
	// over the previous epoch on a partial initialization failure.
	hashJoin.SpillGroup, hashJoin.SpillEpoch = nil, 0
	hashJoin.ctr.spillParticipant = spillParticipantExpected
	return hashJoin.SealSpillScope(proc, []int32{hashJoin.ParallelID})
}
