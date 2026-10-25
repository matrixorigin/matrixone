// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBroadcastSpillTopologyCodec(t *testing.T) {
	join := &hashjoin.HashJoin{EqConds: [][]*plan.Expr{{}, {}}, SpillScopeID: 9, SpillExpectedScopes: []uint32{4, 9}, SpillThreshold: 2}
	_, encoded, err := convertToPipelineInstruction(join, nil, nil, 0)
	require.NoError(t, err)
	wire, err := encoded.Marshal()
	require.NoError(t, err)
	decoded := &pipeline.Instruction{}
	require.NoError(t, decoded.Unmarshal(wire))
	operator, err := convertToVmOperator(decoded, nil, nil)
	require.NoError(t, err)
	roundTrip := operator.(*hashjoin.HashJoin)
	defer roundTrip.Release()
	require.Equal(t, uint32(9), roundTrip.SpillScopeID)
	require.Equal(t, []uint32{4, 9}, roundTrip.SpillExpectedScopes)
	require.Equal(t, int64(2), roundTrip.SpillThreshold)
	for _, topology := range [][]uint32{nil, {4, 9}} {
		build := &hashbuild.HashBuild{NeedHashMap: true, CanSpill: true, JoinMapRefCnt: 2, SpillExpectedScopes: topology, SpillThreshold: 3}
		_, encoded, err := convertToPipelineInstruction(build, nil, nil, 0)
		require.NoError(t, err)
		wire, err := encoded.Marshal()
		require.NoError(t, err)
		decoded := &pipeline.Instruction{}
		require.NoError(t, decoded.Unmarshal(wire))
		operator, err := convertToVmOperator(decoded, nil, nil)
		require.NoError(t, err)
		roundTrip := operator.(*hashbuild.HashBuild)
		require.Equal(t, len(topology) > 0, roundTrip.CanSpill, "old wire cannot enable unsupported broadcast FD consumption")
		require.Equal(t, int64(3), roundTrip.SpillThreshold)
		roundTrip.Release()
	}
	// Shuffle uses its existing single-consumer ownership on an old topology.
	old := &pipeline.Instruction{Op: int32(vm.HashBuild), HashBuild: &pipeline.HashBuild{IsShuffle: true, CanSpill: true}}
	operator, err = convertToVmOperator(old, nil, nil)
	require.NoError(t, err)
	build := operator.(*hashbuild.HashBuild)
	require.True(t, build.CanSpill)
	build.Release()
}

func TestStatementMemoryTokenScopeCodec(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	original := newScope(Remote)
	defer original.release()
	original.Proc = proc
	original.NodeInfo.Mcpu = 1
	original.MemoryToken = "opaque-admitted-generation"
	context := &scopeContext{id: 7, regs: make(map[*process.WaitRegister]int32)}
	context.root = context
	encoded, _, err := generatePipeline(original, context, 1)
	require.NoError(t, err)
	wire, err := encoded.Marshal()
	require.NoError(t, err)
	decoded, err := decodeScope(wire, proc, true, nil)
	require.NoError(t, err)
	defer decoded.release()
	require.Equal(t, original.MemoryToken, decoded.MemoryToken)
}

func TestBroadcastSpillAdmissionGateCanRecoverNextAttempt(t *testing.T) {
	join := &hashjoin.HashJoin{JoinMapTag: 41, SpillScopeID: 1, SpillExpectedScopes: []uint32{1}}
	build := &hashbuild.HashBuild{JoinMapTag: 41, CanSpill: true, SpillExpectedScopes: []uint32{1}}
	compiler := &Compile{addr: "local", scopes: []*Scope{{RootOp: join}, {RootOp: build}}}
	compiler.beginSpillEpochs()
	require.Empty(t, join.SpillExpectedScopes)
	require.Empty(t, build.SpillExpectedScopes)
	require.True(t, build.CanSpill, "legacy compatibility applies to this attempt, not permanent plan eligibility")
	compiler.importMemory = &process.ImportMemory{}
	compiler.beginSpillEpochs()
	require.Equal(t, []uint32{1}, join.SpillExpectedScopes)
	require.Equal(t, join.SpillExpectedScopes, build.SpillExpectedScopes)
}
