// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package hashbuild

import (
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type observedBuildBudget struct {
	rscthrottler.RSCThrottler
	acquire func(int64) bool
	held    int64
	calls   int
	denied  bool
}

func (b *observedBuildBudget) Acquire(n int64) (int64, bool) {
	b.calls++
	if !b.acquire(n) {
		b.denied = true
		return b.held, false
	}
	b.held += n
	return b.held, true
}
func (b *observedBuildBudget) Release(n int64) int64 { b.held -= n; return b.held }
func (b *observedBuildBudget) Available() int64      { return 0 }

func TestBuildReservesBeforeCopyAndHashAllocation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		deny       int
		controlled bool
	}{
		{"copy denied", 1, false},
		{"hash denied", 2, false},
		{"map owns reservation", 0, false},
		{"controlled copy denied", 1, true},
		{"controlled hash denied", 2, true},
		{"controlled map owns reservation", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deny := tc.deny
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			if tc.controlled {
				proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: 1 << 20, JoinInputBytes: 8 << 20}
			}
			proc.SetMessageBoard(message.NewMessageBoard())
			source := batch.NewOffHeapWithSize(1)
			source.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
			source.SetRowCount(2)
			defer source.Clean(proc.Mp())
			build := &HashBuild{NeedHashMap: true, NeedBatches: true, NeedAllocateSels: true, CanSpill: true, SpillExpectedScopes: []uint32{1}, Conditions: []*plan.Expr{newExpr(0, types.T_int32.ToType())}, JoinMapTag: 91, SpillThreshold: 1 << 30}
			build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}))
			require.NoError(t, build.Prepare(proc))
			defer build.Free(proc, false, nil)
			sourceBytes := proc.Mp().AllocatedBytes()
			calls := 0
			budget := &observedBuildBudget{}
			budget.acquire = func(n int64) bool {
				calls++
				require.Positive(t, n)
				require.Nil(t, build.ctr.hashmapBuilder.IntHashMap, "no hash table can exist before its reservation")
				require.Nil(t, build.ctr.hashmapBuilder.StrHashMap)
				if calls == 1 {
					require.Zero(t, build.ctr.hashmapBuilder.Batches.RowCount(), "copy cannot precede Acquire")
					require.Equal(t, sourceBytes, proc.Mp().AllocatedBytes())
				} else {
					require.Equal(t, 2, build.ctr.hashmapBuilder.Batches.RowCount())
					require.Equal(t, build.ctr.hashmapBuilder.Batches.Allocated(), build.ctr.hashmapBuilder.Batches.RetainedBytes())
				}
				return calls != deny
			}
			build.ctr.memoryThrottler = budget
			_, err := build.Call(proc)
			require.NoError(t, err)
			require.Equal(t, []int32{1, 2}, vector.MustFixedColNoTypeCheck[int32](source.Vecs[0]))
			require.Zero(t, build.ctr.memoryReserved)
			build.ctr.hashmapBuilder.Batches.TrackAllocation()
			require.Zero(t, build.ctr.hashmapBuilder.Batches.RetainedBytes(), "spill retirement or JoinMap transfer detaches retained storage")
			if deny != 0 {
				require.True(t, build.SpillGroup.IsSpilled())
				require.Zero(t, budget.held, "re-spill must retire old copied storage and its D reservation")
			} else {
				require.Positive(t, budget.held, "published map owns its transferred D reservation")
				build.Free(proc, false, nil)
				require.Positive(t, budget.held, "producer Free cannot release the consumer's map reservation")
			}
			proc.GetMessageBoard().Reset()
			require.Zero(t, budget.held, "terminal group and queued message must release exactly once")
		})
	}
}

func TestControlledBuildResetRetiresExpressionCapacity(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.Base.ImportMemory = &process.ImportMemory{}
	proc.SetMessageBoard(message.NewMessageBoard())
	source := batch.NewOffHeapWithSize(1)
	source.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	source.SetRowCount(2)
	defer source.Clean(proc.Mp())
	fn, err := function.GetFunctionByName(proc.Ctx, "+", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()})
	require.NoError(t, err)
	expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "+"}, Args: []*plan.Expr{newExpr(0, types.T_int32.ToType()), newExpr(0, types.T_int32.ToType())}}}}
	build := &HashBuild{NeedHashMap: true, Conditions: []*plan.Expr{expr}, JoinMapTag: 91}
	defer build.Free(proc, false, nil)
	baseline := proc.Mp().AllocatedBytes()
	for range 2 {
		require.NoError(t, build.Prepare(proc))
		_, err := build.ctr.hashmapBuilder.executors[0].Eval(proc, []*batch.Batch{source}, nil)
		require.NoError(t, err)
		require.Greater(t, proc.Mp().AllocatedBytes(), baseline)
		build.Reset(proc, false, nil)
		require.Nil(t, build.ctr.hashmapBuilder.executors)
		require.Equal(t, baseline, proc.Mp().AllocatedBytes())
	}
}

func TestLegacyExpressionBuildSpillsOnlyUnderPressure(t *testing.T) {
	for _, mode := range []string{"memory-available", "copy-budget-denied", "threshold-reached"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			proc.SetMessageBoard(message.NewMessageBoard())
			defer proc.GetMessageBoard().Reset()
			require.Nil(t, proc.Base.ImportMemory)
			input := batch.NewOffHeapWithSize(1)
			input.Vecs[0] = testutil.MakeInt64Vector([]int64{-7}, nil, proc.Mp())
			input.SetRowCount(1)
			defer input.Clean(proc.Mp())
			fn, err := function.GetFunctionByName(proc.Ctx, "abs", []types.Type{types.T_int64.ToType()})
			require.NoError(t, err)
			key := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "abs"},
				Args: []*plan.Expr{newExpr(0, types.T_int64.ToType())},
			}}}
			require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{key}), "ordinary supported functions need not have an import capacity contract")
			build := &HashBuild{NeedHashMap: true, NeedBatches: true, IsShuffle: true, CanSpill: true,
				Conditions: []*plan.Expr{key}, JoinMapTag: 702, JoinMapRefCnt: 1,
				SpillThreshold: 1 << 30, RuntimeFilterSpec: &plan.RuntimeFilterSpec{Tag: 701}}
			if mode == "threshold-reached" {
				build.SpillThreshold = 1
			}
			build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
			require.NoError(t, build.Prepare(proc))
			defer build.Free(proc, false, nil)
			budget := &observedBuildBudget{acquire: func(int64) bool { return mode != "copy-budget-denied" }}
			build.ctr.memoryThrottler = budget
			_, err = build.Call(proc)
			require.NoError(t, err)
			jm, err := message.ReceiveJoinMap(702, true, 0, proc.GetMessageBoard(), proc.Ctx)
			require.NoError(t, err)
			require.NotNil(t, jm)
			require.Equal(t, mode != "memory-available", jm.IsSpilled())
			if !jm.IsSpilled() {
				require.Equal(t, int64(1), jm.GetRowCount())
				require.Equal(t, int64(-7), vector.GetFixedAtNoTypeCheck[int64](jm.GetBatches()[0].Vecs[0], 0))
			}
			jm.Free()
			require.Zero(t, budget.held, "consumer cleanup retires the transferred copy reservation once")
		})
	}
}

func TestBuildPayloadSpillPolicy(t *testing.T) {
	const payloadLimit = int64(2 * colexec.DefaultBatchSize * 8)
	for _, tc := range []struct {
		name        string
		rows        []int
		threshold   int64
		controlled  bool
		noBudget    bool
		denyCall    int
		spill       bool
		auto        bool
		unsupported bool
	}{
		{name: "ordinary equal payload", rows: []int{8192, 8192}, threshold: payloadLimit},
		{name: "controlled keeps capacity threshold", rows: []int{8192, 8192}, threshold: payloadLimit, controlled: true, spill: true},
		{name: "no budget keeps capacity threshold", rows: []int{8192, 8192}, threshold: payloadLimit, noBudget: true, spill: true},
		{name: "ordinary payload exceeds threshold", rows: []int{8192, 8192, 1}, threshold: payloadLimit, spill: true},
		{name: "ordinary includes partial tail", rows: []int{8192, 8191}, threshold: 100001, spill: true},
		{name: "ordinary copy budget denied", rows: []int{8192, 8192}, threshold: payloadLimit, denyCall: 1, spill: true},
		{name: "auto admits payload above static share", rows: []int{8192, 8192, 1}, threshold: payloadLimit, auto: true},
		{name: "auto copy budget denied", rows: []int{1}, threshold: payloadLimit, auto: true, denyCall: 1, spill: true},
		{name: "auto map budget denied", rows: []int{1}, threshold: payloadLimit, auto: true, denyCall: 2, spill: true},
		{name: "auto controlled keeps capacity threshold", rows: []int{8192, 8192}, threshold: payloadLimit, auto: true, controlled: true, spill: true},
		{name: "auto no budget keeps capacity threshold", rows: []int{8192, 8192}, threshold: payloadLimit, auto: true, noBudget: true, spill: true},
		{name: "auto unsupported key keeps payload threshold", rows: []int{8192, 8192, 1}, threshold: payloadLimit, auto: true, unsupported: true, spill: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previousRuntime := moruntime.ServiceRuntime("")
			rt := moruntime.DefaultRuntime()
			moruntime.SetupServiceBasedRuntime("", rt)
			t.Cleanup(func() { moruntime.SetupServiceBasedRuntime("", previousRuntime) })
			budget := &observedBuildBudget{}
			budget.acquire = func(int64) bool { return budget.calls != tc.denyCall }
			if !tc.noBudget {
				rt.SetGlobalVariables(moruntime.CNMemoryThrottler, budget)
			}
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			t.Cleanup(proc.Free)
			t.Cleanup(func() { require.Zero(t, mp.CurrNB(), "all source, copy, spill and map storage released") })
			t.Cleanup(func() { require.Zero(t, budget.held, "reservation retired exactly once") })
			proc.SetMessageBoard(message.NewMessageBoard())
			t.Cleanup(func() { proc.GetMessageBoard().Reset() })
			if tc.controlled {
				proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: 1 << 20, JoinInputBytes: 8 << 20}
			}
			inputs := make([]*batch.Batch, 0, len(tc.rows))
			totalRows := 0
			for _, rows := range tc.rows {
				input := batch.NewOffHeapWithSize(1)
				t.Cleanup(func() { input.Clean(mp) })
				values := make([]int64, rows)
				for i := range values {
					values[i] = int64(totalRows + i)
				}
				input.Vecs[0] = testutil.MakeInt64Vector(values, nil, mp)
				input.SetRowCount(rows)
				require.Equal(t, rows*8, input.Size(), "independent fixed-width payload")
				inputs = append(inputs, input)
				totalRows += rows
			}
			key := newExpr(0, types.T_int64.ToType())
			if tc.unsupported {
				fn, err := function.GetFunctionByName(proc.Ctx, "abs", []types.Type{types.T_int64.ToType()})
				require.NoError(t, err)
				key = &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "abs"}, Args: []*plan.Expr{key},
				}}}
			}
			require.Equal(t, !tc.unsupported, SupportsBoundedSpillKeys([]*plan.Expr{key}))
			build := &HashBuild{
				NeedHashMap: true, NeedBatches: true, HashOnPK: true,
				IsShuffle: true, CanSpill: true, Conditions: []*plan.Expr{key},
				JoinMapTag: 711, JoinMapRefCnt: 1, SpillThreshold: tc.threshold,
				RuntimeFilterSpec: &plan.RuntimeFilterSpec{Tag: 712},
			}
			if tc.auto {
				build.SpillThreshold = 0
			}
			build.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
			require.NoError(t, build.Prepare(proc))
			t.Cleanup(func() { build.Free(proc, false, nil) })
			if tc.auto {
				require.Zero(t, build.SpillThreshold, "Prepare preserves the requested auto mode")
				// Scale only the computed byte share; do not change global memory,
				// cache hints or CPU counts to make this boundary inexpensive.
				build.ctr.spillThreshold = tc.threshold
			}
			if tc.noBudget {
				require.Nil(t, build.ctr.memoryThrottler)
			} else {
				require.Same(t, budget, build.ctr.memoryThrottler, "use the real runtime budget lookup")
			}
			_, err := build.Call(proc)
			require.NoError(t, err)
			if tc.denyCall > 0 {
				require.True(t, budget.denied, "the intended copy/map allocation must reach Acquire")
			}
			jm, err := message.ReceiveJoinMap(711, true, 0, proc.GetMessageBoard(), proc.Ctx)
			require.NoError(t, err)
			require.NotNil(t, jm)
			t.Cleanup(jm.Free)
			require.Equal(t, tc.spill, jm.IsSpilled())
			require.Equal(t, int64(totalRows), jm.GetRowCount())
			assertBuildPayloadRows(t, proc, jm, totalRows)
			if !tc.spill {
				require.Equal(t, uint64(totalRows), jm.GetGroupCount(), "all input keys reached the actual hash table")
				require.Positive(t, budget.held, "the consumer owns the admitted allocation")
			}
		})
	}
}

func TestAutoBuildCompetingJoinMapsReleaseAdmission(t *testing.T) {
	previousRuntime := moruntime.ServiceRuntime("")
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	t.Cleanup(func() { moruntime.SetupServiceBasedRuntime("", previousRuntime) })
	limit := int64(1 << 30)
	budget := &observedBuildBudget{}
	budget.acquire = func(bytes int64) bool { return bytes <= limit-budget.held }
	rt.SetGlobalVariables(moruntime.CNMemoryThrottler, budget)
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	t.Cleanup(func() { require.Zero(t, budget.held) })
	proc.SetMessageBoard(message.NewMessageBoard())
	t.Cleanup(func() { proc.GetMessageBoard().Reset() })
	input := batch.NewOffHeapWithSize(1)
	input.Vecs[0] = testutil.MakeInt64Vector([]int64{0, 1}, nil, mp)
	input.SetRowCount(2)
	t.Cleanup(func() { input.Clean(mp) })
	var maps []*message.JoinMap
	t.Cleanup(func() {
		for _, jm := range maps {
			if jm != nil {
				jm.Free()
			}
		}
	})
	buildMap := func() *message.JoinMap {
		t.Helper()
		tag := int32(720 + len(maps)*2)
		build := &HashBuild{NeedHashMap: true, NeedBatches: true, HashOnPK: true,
			IsShuffle: true, CanSpill: true, Conditions: []*plan.Expr{newExpr(0, types.T_int64.ToType())},
			JoinMapTag: tag, JoinMapRefCnt: 1, RuntimeFilterSpec: &plan.RuntimeFilterSpec{Tag: tag + 1}}
		build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
		require.NoError(t, build.Prepare(proc))
		t.Cleanup(func() { build.Free(proc, false, nil) })
		require.Zero(t, build.SpillThreshold)
		require.Same(t, budget, build.ctr.memoryThrottler)
		_, err := build.Call(proc)
		require.NoError(t, err)
		jm, err := message.ReceiveJoinMap(tag, true, 0, proc.GetMessageBoard(), proc.Ctx)
		require.NoError(t, err)
		require.NotNil(t, jm)
		maps = append(maps, jm)
		assertBuildPayloadRows(t, proc, jm, 2)
		build.Free(proc, false, nil)
		return jm
	}
	first := buildMap()
	require.False(t, first.IsSpilled())
	perMap := budget.held
	require.Positive(t, perMap, "the live JoinMap owns both copied input and hashmap reservations")
	limit = 2 * perMap
	second := buildMap()
	require.False(t, second.IsSpilled())
	require.Equal(t, limit, budget.held, "two live consumers exhaust the finite CN budget")
	third := buildMap()
	require.True(t, third.IsSpilled(), "another build must spill while both admitted maps are live")
	require.True(t, budget.denied)
	require.Equal(t, limit, budget.held)
	first.Free()
	maps[0] = nil
	require.Equal(t, perMap, budget.held)
	fourth := buildMap()
	require.False(t, fourth.IsSpilled(), "releasing one consumer admits a later build without restarting CN")
	require.Equal(t, limit, budget.held)
}

// Read actual producer output, including every spill file. A post-copy spill
// must not write the just-copied incoming batch a second time.
func assertBuildPayloadRows(t *testing.T, proc *process.Process, jm *message.JoinMap, rows int) {
	t.Helper()
	seen := make([]bool, rows)
	count := 0
	check := func(bat *batch.Batch) {
		for _, value := range vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]) {
			require.GreaterOrEqual(t, value, int64(0))
			require.Less(t, value, int64(rows))
			require.False(t, seen[value], "input row %d duplicated", value)
			seen[value] = true
			count++
		}
	}
	if !jm.IsSpilled() {
		for _, bat := range jm.GetBatches() {
			check(bat)
		}
	} else {
		for _, file := range jm.SpillBuildFds {
			if file == nil {
				continue
			}
			_, err := file.Seek(0, io.SeekStart)
			require.NoError(t, err)
			for {
				var header [16]byte
				_, err = io.ReadFull(file, header[:])
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				frameRows, bytes := types.DecodeInt64(header[:8]), types.DecodeInt64(header[8:])
				require.Positive(t, frameRows)
				require.Positive(t, bytes)
				func() {
					bat := batch.NewOffHeapWithSize(0)
					defer bat.Clean(proc.Mp())
					reader := &io.LimitedReader{R: file, N: bytes}
					require.NoError(t, bat.UnmarshalFromReader(reader, proc.Mp()))
					require.Zero(t, reader.N)
					require.Equal(t, frameRows, int64(bat.RowCount()))
					check(bat)
				}()
				var marker [8]byte
				_, err = io.ReadFull(file, marker[:])
				require.NoError(t, err)
				require.Equal(t, uint64(spillMagic), types.DecodeUint64(marker[:]))
			}
		}
	}
	require.Equal(t, rows, count, "all input rows occur exactly once")
}

func TestCollectedBatchBytes(t *testing.T) {
	for _, mode := range []string{"tail-full-tail", "cross-8193", "const-fixed", "const-varlen", "varlen-window"} {
		t.Run(mode, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			t.Cleanup(proc.Free)
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			newBatch := func(vec *vector.Vector, rows int) *batch.Batch {
				bat := batch.NewOffHeapWithSize(1)
				bat.Vecs[0] = vec
				bat.SetRowCount(rows)
				t.Cleanup(func() { bat.Clean(mp) })
				return bat
			}
			var inputs []*batch.Batch
			switch mode {
			case "tail-full-tail":
				for _, rows := range []int{3, 8192, 8189} {
					inputs = append(inputs, newBatch(testutil.MakeInt64Vector(make([]int64, rows), nil, mp), rows))
				}
			case "cross-8193":
				inputs = append(inputs, newBatch(testutil.MakeInt64Vector(make([]int64, 8193), nil, mp), 8193))
			case "const-fixed":
				vec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(17), 8193, mp)
				require.NoError(t, err)
				inputs = append(inputs, newBatch(vec, 8193))
			case "const-varlen":
				vec, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(strings.Repeat("v", 32)), 8193, mp)
				require.NoError(t, err)
				inputs = append(inputs, newBatch(vec, 8193))
			case "varlen-window":
				values := make([]string, 40)
				for i := range values {
					values[i] = strings.Repeat("w", 32+i)
				}
				base := newBatch(testutil.MakeVarcharVector(values, nil, mp), len(values))
				window, err := base.Window(3, 33)
				require.NoError(t, err)
				t.Cleanup(func() { window.Clean(mp) })
				inputs = append(inputs, window)
			}
			builder := &HashmapBuilder{}
			t.Cleanup(func() { builder.Batches.Clean(mp) })
			require.Zero(t, builder.CollectedBatchBytes())
			rows := 0
			for _, input := range inputs {
				require.NoError(t, builder.Batches.CopyIntoBatches(input, proc))
				rows += input.RowCount()
				var recount int64
				for _, bat := range builder.Batches.Buf {
					recount += int64(bat.Size())
				}
				fullBytes := builder.Batches.MemSize
				require.Equal(t, recount, builder.CollectedBatchBytes(), "compare with an independent full destination recount")
				require.Equal(t, fullBytes, builder.Batches.MemSize, "read-only payload lookup preserves the full-batch counter")
				require.Equal(t, rows, builder.Batches.RowCount())
			}
		})
	}
}
