// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package hashjoin

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func outputMemoryJoin() *HashJoin {
	return &HashJoin{
		JoinType:   plan.Node_OUTER,
		LeftTypes:  []types.Type{types.T_int32.ToType(), types.T_varchar.ToType()},
		RightTypes: []types.Type{types.T_int32.ToType(), types.T_varchar.ToType()},
		EqConds:    [][]*plan.Expr{{newExpr(0, types.T_int32.ToType())}, {newExpr(0, types.T_int32.ToType())}},
		ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 1), colexec.NewResultPos(1, 1), colexec.NewResultPos(1, 1)},
	}
}

func outputMemoryBatch(t testing.TB, proc *process.Process, rows int, constant bool) *batch.Batch {
	bat := batch.NewOffHeapWithSize(2)
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	var err error
	bat.Vecs[0], err = vector.NewConstFixed(types.T_int32.ToType(), int32(7), rows, proc.Mp())
	require.NoError(t, err)
	payload, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(strings.Repeat("x", 40)), rows, proc.Mp())
	require.NoError(t, err)
	if constant {
		bat.Vecs[1] = payload
	} else {
		defer payload.Free(proc.Mp())
		bat.Vecs[1] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
		require.NoError(t, bat.Vecs[1].UnionBatch(payload, 0, rows, nil, proc.Mp()))
	}
	bat.SetRowCount(rows)
	return bat
}

// The same shared/constant projections reach each output branch. No physical
// vector-size oracle is used: duplicate logical references count independently.
func TestJoinOutputBudgetScope(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		for _, spilled := range []bool{false, true} {
			for _, shuffle := range []bool{false, true} {
				for _, branch := range []string{"matched", "unmatched", "empty", "finalize", "skew-finalize"} {
					t.Run(fmt.Sprintf("controlled=%t/spilled=%t/shuffle=%t/%s", controlled, spilled, shuffle, branch), func(t *testing.T) {
						proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
						t.Cleanup(proc.Free)
						if controlled {
							proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: 1}
						}
						left, right := outputMemoryBatch(t, proc, 3, true), outputMemoryBatch(t, proc, 3, false)
						join := outputMemoryJoin()
						join.IsShuffle = shuffle
						require.NoError(t, join.Prepare(proc))
						t.Cleanup(func() { join.Free(proc, false, nil) })
						require.Equal(t, controlled, join.ctr.outputTarget > 0)
						join.ctr.spillGeneration = spilled
						join.ctr.leftBat, join.ctr.rightBats = left, []*batch.Batch{right}
						join.resetResultBat()
						result := vm.NewCallResult()
						wantBytes := int64(3 * (types.VarlenaSize + 40))
						switch branch {
						case "matched":
							require.NoError(t, join.ctr.appendOneMatch(join, proc, 0, 0, 0))
						case "unmatched":
							require.NoError(t, join.ctr.appendOneNotMatch(join, proc, 0))
							wantBytes = int64(3*types.VarlenaSize + 40)
						case "empty":
							require.NoError(t, join.ctr.emptyProbe(join, proc, &result))
							wantBytes = int64(3*types.VarlenaSize + 40)
							if controlled {
								require.Equal(t, 1, result.Batch.RowCount())
							} else {
								require.Equal(t, 3, result.Batch.RowCount())
								require.Equal(t, 40, len(result.Batch.Vecs[0].GetArea()), "ordinary empty build retains the bulk shared-value copy")
							}
						case "finalize":
							var matched bitmap.Bitmap
							matched.InitWithSize(3)
							matched.AddRange(0, 3)
							join.ctr.rightMatchedIter = matched.Iterator()
							require.NoError(t, join.ctr.finalize(join, proc, &result))
							wantBytes = int64(3*types.VarlenaSize + 80)
							if controlled {
								require.Equal(t, 1, result.Batch.RowCount())
							} else {
								require.Equal(t, 3, result.Batch.RowCount())
							}
						case "skew-finalize":
							require.NoError(t, join.appendSkewBuildRow(proc, 0))
							wantBytes = int64(3*types.VarlenaSize + 80)
						}
						if controlled {
							require.Equal(t, wantBytes, join.ctr.outputBytes)
							require.True(t, join.ctr.outputFull())
						} else {
							require.Zero(t, join.ctr.outputBytes, "ordinary output has no byte accounting consumer, including spilled shuffle")
							require.False(t, join.ctr.outputFull())
						}
					})
				}
			}
		}
	}
}

func TestJoinOutputRequiresPositiveUnitAndResetsMode(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	join := outputMemoryJoin()
	t.Cleanup(func() { join.Free(proc, false, nil) })
	for _, unit := range []int64{0, -1} {
		proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: unit}
		require.ErrorContains(t, join.Prepare(proc), "invalid join output memory unit")
	}
	proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: 10}
	require.NoError(t, join.Prepare(proc))
	require.Equal(t, int64(10), join.ctr.outputTarget)
	join.Reset(proc, false, nil)
	proc.Base.ImportMemory = nil
	require.NoError(t, join.Prepare(proc))
	require.Zero(t, join.ctr.outputTarget)
}

func TestJoinControlledMatchSizing(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: 400}
	left, right := outputMemoryBatch(t, proc, 3, true), outputMemoryBatch(t, proc, 3, false)
	join := outputMemoryJoin()
	require.NoError(t, join.Prepare(proc))
	t.Cleanup(func() { join.Free(proc, false, nil) })
	join.ctr.leftBat, join.ctr.rightBats, join.ctr.sels = left, []*batch.Batch{right}, []int32{0, 1, 2}
	count, bytes := join.ctr.boundedMatchCount(join, 0, 3)
	require.Equal(t, 2, count)
	require.Equal(t, int64(2*3*(types.VarlenaSize+40)), bytes, "logical repeated/constant/shared references each consume their projected bytes")
	join.ResultCols = []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0), colexec.NewResultPos(1, 0)}
	join.ctr.outputRowWidth, _ = join.fixedOutputRowWidth()
	join.ctr.outputBytes, join.ctr.outputTarget = 3, 16
	count, bytes = join.ctr.boundedMatchCount(join, 0, 3)
	require.Equal(t, 1, count)
	require.Equal(t, int64(12), bytes)
}

func BenchmarkJoinEmptyOutput(b *testing.B) {
	for _, controlled := range []bool{false, true} {
		b.Run(fmt.Sprintf("controlled=%t", controlled), func(b *testing.B) {
			proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
			b.Cleanup(proc.Free)
			if controlled {
				proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: 1 << 20}
			}
			input := outputMemoryBatch(b, proc, 128, false)
			join := outputMemoryJoin()
			require.NoError(b, join.Prepare(proc))
			b.Cleanup(func() { join.Free(proc, false, nil) })
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				join.resetResultBat()
				join.ctr.leftBat, join.ctr.lastIdx = input, 0
				result := vm.NewCallResult()
				if err := join.ctr.emptyProbe(join, proc, &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Drive real build/probe/rebuild/skew consumers. Three duplicate keys make
// controlled variable projections stop/resume inside one candidate selection.
func TestJoinOutputProbeResume(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		for _, spilled := range []bool{false, true} {
			for _, shuffle := range []bool{false, true} {
				for _, predicate := range []bool{false, true} {
					t.Run(fmt.Sprintf("controlled=%t/spilled=%t/shuffle=%t/predicate=%t", controlled, spilled, shuffle, predicate), func(t *testing.T) {
						proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
						t.Cleanup(proc.Free)
						proc.SetMessageBoard(message.NewMessageBoard())
						t.Cleanup(func() { proc.GetMessageBoard().Reset() })
						if controlled {
							proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: 200, JoinInputBytes: 8 << 20}
						}
						left, right := outputMemoryBatch(t, proc, 3, true), outputMemoryBatch(t, proc, 3, false)
						join := outputMemoryJoin()
						join.JoinType, join.JoinMapTag, join.IsShuffle = plan.Node_INNER, 901, shuffle
						if predicate {
							fn, err := function.GetFunctionByName(proc.Ctx, "=", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()})
							require.NoError(t, err)
							a, z := newExpr(0, types.T_int32.ToType()), newExpr(0, types.T_int32.ToType())
							a.GetCol().RelPos, z.GetCol().RelPos = 0, 1
							join.NonEqCond = &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "="}, Args: []*plan.Expr{a, z}}}}
						}
						build := &hashbuild.HashBuild{NeedHashMap: true, NeedBatches: true, NeedAllocateSels: true, CanSpill: true, IsShuffle: shuffle,
							Conditions: join.EqConds[1], JoinMapTag: join.JoinMapTag, JoinMapRefCnt: 1, SpillThreshold: 1 << 30,
							RuntimeFilterSpec: &plan.RuntimeFilterSpec{Tag: 902}}
						if spilled {
							build.SpillThreshold, join.SpillThreshold = 2, 2
							if !shuffle {
								join.SpillScopeID, join.SpillExpectedScopes = 1, []uint32{1}
								build.SpillExpectedScopes = []uint32{1}
								require.NoError(t, join.SealSpillScope(proc, []int32{0}))
							}
						}
						resetChildrenWithBatch(join, left)
						resetHashBuildChildrenWithBatch(build, right)
						require.NoError(t, join.Prepare(proc))
						t.Cleanup(func() { join.Free(proc, false, nil) })
						require.NoError(t, build.Prepare(proc))
						t.Cleanup(func() { build.Free(proc, false, nil) })
						_, err := build.Call(proc)
						require.NoError(t, err)
						rows, stopped := 0, false
						for calls := 0; calls < 100; calls++ {
							result, err := join.Call(proc)
							require.NoError(t, err)
							if result.Batch == nil {
								stopped = true
								break
							}
							require.LessOrEqual(t, result.Batch.RowCount(), colexec.DefaultBatchSize)
							logical := int64(0)
							for _, vec := range result.Batch.Vecs {
								logical += vec.WindowSize(0, result.Batch.RowCount())
								for row := range result.Batch.RowCount() {
									require.Equal(t, strings.Repeat("x", 40), string(vec.GetBytesAt(row)))
								}
							}
							if controlled {
								require.LessOrEqual(t, logical, int64(200+3*(types.VarlenaSize+40)))
							} else {
								require.Zero(t, join.ctr.outputBytes)
							}
							rows += result.Batch.RowCount()
						}
						require.True(t, stopped)
						require.Equal(t, 9, rows)
					})
				}
			}
		}
	}
}

func TestJoinEmptyOutputRowLimit(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		t.Run(fmt.Sprint(controlled), func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			t.Cleanup(proc.Free)
			if controlled {
				proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: 1 << 20}
			}
			input := batch.NewOffHeapWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			var err error
			input.Vecs[0], err = vector.NewConstFixed(types.T_int32.ToType(), int32(7), colexec.DefaultBatchSize+1, proc.Mp())
			require.NoError(t, err)
			input.SetRowCount(colexec.DefaultBatchSize + 1)
			join := outputMemoryJoin()
			join.ResultCols = []colexec.ResultPos{colexec.NewResultPos(0, 0)}
			require.NoError(t, join.Prepare(proc))
			t.Cleanup(func() { join.Free(proc, false, nil) })
			join.ctr.leftBat = input
			for _, rows := range []int{colexec.DefaultBatchSize, 1} {
				join.resetResultBat()
				result := vm.NewCallResult()
				require.NoError(t, join.ctr.emptyProbe(join, proc, &result))
				require.Equal(t, rows, result.Batch.RowCount())
				require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[0], rows-1))
			}
			require.Nil(t, join.ctr.leftBat)
		})
	}
}
