// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package hashjoin

import (
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type oracleJoinRow struct {
	key, value int32
	null       bool
}

func skewOracle(join *HashJoin, left, right []oracleJoinRow, predicate bool) ([]string, bool) {
	result := []string{}
	leftCount := make([]int, len(left))
	rightCount := make([]int, len(right))
	encode := func(l, r *oracleJoinRow) string {
		values := []string{}
		for _, col := range join.ResultCols {
			value := l
			if col.Rel != 0 {
				value = r
			}
			if value == nil {
				values = append(values, "NULL")
			} else {
				values = append(values, fmt.Sprint(value.value))
			}
		}
		return fmt.Sprint(values)
	}
	for i, l := range left {
		for j, r := range right {
			if l.null || r.null || l.key != r.key || predicate && l.value >= r.value {
				continue
			}
			leftCount[i]++
			rightCount[j]++
			if !join.IsSemi() && !join.IsAnti() {
				result = append(result, encode(&l, &r))
			}
		}
	}
	for i, l := range left {
		if join.IsLeftSingle() && leftCount[i] > 1 {
			return nil, true
		}
		if join.IsLeftSemi() && leftCount[i] > 0 || join.EmitUnmatchedProbe() && leftCount[i] == 0 {
			result = append(result, encode(&l, nil))
		}
	}
	for i, r := range right {
		if join.IsRightSingle() && rightCount[i] > 1 {
			return nil, true
		}
		if join.IsRightSemi() && rightCount[i] > 0 || join.EmitUnmatchedBuild() && !join.IsRightSemi() && rightCount[i] == 0 {
			result = append(result, encode(nil, &r))
		}
	}
	sort.Strings(result)
	return result, false
}

func skewTestBatch(proc *process.Process, rows []oracleJoinRow) *batch.Batch {
	keys, values := make([]int32, len(rows)), make([]int32, len(rows))
	nulls := make([]uint64, 0)
	for i, row := range rows {
		keys[i], values[i] = row.key, row.value
		if row.null {
			nulls = append(nulls, uint64(i))
		}
	}
	bat := batch.NewOffHeapWithSize(2)
	bat.Vecs[0] = testutil.MakeInt32Vector(keys, nulls, proc.Mp())
	bat.Vecs[1] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(len(rows))
	return bat
}

func TestSkewBoundedAllJoinSemantics(t *testing.T) {
	modes := []struct {
		name  string
		typ   plan.Node_JoinType
		right bool
	}{
		{"inner", plan.Node_INNER, false}, {"left", plan.Node_LEFT, false}, {"right", plan.Node_RIGHT, true}, {"full", plan.Node_OUTER, true},
		{"left semi", plan.Node_SEMI, false}, {"right semi", plan.Node_SEMI, true}, {"left anti", plan.Node_ANTI, false}, {"right anti", plan.Node_ANTI, true},
		{"left single", plan.Node_SINGLE, false}, {"right single", plan.Node_SINGLE, true},
	}
	for _, mode := range modes {
		for _, predicate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/predicate=%t", mode.name, predicate), func(t *testing.T) {
				proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
				defer proc.Free()
				left := []oracleJoinRow{{1, 20, false}, {2, 21, false}, {3, 22, false}, {0, 23, true}}
				right := []oracleJoinRow{{1, 30, false}, {1, 19, false}, {2, 25, false}, {0, 26, true}}
				if mode.right && mode.typ == plan.Node_SINGLE {
					left[1] = oracleJoinRow{1, 21, false}
				}
				leftBat, rightBat := skewTestBatch(proc, left), skewTestBatch(proc, right)
				defer leftBat.Clean(proc.Mp())
				defer rightBat.Clean(proc.Mp())
				join := &HashJoin{JoinType: mode.typ, IsRightJoin: mode.right, LeftTypes: []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, RightTypes: []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, EqConds: [][]*plan.Expr{{newExpr(0, types.T_int32.ToType())}, {newExpr(0, types.T_int32.ToType())}}, ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 1), colexec.NewResultPos(1, 1)}}
				if mode.typ == plan.Node_SEMI || mode.typ == plan.Node_ANTI {
					rel := int32(0)
					if mode.right {
						rel = 1
					}
					join.ResultCols = []colexec.ResultPos{colexec.NewResultPos(rel, 1)}
				}
				if predicate {
					fn, err := function.GetFunctionByName(proc.Ctx, "<", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()})
					require.NoError(t, err)
					a, b := newExpr(1, types.T_int32.ToType()), newExpr(1, types.T_int32.ToType())
					a.GetCol().RelPos = 0
					b.GetCol().RelPos = 1
					join.NonEqCond = &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "<"}, Args: []*plan.Expr{a, b}}}}
				}
				require.NoError(t, join.Prepare(proc))
				defer join.Free(proc, false, nil)
				join.ctr.outputTarget = 1   // force every row over a distinct Call boundary
				join.ctr.spillThreshold = 2 // one build row per chunk; duplicate keys cross chunks
				fs, err := proc.GetSpillFileService()
				require.NoError(t, err)
				build, err := fs.CreateAndRemoveFile(proc.Ctx, "skew_build")
				require.NoError(t, err)
				defer build.Close()
				_, err = message.WriteJoinSpillFrame(build, rightBat)
				require.NoError(t, err)
				probe, err := fs.CreateAndRemoveFile(proc.Ctx, "skew_probe")
				require.NoError(t, err)
				defer probe.Close()
				_, err = message.WriteJoinSpillFrame(probe, leftBat)
				require.NoError(t, err)
				require.NoError(t, join.startSkew(proc, spillBucket{buildFd: build, probeFd: probe, depth: spillMaxPass}))
				expected, wantError := skewOracle(join, left, right, predicate)
				actual := []string{}
				var callError error
				for calls := 0; calls < 100; calls++ {
					result, done, err := join.callSkew(proc)
					if err != nil {
						callError = err
						break
					}
					if result.Batch != nil {
						require.Equal(t, 1, result.Batch.RowCount())
						values := []string{}
						for _, vec := range result.Batch.Vecs {
							if vec.IsConstNull() || vec.GetNulls().Contains(0) {
								values = append(values, "NULL")
							} else {
								values = append(values, fmt.Sprint(vector.MustFixedColNoTypeCheck[int32](vec)[0]))
							}
						}
						actual = append(actual, fmt.Sprint(values))
					}
					if done || join.ctr.skew == nil {
						break
					}
					require.Less(t, calls, 99, "bounded fallback must terminate")
				}
				if wantError {
					require.ErrorContains(t, callError, "scalar subquery returns more than 1 row")
				} else {
					require.NoError(t, callError)
					sort.Strings(actual)
					require.Equal(t, expected, actual)
				}
			})
		}
	}
}

func TestSkewWideSchemaFitsTinyUnit(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	join := &HashJoin{LeftTypes: []types.Type{types.T_int32.ToType()}, RightTypes: []types.Type{types.T_int32.ToType()}, EqConds: [][]*plan.Expr{{newExpr(0, types.T_int32.ToType())}, {newExpr(0, types.T_int32.ToType())}}, ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0)}}
	right := batch.NewOffHeapWithSize(51)
	defer right.Clean(proc.Mp())
	right.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	for col := 1; col < 51; col++ {
		join.RightTypes = append(join.RightTypes, types.T_varchar.ToType())
		right.Vecs[col] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(right.Vecs[col], []byte("a referenced varlen payload with more than 23 bytes"), false, proc.Mp()))
	}
	right.SetRowCount(1)
	require.NoError(t, join.Prepare(proc))
	defer join.Free(proc, false, nil)
	envelope, ok := join.InputEnvelope(1 << 20)
	require.True(t, ok)
	fs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	build, err := fs.CreateAndRemoveFile(proc.Ctx, "wide_schema_build")
	require.NoError(t, err)
	defer build.Close()
	_, err = message.WriteJoinSpillFrame(build, right)
	require.NoError(t, err)
	require.NoError(t, join.startSkew(proc, spillBucket{buildFd: build, depth: spillMaxPass}))
	join.ctr.skew.inputBytes = envelope
	require.NoError(t, join.loadSkewChunk(proc))
	require.Equal(t, int64(1), join.ctr.rightRowCnt)
	require.Less(t, join.ctr.mp.GetBatches()[0].Vecs[1].Capacity(), 8192, "fixed 8192 row-ID layout does not require 8192-row initial allocation")
	_, ok = (&HashJoin{}).InputEnvelope(1 << 20)
	require.False(t, ok)
}

func TestSkewWidePayloadWithNarrowKeyUsesAdmittedEnvelope(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	join := &HashJoin{LeftTypes: []types.Type{types.T_int32.ToType()}, RightTypes: []types.Type{types.T_int32.ToType(), types.T_varchar.ToType()}, EqConds: [][]*plan.Expr{{newExpr(0, types.T_int32.ToType())}, {newExpr(0, types.T_int32.ToType())}}, ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0)}}
	right := batch.NewOffHeapWithSize(2)
	defer right.Clean(proc.Mp())
	right.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	right.Vecs[1] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(right.Vecs[1], make([]byte, 2<<20), false, proc.Mp()))
	right.SetRowCount(1)
	require.NoError(t, join.Prepare(proc))
	defer join.Free(proc, false, nil)
	envelope, ok := join.InputEnvelope(3 << 20)
	require.True(t, ok)
	fs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	build, err := fs.CreateAndRemoveFile(proc.Ctx, "wide_payload_narrow_key")
	require.NoError(t, err)
	_, err = message.WriteJoinSpillFrame(build, right)
	require.NoError(t, err)
	require.NoError(t, join.startSkew(proc, spillBucket{buildFd: build, depth: spillMaxPass}))
	join.ctr.skew.inputBytes = envelope
	require.NoError(t, join.loadSkewChunk(proc), "non-key payload cannot be charged as eight copies of a string key")
	require.Equal(t, int64(1), join.ctr.rightRowCnt)
}

func TestSkewMatchStatePageReplayAndSaturation(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "match-state")
	require.NoError(t, err)
	state := spillMatchState{file: file, pageID: -1}
	defer state.close()
	for _, id := range []int64{0, 4095, 4096, 8192, 0, 4096, 4096} {
		_, err := state.match(id)
		require.NoError(t, err)
	}
	for id, expected := range map[int64]byte{0: 2, 1: 0, 4095: 1, 4096: 2, 8191: 0, 8192: 1} {
		actual, err := state.count(id)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
	require.Equal(t, 4096, len(state.page), "sidecar retains exactly one fixed page across replay")
}

func TestControlledJoinResetRetiresCapacityBeforeNextPrepare(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := rscthrottler.NewMemThrottler(t.Name(), .9, rscthrottler.WithConstLimit(1<<30))
	working, err := rscthrottler.ReserveWorkingSet(proc.Ctx, budget, rscthrottler.WorkingSetSpec{UnitBytes: 1 << 20, Inputs: []rscthrottler.InputSlotSpec{{Name: "source", Count: 1, Bytes: 1}, {Name: "output", Count: 1, Bytes: 1 << 20}, {Name: "join", Count: 2, Bytes: 8 << 20}}, IOBytes: 8 << 20})
	require.NoError(t, err)
	defer working.Close()
	proc.Base.ImportMemory = &process.ImportMemory{WorkingSet: working, UnitBytes: 1 << 20, JoinInputBytes: 8 << 20, OutputBytes: 1 << 20, IOBytes: 8 << 20}
	source := skewTestBatch(proc, []oracleJoinRow{{1, 2, false}, {3, 4, false}})
	defer source.Clean(proc.Mp())
	fn, err := function.GetFunctionByName(proc.Ctx, "+", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()})
	require.NoError(t, err)
	expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "+"}, Args: []*plan.Expr{newExpr(0, types.T_int32.ToType()), newExpr(1, types.T_int32.ToType())}}}}
	join := &HashJoin{LeftTypes: []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, RightTypes: []types.Type{types.T_int32.ToType()}, EqConds: [][]*plan.Expr{{expr}, {newExpr(0, types.T_int32.ToType())}}, ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0)}}
	defer join.Free(proc, false, nil)
	baseline := proc.Mp().AllocatedBytes()
	for range 2 {
		require.NoError(t, join.Prepare(proc))
		require.NoError(t, join.ctr.evalJoinCondition(source, proc))
		require.Greater(t, proc.Mp().AllocatedBytes(), baseline)
		file, err := os.CreateTemp(t.TempDir(), "reader-lookahead")
		require.NoError(t, err)
		join.ctr.spillBuildReader = &spillBucketReader{}
		join.ctr.spillBuildReader.resetForFd(file)
		join.Reset(proc, false, nil)
		require.Nil(t, join.ctr.spillBuildReader, "four MiB reader capacity cannot survive the I lease")
		require.Nil(t, join.ctr.eqCondExecs)
		require.Nil(t, join.ctr.eqCondVecs)
		require.Equal(t, baseline, proc.Mp().AllocatedBytes(), "cached function result must retire before the next attempt")
		_, err = file.Stat()
		require.Error(t, err)
	}
}

func TestSkewExpressionKeysKeepOriginalSemantics(t *testing.T) {
	for _, name := range []string{"+", "serial", "serial_full"} {
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			left := skewTestBatch(proc, []oracleJoinRow{{1, 11, false}, {2, 12, false}, {0, 13, true}})
			defer left.Clean(proc.Mp())
			right := skewTestBatch(proc, []oracleJoinRow{{1, 21, false}, {3, 23, false}, {0, 24, true}})
			defer right.Clean(proc.Mp())
			args := []*plan.Expr{newExpr(0, types.T_int32.ToType())}
			inputTypes := []types.Type{types.T_int32.ToType()}
			outputType := types.T_varchar
			if name == "+" {
				args = append(args, &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 1}}}})
				inputTypes = append(inputTypes, types.T_int32.ToType())
				outputType = types.T_int32
			}
			fn, err := function.GetFunctionByName(proc.Ctx, name, inputTypes)
			require.NoError(t, err)
			expr := &plan.Expr{Typ: plan.Type{Id: int32(outputType)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: name}, Args: args}}}
			join := &HashJoin{JoinType: plan.Node_LEFT, LeftTypes: []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, RightTypes: []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, EqConds: [][]*plan.Expr{{expr}, {expr}}, ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 1), colexec.NewResultPos(1, 1)}}
			require.NoError(t, join.Prepare(proc))
			defer join.Free(proc, false, nil)
			join.ctr.outputTarget = 1
			join.ctr.spillThreshold = 2
			envelope, ok := join.InputEnvelope(1 << 20)
			require.True(t, ok)
			fs, err := proc.GetSpillFileService()
			require.NoError(t, err)
			build, err := fs.CreateAndRemoveFile(proc.Ctx, "expr_build")
			require.NoError(t, err)
			_, err = message.WriteJoinSpillFrame(build, right)
			require.NoError(t, err)
			probe, err := fs.CreateAndRemoveFile(proc.Ctx, "expr_probe")
			require.NoError(t, err)
			_, err = message.WriteJoinSpillFrame(probe, left)
			require.NoError(t, err)
			require.NoError(t, join.startSkew(proc, spillBucket{buildFd: build, probeFd: probe, depth: spillMaxPass}))
			join.ctr.skew.inputBytes = envelope
			actual := []string{}
			for calls := 0; calls < 100; calls++ {
				result, done, err := join.callSkew(proc)
				require.NoError(t, err)
				if result.Batch != nil {
					row := []string{}
					for _, vec := range result.Batch.Vecs {
						if vec.IsConstNull() || vec.GetNulls().Contains(0) {
							row = append(row, "NULL")
						} else {
							row = append(row, fmt.Sprint(vector.MustFixedColNoTypeCheck[int32](vec)[0]))
						}
					}
					actual = append(actual, fmt.Sprint(row))
				}
				if done || join.ctr.skew == nil {
					break
				}
				require.Less(t, calls, 99)
			}
			expected := []string{"[11 21]", "[12 NULL]", "[13 NULL]"}
			if name == "serial_full" {
				expected[2] = "[13 24]"
			}
			sort.Strings(actual)
			sort.Strings(expected)
			require.Equal(t, expected, actual)
		})
	}
}

func TestLegacyUnknownExpressionAtMaxDepthKeepsExecutionAndCleans(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	source := batch.NewOffHeapWithSize(1)
	source.Vecs[0] = testutil.MakeVarcharVector([]string{"a", "b"}, nil, proc.Mp())
	source.SetRowCount(2)
	defer source.Clean(proc.Mp())
	baseline := proc.Mp().AllocatedBytes()
	fn, err := function.GetFunctionByName(proc.Ctx, "concat", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()})
	require.NoError(t, err)
	literal := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "x"}}}}
	expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "concat"}, Args: []*plan.Expr{newExpr(0, types.T_varchar.ToType()), literal}}}}
	require.False(t, hashbuild.SupportsBoundedSpillKeys([]*plan.Expr{expr}))
	join := &HashJoin{EqConds: [][]*plan.Expr{{expr}, {expr}}}
	defer join.Free(proc, false, nil)
	join.ctr.spillThreshold = 1
	fs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	file, err := fs.CreateAndRemoveFile(proc.Ctx, "legacy_unknown_key")
	require.NoError(t, err)
	_, err = message.WriteJoinSpillFrame(file, source)
	require.NoError(t, err)
	jm, repartitioned, err := join.rebuildHashmapForBucket(proc, spillBucket{buildFd: file, depth: spillMaxPass}, process.NewAnalyzer(0, false, false, "legacy"))
	require.NoError(t, err)
	require.False(t, repartitioned)
	require.NotNil(t, jm, "unknown legacy keys cannot become an admission error at max depth")
	require.Equal(t, int64(2), jm.GetRowCount())
	keys := testutil.MakeVarcharVector([]string{"ax", "bx"}, nil, proc.Mp())
	matches, _ := jm.NewIterator().Find(0, 2, []*vector.Vector{keys})
	require.NotZero(t, matches[0])
	require.NotZero(t, matches[1])
	keys.Free(proc.Mp())
	jm.Free()
	join.Free(proc, false, nil)
	require.Equal(t, baseline, proc.Mp().AllocatedBytes(), "local builder expression results/duplicates must have a terminal owner")
}

func TestLegacyLargeSingletonHasNoNewSkewWorkspaceLimit(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	source := batch.NewOffHeapWithSize(1)
	source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(source.Vecs[0], make([]byte, 9<<20), false, proc.Mp()))
	source.SetRowCount(1)
	defer source.Clean(proc.Mp())
	join := &HashJoin{EqConds: [][]*plan.Expr{{newExpr(0, types.T_varchar.ToType())}, {newExpr(0, types.T_varchar.ToType())}}}
	defer join.Free(proc, false, nil)
	join.ctr.spillThreshold = 1
	fs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	file, err := fs.CreateAndRemoveFile(proc.Ctx, "legacy_large_singleton")
	require.NoError(t, err)
	_, err = message.WriteJoinSpillFrame(file, source)
	require.NoError(t, err)
	jm, repartitioned, err := join.rebuildHashmapForBucket(proc, spillBucket{buildFd: file, depth: spillMaxPass}, process.NewAnalyzer(0, false, false, "legacy"))
	require.NoError(t, err, "an unadmitted legacy query cannot acquire a new 64MiB singleton limit")
	require.False(t, repartitioned)
	require.NotNil(t, jm)
	require.Equal(t, int64(1), jm.GetRowCount())
	require.Nil(t, join.ctr.skew)
	require.Equal(t, source.Vecs[0].GetBytesAt(0), jm.GetBatches()[0].Vecs[0].GetBytesAt(0))
	jm.Free()
}
