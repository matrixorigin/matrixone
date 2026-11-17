// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCollationMetadataRemoteBatchAdmission(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	source := batch.NewWithSize(2)
	defer func() {
		source.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	source.Attrs = []string{"legacy", "disabled"}
	for i, version := range []uint8{0, 1} {
		typ := types.NewWithCharset(types.T_varchar, 8, 0, 3)
		typ.CollationVersion = version
		source.Vecs[i] = vector.NewVec(typ)
		require.NoError(t, vector.AppendBytes(source.Vecs[i], []byte("x"), false, mp))
	}
	source.SetRowCount(1)
	data, err := source.MarshalBinaryForPipeline(new(bytes.Buffer), true, true)
	require.NoError(t, err)
	before := mp.CurrNB()
	decoded, err := decodeBatch(mp, data)
	require.ErrorContains(t, err, "disabled")
	require.Nil(t, decoded)
	require.Equal(t, before, mp.CurrNB(), "rejection must release all decoded columns")
}

func TestUnicodeSetRemoteBatchAdmission(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	}()

	logical := pb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b", Charset: 4, CollationVersion: 1}
	storage := types.MustTypeFromPlan(logical)
	source := batch.NewWithSize(1)
	defer source.Clean(mp)
	source.Vecs[0] = vector.NewVec(storage)
	require.NoError(t, vector.AppendFixed(source.Vecs[0], uint64(3), false, mp))
	source.SetRowCount(1)
	data, err := source.MarshalBinaryForPipeline(new(bytes.Buffer), true, true)
	require.NoError(t, err)
	decoded, err := decodeBatch(mp, data)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	defer decoded.Clean(mp)
	require.Zero(t, decoded.Vecs[0].GetType().Charset)
	require.Zero(t, decoded.Vecs[0].GetType().CollationVersion)
	require.Equal(t, []uint64{3}, vector.MustFixedColWithTypeCheck[uint64](decoded.Vecs[0]))

	// Raw decoding remains fail-closed. Forge the same physical payload as an
	// ordinary versioned uint64; it is valid native metadata but an invalid plan
	// type, and batch admission must reject it before execution.
	wireSource := vector.NewVec(storage)
	defer wireSource.Free(mp)
	require.NoError(t, vector.AppendFixed(wireSource, uint64(3), false, mp))
	wire, err := wireSource.MarshalBinary()
	require.NoError(t, err)
	wire[1+1] = 4 // Type.Charset, after the vector class byte.
	wire[1+3] = 1 // Type.CollationVersion.
	forged := vector.NewVec(types.T_any.ToType())
	require.NoError(t, forged.UnmarshalBinary(wire))
	require.Equal(t, uint8(1), forged.GetType().CollationVersion)
	invalid := batch.NewWithSize(1)
	defer invalid.Clean(mp)
	invalid.Vecs[0] = forged
	invalid.SetRowCount(1)
	data, err = invalid.MarshalBinaryForPipeline(new(bytes.Buffer), true, true)
	require.NoError(t, err)
	decoded, err = decodeBatch(mp, data)
	require.ErrorContains(t, err, "collation revision on a non-string type")
	require.Nil(t, decoded)
}

func TestCollationMetadataExecutionBoundaries(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	typ := pb.Type{Id: int32(types.T_varchar), Charset: 4, CollationVersion: 1}
	table := &pb.TableDef{DefaultCharset: 4, CollationVersion: 1, KeyFormat: 1}
	expr := &pb.Expr{Typ: typ}
	for _, instruction := range []*pipeline.Instruction{
		{ProjectList: []*pb.Expr{expr}},
		{Agg: &pipeline.Group{Types: []pb.Type{typ}}},
		{HashJoin: &pipeline.HashJoin{LeftTypes: []pb.Type{typ}}},
		{Insert: &pipeline.Insert{TableDef: table}},
		{Insert: &pipeline.Insert{TableDef: &pb.TableDef{Indexes: []*pb.IndexDef{{KeyFormat: 1}}}}},
	} {
		wireOwner := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{instruction}}
		require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(nil, wireOwner), "disabled")
		data, err := wireOwner.Marshal()
		require.NoError(t, err)
		_, err = decodeScope(data, c.proc, true, nil)
		require.ErrorContains(t, err, "disabled")
	}
	// Rejection precedes process access, storage side effects, or executor allocation.
	plan := &pb.Plan{Plan: &pb.Plan_Query{Query: &pb.Query{Nodes: []*pb.Node{{ProjectList: []*pb.Expr{expr}}}}}}
	require.ErrorContains(t, (&Compile{}).Compile(t.Context(), plan, nil), "disabled")
	executor, err := colexec.NewExpressionExecutor(nil, expr)
	require.ErrorContains(t, err, "disabled")
	require.Nil(t, executor)
	// A nil process detects construction before the whole list is admitted.
	exprs := []*pb.Expr{{Typ: pb.Type{Id: int32(types.T_int64)}, Expr: &pb.Expr_Col{Col: &pb.ColRef{}}}, expr}
	execs, err := colexec.NewExpressionExecutorsFromPlanExpressions(nil, exprs)
	require.ErrorContains(t, err, "disabled")
	require.Nil(t, execs)
	owner := &colexec.DeferredJoinDiagnostic{}
	execs, err = colexec.NewJoinBuildExpressionExecutors(nil, exprs, nil, owner)
	require.ErrorContains(t, err, "disabled")
	require.Nil(t, execs)
	execs, activation, err := colexec.NewJoinProbeExpressionExecutors(nil, exprs, nil, owner)
	require.ErrorContains(t, err, "disabled")
	require.Nil(t, execs)
	require.Nil(t, activation)
	literalVector, err := colexec.GenerateConstListExpressionExecutor(nil, exprs)
	require.ErrorContains(t, err, "disabled")
	require.Nil(t, literalVector)
	// A legacy outer Type must not hide disabled metadata inside opaque bytes.
	payload := vector.NewVec(types.MustTypeFromPlan(typ))
	data, err := payload.MarshalBinary()
	require.NoError(t, err)
	payload.Free(c.proc.Mp())
	executor, err = colexec.NewExpressionExecutor(c.proc, &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_varchar), Charset: 3},
		Expr: &pb.Expr_Vec{Vec: &pb.LiteralVec{Data: data}},
	})
	require.ErrorContains(t, err, "disabled")
	require.Nil(t, executor)
	defs, extra, err := engine.PlanDefsToExeDefs(table)
	require.ErrorContains(t, err, "disabled")
	require.Nil(t, defs)
	require.Nil(t, extra)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, &pipeline.Pipeline{
		InstructionList: []*pipeline.Instruction{{Agg: &pipeline.Group{Types: []pb.Type{{Id: int32(types.T_varchar), Charset: 3}}}}},
	}))
	// The transport codec can retain a known domain without admitting it.
	runtime := types.MustTypeFromPlan(typ)
	require.Equal(t, []types.Type{runtime}, convertToTypes(convertToPlanTypes([]types.Type{runtime})))
}
