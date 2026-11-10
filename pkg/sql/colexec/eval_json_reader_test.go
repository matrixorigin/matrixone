// Copyright 2026 Matrix Origin
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

package colexec

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestEvalJSONReaderSelectionAndReset(t *testing.T) {
	proc := testutil.NewProcess(t)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_json.ToType())
	defer bat.Clean(proc.Mp())
	for _, text := range []string{`{"keep":1}`, `{"keep":2}`, `{"keep":3}`} {
		doc, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(bat.Vecs[0], doc, false, proc.Mp()))
	}
	bat.SetRowCount(3)
	payload, err := bat.Vecs[0].MarshalBinary()
	require.NoError(t, err)
	encoded := append([]byte(nil), objectio.EncodeIOEntryHeader(&objectio.IOEntryHeader{Type: objectio.IOET_ColData, Version: objectio.IOET_ColumnData_V2})...)
	encoded = append(encoded, payload...)
	cache := fileservice.NewBytes(encoded)
	reader, err := objectio.OpenJSONReader(cache, proc.Mp(), nil)
	cache.Release()
	require.NoError(t, err)
	defer reader.Close()
	column := &plan.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	path := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "$.keep"}}}}
	executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "json_value", column, path))
	require.NoError(t, err)
	defer executor.Free()
	fn := executor.(*FunctionExpressionExecutor)
	for _, mask := range [][]bool{nil, {true, false, true}, {false, false, false}} {
		out, err := fn.EvalWithJSONReader(proc, []*batch.Batch{bat}, mask, reader)
		require.NoError(t, err)
		require.Equal(t, 3, out.Length())
		for i, want := range []string{"1", "2", "3"} {
			if mask != nil && !mask[i] {
				require.True(t, out.IsNull(uint64(i)))
			} else {
				require.Equal(t, want, string(out.GetBytesAt(i)))
			}
		}
		require.Nil(t, fn.jsonReader)
	}
	func() {
		savedType := *bat.Vecs[0].GetType()
		defer bat.Vecs[0].SetType(savedType)
		bat.Vecs[0].SetType(types.T_blob.ToType())
		output, err := fn.EvalWithJSONReader(proc, []*batch.Batch{bat}, nil, reader)
		require.NoError(t, err, "the frozen owner, not mutable source metadata, defines the document")
		require.Equal(t, "1", string(output.GetBytesAt(0)))
		require.Nil(t, fn.parameterResults[0])
	}()
	executor.ResetForNextQuery()
	out, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.Equal(t, "2", string(out.GetBytesAt(1)))
	source, err := NewJSONReaderExpressionExecutor(proc, reader)
	require.NoError(t, err)
	fn.parameterExecutor[0].Free()
	fn.SetParameter(0, source)
	// Default Eval discovers the sealed input source; no explicit Eval override.
	out, err = executor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.Equal(t, "3", string(out.GetBytesAt(2)))
	require.Nil(t, source.materialized, "trusted input must not materialize the document")
	require.Nil(t, fn.parameterResults[0], "a closed JSON input must not need a mutable metadata Vector")
	executor.ResetForNextQuery()
	_, err = executor.Eval(proc, []*batch.Batch{bat}, nil)
	require.Error(t, err, "reset must require a new bound version")
	require.NoError(t, source.Bind(reader))
	out, err = executor.Eval(proc, []*batch.Batch{bat}, []bool{true, false, true})
	require.NoError(t, err)
	require.Equal(t, "1", string(out.GetBytesAt(0)))
	require.True(t, out.IsNull(1))
	require.Equal(t, "3", string(out.GetBytesAt(2)))
	require.Nil(t, source.materialized)

}

func TestProjectionJSONReaderPublicationAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_json.ToType())
	defer bat.Clean(proc.Mp())
	makeDoc := func(text string) []byte {
		value, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		raw, err := value.Marshal()
		require.NoError(t, err)
		return raw
	}
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], makeDoc(`{"keep":1,"other":2}`), false, proc.Mp()))
	bat.SetRowCount(1)
	column := &plan.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	path := func(text string) *plan.Expr {
		return &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: text}}}}
	}
	integer := func(value int64) *plan.Expr {
		return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: value}}}}
	}
	target := &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_T{T: &plan.TargetType{}}}
	projection := &Projection{ProjectList: []*plan.Expr{
		bindTestFunction(t, proc, "json_value", column, path("$.keep")),
		bindTestFunction(t, proc, "json_value", column, path("$.other")),
		bindTestFunction(t, proc, "json_value", column, path("$.absent"), target, integer(3), integer(7), integer(2), integer(0)),
	}}
	require.NoError(t, projection.PrepareProjection(proc))
	defer projection.FreeProjection(proc)
	require.Len(t, projection.jsonInputs, 3)
	// One private column backing is shared by the two bound consumers.
	before := proc.Mp().CurrNB()
	probe, err := vector.NewJSONReaderSnapshot(bat.Vecs[0], proc.Mp())
	require.NoError(t, err)
	backingBytes := proc.Mp().CurrNB() - before
	probe.Close()
	require.NoError(t, projection.bindJSONInputs(proc, bat))
	require.Equal(t, backingBytes, proc.Mp().CurrNB()-before)
	projection.releaseJSONInputs()
	require.Equal(t, before, proc.Mp().CurrNB())
	out, err := projection.EvalProjection(bat, proc)
	require.NoError(t, err)
	require.Equal(t, "1", string(out.Vecs[0].GetBytesAt(0)))
	require.Equal(t, "2", string(out.Vecs[1].GetBytesAt(0)))
	require.Equal(t, int64(7), vector.MustFixedColWithTypeCheck[int64](out.Vecs[2])[0])
	for _, input := range projection.jsonInputs {
		require.Nil(t, input.source.reader)
	}
	require.NoError(t, vector.SetBytesAt(bat.Vecs[0], 0, makeDoc(`{"keep":9,"other":8}`), proc.Mp()))
	out, err = projection.EvalProjection(bat, proc)
	require.NoError(t, err)
	require.Equal(t, "9", string(out.Vecs[0].GetBytesAt(0)))
	require.Equal(t, "8", string(out.Vecs[1].GetBytesAt(0)))
	published, err := vector.NewJSONReaderSnapshot(bat.Vecs[0], proc.Mp())
	require.NoError(t, err)
	defer published.Close()
	before = proc.Mp().CurrNB()
	out, err = projection.EvalProjectionWithJSONReaders(bat, proc, map[int]vector.JSONReader{0: published})
	require.NoError(t, err)
	require.Equal(t, before, proc.Mp().CurrNB(), "published input must not allocate another column backing")
	require.Equal(t, "9", string(out.Vecs[0].GetBytesAt(0)))
	require.Equal(t, 1, published.Length(), "projection borrows rather than consumes the publication")
}

func TestProjectionJSONValuePreservesNullPathShortCircuit(t *testing.T) {
	for _, contract := range []bool{false, true} {
		t.Run(fmt.Sprintf("contract=%v", contract), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_json.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(proc.Mp())
			doc, err := types.ParseStringToByteJson(`{"keep":1}`)
			require.NoError(t, err)
			require.NoError(t, vector.AppendByteJson(bat.Vecs[0], doc, false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], nil, true, proc.Mp()))
			bat.SetRowCount(1)
			column := &plan.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			path := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
			args := []*plan.Expr{column, path}
			if contract {
				integer := func(v int64) *plan.Expr {
					return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: v}}}}
				}
				target := &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_T{T: &plan.TargetType{}}}
				args = append(args, target, integer(1), integer(0), integer(1), integer(0))
			}
			expression := bindTestFunction(t, proc, "json_value", args...)
			ordinary, err := NewExpressionExecutor(proc, expression)
			require.NoError(t, err)
			defer ordinary.Free()
			projection := &Projection{ProjectList: []*plan.Expr{expression}}
			require.NoError(t, projection.PrepareProjection(proc))
			defer projection.FreeProjection(proc)
			bat.Vecs[0].GetBytesAt(0)[0] = 0xfd
			out, err := ordinary.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			require.True(t, out.IsNull(0))
			projected, err := projection.EvalProjection(bat, proc)
			require.NoError(t, err, "publication must preserve NULL-path evaluation order")
			require.True(t, projected.Vecs[0].IsNull(0))
			require.NoError(t, vector.SetBytesAt(bat.Vecs[1], 0, []byte("$.keep"), proc.Mp()))
			bat.Vecs[1].GetNulls().Del(0)
			_, err = projection.EvalProjection(bat, proc)
			require.Error(t, err, "an evaluated malformed document remains a hard error")
			require.NoError(t, vector.SetBytesAt(bat.Vecs[1], 0, []byte("$["), proc.Mp()))
			_, ordinaryErr := ordinary.Eval(proc, []*batch.Batch{bat}, nil)
			_, projectedErr := projection.EvalProjection(bat, proc)
			require.Error(t, ordinaryErr)
			require.EqualError(t, projectedErr, ordinaryErr.Error(), "path error must keep its original precedence")
		})
	}
}

func TestProjectionJSONValueRespectsMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_json.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_bool.ToType())
	defer bat.Clean(proc.Mp())
	doc, err := types.ParseStringToByteJson(`{"keep":1}`)
	require.NoError(t, err)
	for _, selected := range []bool{true, false} {
		require.NoError(t, vector.AppendByteJson(bat.Vecs[0], doc, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], selected, false, proc.Mp()))
	}
	bat.SetRowCount(2)
	document := &plan.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	flag := &plan.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
	literal := func(s string) *plan.Expr {
		return &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: s}}}}
	}
	value := bindTestFunction(t, proc, "json_value", document, literal("$.keep"))
	expression := bindTestFunction(t, proc, "case", flag, value, literal("skipped"))
	projection := &Projection{ProjectList: []*plan.Expr{expression}}
	require.NoError(t, projection.PrepareProjection(proc))
	defer projection.FreeProjection(proc)
	bat.Vecs[0].GetBytesAt(1)[0] = 0xfd
	result, err := projection.EvalProjection(bat, proc)
	require.NoError(t, err)
	require.Equal(t, "1", string(result.Vecs[0].GetBytesAt(0)))
	require.Equal(t, "skipped", string(result.Vecs[0].GetBytesAt(1)))
	require.NoError(t, vector.SetFixedAtWithTypeCheck(bat.Vecs[1], 1, true))
	_, err = projection.EvalProjection(bat, proc)
	require.Error(t, err, "the same document must fail when its row is evaluated")
}

func TestJSONValueConsumesPrivateConstructor(t *testing.T) {
	for _, name := range []string{"json_array", "json_object", "cast"} {
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
			bat := batch.NewWithSize(1)
			sourceType := types.T_int64.ToType()
			if name == "cast" {
				sourceType = types.T_varchar.ToType()
			}
			bat.Vecs[0] = vector.NewVec(sourceType)
			defer bat.Clean(proc.Mp())
			if name == "cast" {
				for _, text := range []string{`{"keep":1}`, `{"keep":2}`, `{"keep":3}`} {
					require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(text), false, proc.Mp()))
				}
			} else {
				require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{1, 2, 3}, nil, proc.Mp()))
			}
			bat.SetRowCount(3)
			column := &plan.Expr{Typ: sourceType.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			args := []*plan.Expr{column}
			pathText := "$[0]"
			if name == "json_object" {
				key := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "keep"}}}}
				args = []*plan.Expr{key, column}
				pathText = "$.keep"
			}
			if name == "cast" {
				args = []*plan.Expr{column, {Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_T{T: &plan.TargetType{}}}}
				pathText = "$.keep"
			}
			constructor := bindTestFunction(t, proc, name, args...)
			path := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: pathText}}}}
			executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "json_value", constructor, path))
			require.NoError(t, err)
			defer executor.Free()
			outer := executor.(*FunctionExpressionExecutor)
			producer := outer.parameterExecutor[0].(*FunctionExpressionExecutor)
			for _, mask := range [][]bool{nil, {true, false, true}, {false, false, false}} {
				output, err := executor.Eval(proc, []*batch.Batch{bat}, mask)
				require.NoError(t, err)
				for row, want := range []string{"1", "2", "3"} {
					if mask != nil && !mask[row] {
						require.True(t, output.IsNull(uint64(row)))
					} else {
						require.Equal(t, want, string(output.GetBytesAt(row)))
					}
				}
				require.Nil(t, producer.resultVector.GetResultVector(), "constructor must not initialize or export a mutable JSON result")
				require.Nil(t, outer.jsonReader)
			}
			executor.ResetForNextQuery()
			if name == "cast" {
				require.NoError(t, vector.SetBytesAt(bat.Vecs[0], 0, []byte(`{"keep":10}`), proc.Mp()))
			} else {
				vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])[0] = 10
			}
			output, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			require.Equal(t, "10", string(output.GetBytesAt(0)))
		})
	}
}

func TestJSONValuePrivateConstantConstructor(t *testing.T) {
	for _, name := range []string{"json_array", "json_object", "cast"} {
		for _, dynamicPath := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/dynamic_path=%v", name, dynamicPath), func(t *testing.T) {
				proc := testutil.NewProc(t)
				defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
				defer bat.Clean(proc.Mp())
				pathText, missing := "$[0]", "$[1]"
				if name != "json_array" {
					pathText, missing = "$.keep", "$.missing"
				}
				for _, path := range []string{pathText, missing, pathText} {
					require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(path), false, proc.Mp()))
				}
				bat.SetRowCount(3)
				literal := func(text string) *plan.Expr {
					return &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: text}}}}
				}
				value := &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 5}}}}
				args := []*plan.Expr{value}
				if name == "json_object" {
					args = []*plan.Expr{literal("keep"), value}
				}
				if name == "cast" {
					args = []*plan.Expr{literal(`{"keep":5}`), {Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_T{T: &plan.TargetType{}}}}
				}
				path := literal(pathText)
				if dynamicPath {
					path = &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
				}
				executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "json_value", bindTestFunction(t, proc, name, args...), path))
				require.NoError(t, err)
				defer executor.Free()
				outer := executor.(*FunctionExpressionExecutor)
				producer := outer.parameterExecutor[0].(*FunctionExpressionExecutor)
				check := func(want string, mask []bool) {
					t.Helper()
					output, err := executor.Eval(proc, []*batch.Batch{bat}, mask)
					require.NoError(t, err)
					require.Equal(t, 3, output.Length())
					for row := range 3 {
						if dynamicPath && (row == 1 || mask != nil && !mask[row]) {
							require.True(t, output.IsNull(uint64(row)))
						} else {
							require.Equal(t, want, string(output.GetBytesAt(row)))
						}
					}
					require.NotNil(t, producer.jsonConstructorConstant)
					require.Equal(t, 1, producer.jsonConstructorConstant.Length())
					require.Nil(t, producer.resultVector.GetResultVector(), "constant admission must not export an ordinary JSON Vector")
					require.Equal(t, !dynamicPath, outer.folded.canFold, "an entirely constant consumer must retain result folding")
				}
				check("5", nil)
				version := producer.jsonConstructorConstant
				check("5", nil)
				require.Same(t, version, producer.jsonConstructorConstant)
				if dynamicPath {
					check("5", []bool{true, false, true})
				}
				executor.ResetForNextQuery()
				require.Nil(t, producer.jsonConstructorConstant)
				if name == "cast" {
					input := producer.parameterExecutor[0].(*FixedVectorExpressionExecutor).resultVector
					raw := input.GetBytesAt(0)
					raw[len(raw)-2] = '9'
				} else {
					input := producer.parameterExecutor[len(args)-1].(*FixedVectorExpressionExecutor).resultVector
					vector.MustFixedColNoTypeCheck[int64](input)[0] = 9
				}
				check("9", nil)
				require.NotSame(t, version, producer.jsonConstructorConstant)
			})
		}
	}
}

func TestJSONValuePrivateBinaryLiteral(t *testing.T) {
	for _, isNull := range []bool{false, true} {
		for _, dynamicPath := range []bool{false, true} {
			t.Run(fmt.Sprintf("null=%v/dynamic_path=%v", isNull, dynamicPath), func(t *testing.T) {
				proc := testutil.NewProc(t)
				defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
				defer bat.Clean(proc.Mp())
				for _, path := range []string{"$.keep", "$.missing", "$.keep"} {
					require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(path), false, proc.Mp()))
				}
				bat.SetRowCount(3)
				document, err := types.ParseStringToByteJson(`{"keep":5}`)
				require.NoError(t, err)
				raw, err := document.Marshal()
				require.NoError(t, err)
				literal := &plan.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: isNull, Value: &plan.Literal_Sval{Sval: string(raw)}}}}
				path := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "$.keep"}}}}
				if dynamicPath {
					path = &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
				}
				executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "json_value", literal, path))
				require.NoError(t, err)
				defer executor.Free()
				outer := executor.(*FunctionExpressionExecutor)
				source := outer.parameterExecutor[0].(*jsonLiteralExpressionExecutor)
				require.NotNil(t, source.reader, "literal must be admitted before executor publication")
				version := source.reader
				check := func() {
					t.Helper()
					output, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
					require.NoError(t, err)
					for row := range 3 {
						if isNull || dynamicPath && row == 1 {
							require.True(t, output.IsNull(uint64(row)))
						} else {
							require.Equal(t, "5", string(output.GetBytesAt(row)))
						}
					}
					require.Nil(t, source.materialized)
					require.Nil(t, outer.parameterResults[0])
					require.Equal(t, !dynamicPath, outer.folded.canFold)
				}
				check()
				writable, err := source.EvalWithoutResultReusing(proc, []*batch.Batch{bat}, nil)
				require.NoError(t, err)
				if !isNull {
					bytes := writable.GetBytesAt(0)
					binary.LittleEndian.PutUint64(bytes[len(bytes)-8:], 9)
				}
				writable.Free(proc.Mp())
				check()
				executor.ResetForNextQuery()
				require.Nil(t, source.reader)
				check()
				require.NotSame(t, version, source.reader)
			})
		}
	}
}
