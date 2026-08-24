// Copyright 2021 - 2025 Matrix Origin
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

package onnx

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	ort "github.com/yalue/onnxruntime_go"
)

// TestHalfSpecials covers NaN/Inf/subnormal/overflow/rounding paths of the
// IEEE-754 half conversion in both directions.
func TestHalfSpecials(t *testing.T) {
	// NaN round-trips as NaN.
	require.True(t, math.IsNaN(float64(float16ToFloat32(float32ToFloat16(float32(math.NaN()))))))
	// Infinities keep their sign.
	require.True(t, math.IsInf(float64(float16ToFloat32(float32ToFloat16(float32(math.Inf(1))))), 1))
	require.True(t, math.IsInf(float64(float16ToFloat32(float32ToFloat16(float32(math.Inf(-1))))), -1))
	// Overflow -> Inf.
	require.True(t, math.IsInf(float64(float16ToFloat32(float32ToFloat16(1e30))), 1))
	require.True(t, math.IsInf(float64(float16ToFloat32(float32ToFloat16(-1e30))), -1))
	// Too small -> signed zero.
	require.Equal(t, float32(0), float16ToFloat32(float32ToFloat16(1e-10)))
	require.Equal(t, uint16(0x8000), float32ToFloat16(float32(math.Copysign(1e-10, -1))))
	// Subnormal half values survive the round trip.
	for _, f := range []float32{6.0e-8, 5.0e-6, 6.09e-5} {
		got := float16ToFloat32(float32ToFloat16(f))
		require.InEpsilonf(t, f, got, 0.01, "subnormal %g -> %g", f, got)
	}
	// Smallest subnormal and smallest normal half bit patterns.
	require.InDelta(t, 5.96e-8, float16ToFloat32(0x0001), 1e-9)
	require.InDelta(t, 6.104e-5, float16ToFloat32(0x0400), 1e-8)
	// f16 NaN/Inf bit patterns decode correctly.
	require.True(t, math.IsNaN(float64(float16ToFloat32(0x7e01))))
	require.True(t, math.IsInf(float64(float16ToFloat32(0xfc00)), -1))
	// Mantissa rounding that carries into the exponent (2047.5 -> 2048).
	require.Equal(t, float32(2048), float16ToFloat32(float32ToFloat16(2047.5)))
	// Rounding carry at the very top of the half range -> Inf.
	require.True(t, math.IsInf(float64(float16ToFloat32(float32ToFloat16(65535))), 1))
	// Max finite half value.
	require.Equal(t, float32(65504), float16ToFloat32(float32ToFloat16(65504)))
}

// TestBuildInputTensorAllDtypes runs the input path for every supported dtype.
func TestBuildInputTensorAllDtypes(t *testing.T) {
	skipIfNoRuntime(t)
	cases := []struct {
		dt    DType
		input string
	}{
		{DTInt8, `[-128, 127]`},
		{DTUint8, `[0, 255]`},
		{DTInt16, `[-32768, 32767]`},
		{DTUint16, `[0, 65535]`},
		{DTInt32, `[-2147483648, 2147483647]`},
		{DTUint32, `[0, 4294967295]`},
		{DTInt64, `[-9223372036854775808, 9223372036854775807]`},
		{DTUint64, `[0, 18446744073709551615]`},
		{DTFloat32, `[1.5, -2.5]`},
		{DTFloat64, `[1.5e300, -2.5]`},
		{DTFloat16, `[1.5, -2.5]`},
		{DTBool, `[true, false]`},
	}
	for _, c := range cases {
		v, err := buildInputTensor([]byte(c.input), &Shape{Dim: []int64{2}, Dtype: c.dt})
		require.NoErrorf(t, err, "dtype %s", c.dt)
		require.NoError(t, v.Destroy())
	}

	// Bool error paths.
	_, err := buildInputTensor([]byte(`[1, 0]`), &Shape{Dim: []int64{2}, Dtype: DTBool})
	require.ErrorContains(t, err, "invalid bool input")
	_, err = buildInputTensor([]byte(`[true]`), &Shape{Dim: []int64{2}, Dtype: DTBool})
	require.ErrorContains(t, err, "shape needs")
	// Bad float for a float dtype.
	_, err = buildInputTensor([]byte(`["x"]`), &Shape{Dim: []int64{1}, Dtype: DTFloat32})
	require.Error(t, err)
	// float64 out-of-float-range values are accepted as json but must parse.
	_, err = buildInputTensor([]byte(`[1e999]`), &Shape{Dim: []int64{1}, Dtype: DTFloat64})
	require.Error(t, err)
}

// TestBuildOutputTensorAllDtypes pre-allocates outputs of every dtype.
func TestBuildOutputTensorAllDtypes(t *testing.T) {
	skipIfNoRuntime(t)
	for _, dt := range []DType{
		DTInt8, DTUint8, DTInt16, DTUint16, DTInt32, DTUint32,
		DTInt64, DTUint64, DTFloat16, DTFloat32, DTFloat64, DTBool,
	} {
		v, err := buildOutputTensor(&Shape{Dim: []int64{2, 3}, Dtype: dt})
		require.NoErrorf(t, err, "dtype %s", dt)
		require.NoError(t, v.Destroy())
	}
	_, err := buildOutputTensor(&Shape{Dim: []int64{1}, Dtype: DType("blob")})
	require.ErrorContains(t, err, "unsupported output dtype")
}

// mkTensor builds a small ort tensor for output-conversion tests.
func mkTensor[T ort.TensorData](t *testing.T, data []T) ort.Value {
	t.Helper()
	v, err := ort.NewTensor(ort.NewShape(int64(len(data))), data)
	require.NoError(t, err)
	return v
}

// TestOutputConversions covers anyTensorFlat / flatData / tensorToNested for
// every element type, plus the mismatch error paths.
func TestOutputConversions(t *testing.T) {
	skipIfNoRuntime(t)

	check := func(v ort.Value, dt DType, want []any) {
		defer v.Destroy()
		got, err := anyTensorFlat(v, newResultBudget())
		require.NoError(t, err)
		require.Equal(t, want, got)
		got, err = flatData(v, dt)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	check(mkTensor(t, []int8{-1, 2}), DTInt8, []any{int64(-1), int64(2)})
	check(mkTensor(t, []uint8{1, 2}), DTUint8, []any{uint64(1), uint64(2)})
	check(mkTensor(t, []int16{-3, 4}), DTInt16, []any{int64(-3), int64(4)})
	check(mkTensor(t, []uint16{3, 4}), DTUint16, []any{uint64(3), uint64(4)})
	check(mkTensor(t, []int32{-5, 6}), DTInt32, []any{int64(-5), int64(6)})
	check(mkTensor(t, []uint32{5, 6}), DTUint32, []any{uint64(5), uint64(6)})
	check(mkTensor(t, []int64{-7, 8}), DTInt64, []any{int64(-7), int64(8)})
	check(mkTensor(t, []uint64{7, 8}), DTUint64, []any{uint64(7), uint64(8)})
	check(mkTensor(t, []bool{true, false}), DTBool, []any{true, false})
	check(mkTensor(t, []float32{1.5, -2.5}), DTFloat32, []any{1.5, -2.5})
	check(mkTensor(t, []float64{1.5, -2.5}), DTFloat64, []any{1.5, -2.5})

	// Type mismatch: float32 tensor declared as int8.
	v := mkTensor(t, []float32{1})
	_, err := flatData(v, DTInt8)
	require.ErrorContains(t, err, "does not match declared dtype")
	_, err = flatData(v, DType("blob"))
	require.ErrorContains(t, err, "unsupported output dtype")
	require.NoError(t, v.Destroy())

	// tensorToNested reshapes and validates the element count.
	v2 := mkTensor(t, []float32{1, 2, 3, 4})
	nested, err := tensorToNested(v2, &Shape{Dim: []int64{2, 2}, Dtype: DTFloat32})
	require.NoError(t, err)
	require.Equal(t, []any{[]any{1.0, 2.0}, []any{3.0, 4.0}}, nested)
	_, err = tensorToNested(v2, &Shape{Dim: []int64{3, 2}, Dtype: DTFloat32})
	require.ErrorContains(t, err, "output_shape needs")
	require.NoError(t, v2.Destroy())

	// valueToJSON on a plain tensor renders the flat array.
	v3 := mkTensor(t, []int32{9})
	j, err := valueToJSON(v3, newResultBudget())
	require.NoError(t, err)
	require.Equal(t, []any{int64(9)}, j)
	require.NoError(t, v3.Destroy())

	// Model-produced outputs on the NULL-output_shape path are element-capped
	// too: they never pass through ParseShape, so an oversized model output
	// must be rejected before conversion.
	big := mkTensor(t, make([]int8, MaxTensorElements+1))
	_, err = anyTensorFlat(big, newResultBudget())
	require.ErrorContains(t, err, "exceeds")
	require.NoError(t, big.Destroy())
}

// TestAggregateResultBudget: the conversion budget is shared across the whole
// invocation result. Tensors each individually below the per-tensor limit must
// still be rejected when their aggregate exceeds it — both as multiple named
// outputs (sequential charges on one budget) and recursively through a
// sequence.
func TestAggregateResultBudget(t *testing.T) {
	skipIfNoRuntime(t)
	const half = MaxTensorElements/2 + 1 // individually legal, two exceed

	// Multiple named outputs share one budget (as in Session.Run).
	b := newResultBudget()
	o1 := mkTensor(t, make([]int8, half))
	defer o1.Destroy()
	o2 := mkTensor(t, make([]int8, half))
	defer o2.Destroy()
	_, err := valueToJSON(o1, b)
	require.NoError(t, err)
	_, err = valueToJSON(o2, b)
	require.ErrorContains(t, err, "total result size limit")

	// A sequence recursively charges the same budget.
	s1 := mkTensor(t, make([]int8, half))
	s2 := mkTensor(t, make([]int8, half))
	seq, err := ort.NewSequence([]ort.Value{s1, s2})
	require.NoError(t, err)
	defer func() {
		seq.Destroy()
		s1.Destroy()
		s2.Destroy()
	}()
	_, err = valueToJSON(seq, newResultBudget())
	require.ErrorContains(t, err, "total result size limit")

	// A long sequence of tiny values is bounded too: each node charges at
	// least one unit.
	tiny := newResultBudget()
	tiny.remaining = 3
	e1 := mkTensor(t, []int8{1})
	e2 := mkTensor(t, []int8{2})
	e3 := mkTensor(t, []int8{3})
	e4 := mkTensor(t, []int8{4})
	seq2, err := ort.NewSequence([]ort.Value{e1, e2, e3, e4})
	require.NoError(t, err)
	defer func() {
		seq2.Destroy()
		e1.Destroy()
		e2.Destroy()
		e3.Destroy()
		e4.Destroy()
	}()
	_, err = valueToJSON(seq2, tiny)
	require.ErrorContains(t, err, "total result size limit")
}

// TestSequenceModelLoopRejected: the Go binding materializes a sequence output
// EAGERLY inside Run (per-child Go/C objects for the full length) before any
// budget check can run, so unbounded runtime-generated sequences must be made
// unreachable at session load: a model with sequence/map outputs that contains
// Loop/Scan operators is rejected. Tensor-only-output models are unaffected
// (their conversion peak is arena-bounded payload).
func TestSequenceModelLoopRejected(t *testing.T) {
	skipIfNoRuntime(t)
	loopMarker := []byte("\x22\x04Loop") // NodeProto op_type "Loop"
	scanMarker := []byte("\x22\x04Scan")

	// The real sklearn sequence-output model is Loop-free and must keep loading.
	sk := mustModel(t, "sklearn_randomforest.onnx")
	s, err := NewSession(sk)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	// The same model with a Loop op marker (appended as an unknown protobuf
	// field, which keeps the model loadable by ORT) must be rejected.
	skLoop := append(append([]byte(nil), sk...), loopMarker...)
	_, err = NewSession(skLoop)
	require.ErrorContains(t, err, "Loop")
	skScan := append(append([]byte(nil), sk...), scanMarker...)
	_, err = NewSession(skScan)
	require.ErrorContains(t, err, "Scan")

	// Static sequence builders can be chained node-by-node, growing a
	// sequence far beyond the input-derived bound with no Loop/Scan — they
	// are rejected for sequence-output models too.
	for _, op := range []string{"SequenceInsert", "SequenceConstruct", "SequenceEmpty"} {
		marker := append([]byte{0x22, byte(len(op))}, op...)
		tainted := append(append([]byte(nil), sk...), marker...)
		_, err = NewSession(tainted)
		require.ErrorContains(t, err, op)
	}

	// A tensor-only-output model containing the marker is NOT subject to the
	// contract: eager conversion of tensor outputs is payload-bounded by the arena.
	sd := mustModel(t, "sum_and_difference.onnx")
	sdLoop := append(append([]byte(nil), sd...), loopMarker...)
	s2, err := NewSession(sdLoop)
	require.NoError(t, err)
	require.NoError(t, s2.Close())
}

// TestSequenceModelLargeBatchRejected: a ZipMap-style sequence output has one
// member per input ROW with no Loop/Scan involved, and the binding materializes
// every member eagerly inside Run — so for sequence/map-output models the input
// element count must be bounded BEFORE Run. A batch over the limit fails fast
// with a clean error instead of reaching the eager-materialization peak.
func TestSequenceModelLargeBatchRejected(t *testing.T) {
	skipIfNoRuntime(t)
	sess, err := NewSession(mustModel(t, "sklearn_randomforest.onnx"))
	require.NoError(t, err)
	defer sess.Close()
	require.True(t, sess.hasSeqOutput)

	// [20000,4] = 80000 elements > MaxSequenceInputElements (64K): rejected before
	// any input parsing or ORT call — the tiny input json is never even validated.
	big := &Shape{Dim: []int64{20000, 4}, Dtype: DTFloat32}
	_, err = sess.Run(context.Background(), []byte(`[1]`), big, nil)
	require.ErrorContains(t, err, "sequence/map outputs")

	// A batch within the limit still works end to end.
	small := &Shape{Dim: []int64{1, 4}, Dtype: DTFloat32}
	out, err := sess.Run(context.Background(), []byte(`[5.9,3.0,5.1,1.8]`), small, nil)
	require.NoError(t, err)
	require.NotNil(t, out)

	// Tensor-only-output models are not subject to the input bound.
	sd, err := NewSession(mustModel(t, "sum_and_difference.onnx"))
	require.NoError(t, err)
	defer sd.Close()
	require.False(t, sd.hasSeqOutput)
}

// TestScalarNormalization covers the float normalization edge cases.
func TestScalarNormalization(t *testing.T) {
	// float32 widened through its shortest decimal repr.
	f, err := float32ToJSONFloat64(float32(1.9999883))
	require.NoError(t, err)
	require.Equal(t, 1.9999883, f)
	_, err = float32ToJSONFloat64(float32(math.NaN()))
	require.ErrorContains(t, err, "non-finite")
	_, err = float32ToJSONFloat64(float32(math.Inf(1)))
	require.ErrorContains(t, err, "non-finite")

	_, err = checkFiniteFloat64(math.Inf(-1))
	require.ErrorContains(t, err, "non-finite")
	f, err = checkFiniteFloat64(2.5)
	require.NoError(t, err)
	require.Equal(t, 2.5, f)

	out, err := f64sToAny([]float64{1.25, -3})
	require.NoError(t, err)
	require.Equal(t, []any{1.25, -3.0}, out)
	_, err = f64sToAny([]float64{math.NaN()})
	require.Error(t, err)
	_, err = f32sToAny([]float32{float32(math.Inf(1))})
	require.Error(t, err)

	require.Equal(t, []any{uint64(1), uint64(2)}, uintsToAny([]uint16{1, 2}))
	require.Equal(t, []any{true}, boolsToAny([]bool{true}))
	require.Equal(t, "42", scalarKey(int64(42)))
	require.Error(t, wrapErr(nonFiniteErr()))
}

// TestResolveLibPath covers the env-override branches.
func TestResolveLibPath(t *testing.T) {
	// Explicit valid path (any existing file will do).
	t.Setenv(EnvLibPath, "onnx.go")
	p, err := resolveLibPath()
	require.NoError(t, err)
	require.Equal(t, "onnx.go", p)

	// Explicit but missing path is an error, not a fallback.
	t.Setenv(EnvLibPath, "/nonexistent/libonnxruntime.so")
	_, err = resolveLibPath()
	require.ErrorContains(t, err, "non-existent")
}

// TestSessionErrors covers NewSession validation.
func TestSessionErrors(t *testing.T) {
	skipIfNoRuntime(t)
	_, err := NewSession(nil)
	require.ErrorContains(t, err, "empty model")
	var s *Session
	require.NoError(t, s.Close()) // nil receiver is a no-op
}
