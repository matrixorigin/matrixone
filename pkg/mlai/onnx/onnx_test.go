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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func mustModel(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return b
}

// skipIfNoRuntime skips a test when the onnxruntime shared library cannot be
// loaded (e.g. CI without the downloaded lib), so the suite stays green there.
func skipIfNoRuntime(t *testing.T) {
	if err := Available(); err != nil {
		t.Skipf("onnxruntime not available: %v", err)
	}
}

func TestHalfRoundTrip(t *testing.T) {
	// Half conversion must be exact for representable values.
	for _, f := range []float32{0, 1, -1, 0.5, 2, 3, 6, 9, 65504, -65504} {
		h := float32ToFloat16(f)
		got := float16ToFloat32(h)
		require.InDeltaf(t, f, got, 1e-3, "half round-trip for %v", f)
	}
}

func TestParseShape(t *testing.T) {
	s, err := ParseShape([]byte(`{"dim":[1,1,4],"dtype":"float32"}`))
	require.NoError(t, err)
	require.Equal(t, []int64{1, 1, 4}, s.Dim)
	n, _ := s.NumElements()
	require.Equal(t, int64(4), n)

	_, err = ParseShape([]byte(`{"dim":[1],"dtype":"blob"}`))
	require.Error(t, err)
	_, err = ParseShape([]byte(`{"dim":[],"dtype":"int8"}`))
	require.Error(t, err)
}

func TestSumAndDifference(t *testing.T) {
	skipIfNoRuntime(t)
	sess, err := NewSession(mustModel(t, "sum_and_difference.onnx"))
	require.NoError(t, err)
	defer sess.Close()

	in := ParseShapeMust(t, `{"dim":[1,1,4],"dtype":"float32"}`)
	out := ParseShapeMust(t, `{"dim":[1,1,2],"dtype":"float32"}`)
	got, err := sess.Run([]byte(`[0.2,0.3,0.6,0.9]`), in, out)
	require.NoError(t, err)
	t.Logf("sum_and_difference output: %s", got)
	require.Contains(t, string(got), "[[[")
}

func TestNonTensorOutputs(t *testing.T) {
	skipIfNoRuntime(t)
	sess, err := NewSession(mustModel(t, "sklearn_randomforest.onnx"))
	require.NoError(t, err)
	defer sess.Close()

	in := ParseShapeMust(t, `{"dim":[6,4],"dtype":"float32"}`)
	input := `[5.9,3.0,5.1,1.8, 6.8,2.8,4.8,1.4, 6.3,2.3,4.4,1.3, 6.5,3.0,5.5,1.8, 7.7,2.8,6.7,2.0, 5.5,2.5,4.0,1.3]`
	got, err := sess.Run([]byte(input), in, nil) // NULL output_shape
	require.NoError(t, err)
	t.Logf("non_tensor_outputs: %s", got)
	require.NotEmpty(t, got)
}

func TestMnistFloat16(t *testing.T) {
	skipIfNoRuntime(t)
	sess, err := NewSession(mustModel(t, "mnist_float16.onnx"))
	require.NoError(t, err)
	defer sess.Close()

	// 1x1x28x28 float16 input of zeros exercises the float16 in+out path.
	buf := make([]byte, 0, 784*2)
	buf = append(buf, '[')
	for i := range 784 {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, '0')
	}
	buf = append(buf, ']')

	in := ParseShapeMust(t, `{"dim":[1,1,28,28],"dtype":"float16"}`)
	out := ParseShapeMust(t, `{"dim":[1,10],"dtype":"float16"}`)
	got, err := sess.Run(buf, in, out)
	require.NoError(t, err)
	t.Logf("mnist_float16 logits: %s", got)
	require.Contains(t, string(got), "[[")
}

func ParseShapeMust(t *testing.T, s string) *Shape {
	t.Helper()
	sh, err := ParseShape([]byte(s))
	require.NoError(t, err)
	return sh
}
