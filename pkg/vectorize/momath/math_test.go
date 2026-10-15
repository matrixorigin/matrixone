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

package momath

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func TestAbsSignedFloat(t *testing.T) {
	t.Run("float32", func(t *testing.T) {
		testAbsSignedFloat(t, float32(math.SmallestNonzeroFloat32), float32(math.MaxFloat32))
	})
	t.Run("float64", func(t *testing.T) {
		testAbsSignedFloat(t, math.SmallestNonzeroFloat64, math.MaxFloat64)
	})
}

func testAbsSignedFloat[T ~float32 | ~float64](t *testing.T, smallest, largest T) {
	t.Helper()
	for _, tc := range []struct {
		name  string
		input T
	}{
		{"negative_zero", T(math.Copysign(0, -1))},
		{"positive_zero", 0},
		{"negative", -2.5},
		{"positive", 2.5},
		{"negative_subnormal", -smallest},
		{"positive_subnormal", smallest},
		{"negative_max", -largest},
		{"positive_max", largest},
		{"negative_infinity", T(math.Inf(-1))},
		{"positive_infinity", T(math.Inf(1))},
		{"negative_nan", T(math.Copysign(math.NaN(), -1))},
		{"positive_nan", T(math.NaN())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := AbsSigned(tc.input)
			if err != nil {
				t.Fatalf("AbsSigned(%g) returned unexpected error: %v", tc.input, err)
			}
			if math.IsNaN(float64(tc.input)) {
				if math.Float64bits(float64(got)) != math.Float64bits(float64(tc.input)) {
					t.Fatalf("AbsSigned(%g) changed NaN bits", tc.input)
				}
				return
			}
			want := T(math.Abs(float64(tc.input)))
			if got != want || math.Signbit(float64(got)) {
				t.Fatalf("AbsSigned(%g) = %g (signbit %v), want %g with positive sign", tc.input, got, math.Signbit(float64(got)), want)
			}
			if got == 0 && math.Atan2(float64(got), -1) != math.Pi {
				t.Errorf("Atan2(AbsSigned(%g), -1) = %g, want +pi", tc.input, math.Atan2(float64(got), -1))
			}
		})
	}
}

func TestAbsSignedInteger(t *testing.T) {
	t.Run("int8", func(t *testing.T) { testAbsSignedInteger(t, int8(math.MinInt8), int8(math.MaxInt8)) })
	t.Run("int16", func(t *testing.T) { testAbsSignedInteger(t, int16(math.MinInt16), int16(math.MaxInt16)) })
	t.Run("int32", func(t *testing.T) { testAbsSignedInteger(t, int32(math.MinInt32), int32(math.MaxInt32)) })
	t.Run("int64", func(t *testing.T) { testAbsSignedInteger(t, int64(math.MinInt64), int64(math.MaxInt64)) })
	t.Run("int", func(t *testing.T) { testAbsSignedInteger(t, math.MinInt, math.MaxInt) })
}

func testAbsSignedInteger[T ~int | ~int8 | ~int16 | ~int32 | ~int64](t *testing.T, smallest, largest T) {
	t.Helper()
	for _, tc := range []struct {
		input T
		want  T
	}{
		{0, 0}, {-1, 1}, {1, 1}, {-largest, largest}, {largest, largest},
	} {
		got, err := AbsSigned(tc.input)
		if err != nil || got != tc.want {
			t.Errorf("AbsSigned(%d) = %d, %v; want %d, nil", tc.input, got, err, tc.want)
		}
	}
	got, err := AbsSigned(smallest)
	if got != 0 || !moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
		t.Errorf("AbsSigned(%d) = %d, %v; want 0, out of range", smallest, got, err)
	}
}

func TestCotMatchesReciprocalTangent(t *testing.T) {
	for _, input := range []float64{1, 1e-20, -1e-20, 1e-308, -1e-308, 1e100, -1e100, math.MaxFloat64, -math.MaxFloat64} {
		want := 1 / math.Tan(input)
		got, err := Cot(input)
		if err != nil {
			t.Fatalf("Cot(%g) returned unexpected error: %v", input, err)
		}
		if got != want {
			t.Errorf("Cot(%g) = %g, want 1/Tan(%g) = %g", input, got, input, want)
		}
	}
}

func TestCotPreservesOddSymmetry(t *testing.T) {
	for _, input := range []float64{1e-20, 1e-308, 1e100, math.MaxFloat64} {
		positive, err := Cot(input)
		if err != nil {
			t.Fatalf("Cot(%g) returned unexpected error: %v", input, err)
		}
		negative, err := Cot(-input)
		if err != nil {
			t.Fatalf("Cot(%g) returned unexpected error: %v", -input, err)
		}
		if negative != -positive {
			t.Errorf("Cot(%g) = %g, want -Cot(%g) = %g", -input, negative, input, -positive)
		}
	}
}

func TestCotPreservesNonFiniteInputs(t *testing.T) {
	for _, input := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
		got, err := Cot(input)
		if err != nil {
			t.Errorf("Cot(%g) returned unexpected error: %v", input, err)
		}
		if !math.IsNaN(got) {
			t.Errorf("Cot(%g) = %g, want NaN", input, got)
		}
	}
}

func TestCotRejectsZeroAndReciprocalOverflow(t *testing.T) {
	for _, input := range []float64{0, math.Copysign(0, -1)} {
		_, err := Cot(input)
		if !moerr.IsMoErrCode(err, moerr.ErrInvalidArg) {
			t.Errorf("Cot(%g) error = %v, want invalid argument", input, err)
		}
	}

	for _, input := range []float64{math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64} {
		_, err := Cot(input)
		if !moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
			t.Errorf("Cot(%g) error = %v, want out of range", input, err)
		}
	}
}
