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
