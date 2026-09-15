// Copyright 2023 Matrix Origin
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

package types

import (
	"math"
	"testing"
)

func Test_unsafeStringAt(t *testing.T) {
	type args struct {
		str string
		idx int
	}
	tests := []struct {
		name string
		args args
		want rune
	}{
		{
			name: "Test 1",
			args: args{str: "hello", idx: 1},
			want: 'e',
		},
		{
			name: "Test 2",
			args: args{str: "hello", idx: 0},
			want: 'h',
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := unsafeStringAt(tt.args.str, tt.args.idx); got != tt.want {
				t.Errorf("unsafeStringAt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_unsafeStringAt(b *testing.B) {
	dim := 4024
	arr := make([]float32, dim)
	for i := 0; i < dim; i++ {
		arr[i] = math.MaxFloat32
	}
	str := ArrayToString(arr)

	b.Run("stringAt-RuneCast", func(b *testing.B) {
		var idx int
		strLen := len(str)
		b.ResetTimer()
		runes := []rune(str)
		for i := 0; i < b.N; i++ {
			idx = i % strLen // ideally this should not have been included in the micro-benchmark
			_ = runes[idx]
		}
	})

	b.Run("stringAt-Unsafe", func(b *testing.B) {
		b.ResetTimer()
		var idx int
		strLen := len(str)
		for i := 0; i < b.N; i++ {
			idx = i % strLen
			_ = unsafeStringAt(str, idx)
		}
	})

	/*
		Benchmark_unsafeStringAt
		Benchmark_unsafeStringAt/stringAt-RuneCast
		Benchmark_unsafeStringAt/stringAt-RuneCast-10         	1000000000	         0.6082 ns/op
		Benchmark_unsafeStringAt/stringAt-Unsafe
		Benchmark_unsafeStringAt/stringAt-Unsafe-10           	1000000000	         0.3023 ns/op
	*/
}

func BenchmarkStringToArray(b *testing.B) {
	dim := 4024
	arr := make([]float32, dim)
	for i := 0; i < dim; i++ {
		arr[i] = math.MaxFloat32
	}
	str := ArrayToString(arr)

	b.Run("StringToArray-Trim_Split", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := StringToArray[float32](str)
			if err != nil {
				return
			}
		}
	})

	b.Run("StringToArray-ForLoop-RuneCast", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := StringToArrayV2[float32](str)
			if err != nil {
				return
			}
		}
	})

	b.Run("StringToArray-ForLoop-Unsafe", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := StringToArrayV3[float32](str)
			if err != nil {
				return
			}
		}
	})

	/*
		Benchmark 1: (this)
		BenchmarkStringToArray/StringToArray-Trim_Split
		BenchmarkStringToArray/StringToArray-Trim_Split-10         	1000000000	         0.0000663 ns/op
		BenchmarkStringToArray/StringToArray-ForLoop-RuneCast
		BenchmarkStringToArray/StringToArray-ForLoop-RuneCast-10   	    4532	    253686 ns/op
		BenchmarkStringToArray/StringToArray-ForLoop-Unsafe
		BenchmarkStringToArray/StringToArray-ForLoop-Unsafe-10     	    6182	    190168 ns/op

		Benchmark 2:  While inserting via SQL Alchemy
		# mo       macos   insert/second=340.54838430904914 Split 				 (v1)
		# mo       macos   insert/second=312.77797824688696 ForLoop-WithRuneCast (v2)
		# mo       macos   insert/second=320.189254686237	ForLoop-WithUnsafe 	 (v3)
	*/
}

// TestStringToArrayRejectsNonFinite covers #28688: a vector string literal with a NaN or
// ±Inf element must error at the cast boundary (for every float element type) rather than
// being stored, since non-finite components poison IVFFLAT kmeans/assignment. Finite
// literals and integer narrow vectors are unaffected.
func TestStringToArrayRejectsNonFinite(t *testing.T) {
	nonFinite := []string{"[NaN,0,0]", "[nan,0,0]", "[Inf,0,0]", "[+Inf,0,0]", "[-Inf,0,0]", "[0,inf,0]", "[0,0,-inf]"}
	for _, s := range nonFinite {
		if _, err := StringToArray[float32](s); err == nil {
			t.Errorf("float32 %q: expected non-finite error, got nil", s)
		}
		if _, err := StringToArray[float64](s); err == nil {
			t.Errorf("float64 %q: expected non-finite error, got nil", s)
		}
		if _, err := StringToArray[BF16](s); err == nil {
			t.Errorf("bf16 %q: expected non-finite error, got nil", s)
		}
		if _, err := StringToArray[Float16](s); err == nil {
			t.Errorf("f16 %q: expected non-finite error, got nil", s)
		}
	}

	for _, s := range []string{"[1,2,3]", "[0,0,0]", "[-1.5,2.5,3]"} {
		if _, err := StringToArray[float32](s); err != nil {
			t.Errorf("float32 %q: unexpected error %v", s, err)
		}
		if _, err := StringToArray[float64](s); err != nil {
			t.Errorf("float64 %q: unexpected error %v", s, err)
		}
	}

	// Integer narrow vectors cannot be non-finite; a finite literal still parses.
	if _, err := StringToArray[int8]("[1,2,3]"); err != nil {
		t.Errorf("int8 finite: unexpected error %v", err)
	}
	if _, err := StringToArray[uint8]("[1,2,3]"); err != nil {
		t.Errorf("uint8 finite: unexpected error %v", err)
	}
}

// TestStringToArrayRejectsNarrowOverflow covers the #28688 narrow-type overflow case: a finite
// literal that overflows the 16-bit range narrows to ±Inf, which must be rejected (the finite
// check runs on the narrowed value, not the pre-narrowing float). f16 max ≈ 65504; bf16 ≈ f32.
func TestStringToArrayRejectsNarrowOverflow(t *testing.T) {
	if _, err := StringToArray[Float16]("[70000,0,0]"); err == nil {
		t.Errorf("f16 [70000,...]: expected overflow-to-Inf rejection, got nil")
	}
	if _, err := StringToArray[BF16]("[3.4e38,0,0]"); err == nil {
		t.Errorf("bf16 [3.4e38,...]: expected overflow-to-Inf rejection, got nil")
	}
	// in-range narrow values still parse
	if _, err := StringToArray[Float16]("[65000,0,0]"); err != nil {
		t.Errorf("f16 [65000,...]: unexpected error %v", err)
	}
	if _, err := StringToArray[BF16]("[70000,0,0]"); err != nil {
		t.Errorf("bf16 [70000,...]: unexpected error %v", err)
	}
}
