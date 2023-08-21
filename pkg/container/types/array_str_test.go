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
	str := ArrayToString[float32](arr)

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
	str := ArrayToString[float32](arr)

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
