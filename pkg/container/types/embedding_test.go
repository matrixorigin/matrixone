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
	"reflect"
	"testing"
)

func TestBytesToEmbedding(t *testing.T) {
	type args struct {
		input []byte
	}
	tests := []struct {
		name    string
		args    args
		wantRes []float32
	}{
		{
			name:    "Test1",
			args:    args{input: []byte{0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64}},
			wantRes: []float32{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRes := BytesToEmbedding(tt.args.input); !reflect.DeepEqual(gotRes, tt.wantRes) {
				t.Errorf("BytesToEmbedding() = %v, want %v", gotRes, tt.wantRes)
			}
		})
	}
}

func TestEmbeddingToBytes(t *testing.T) {
	type args struct {
		input []float32
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "Test1",
			args: args{input: []float32{1, 2, 3}},
			want: []byte{0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EmbeddingToBytes(tt.args.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EmbeddingToBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEmbeddingToString(t *testing.T) {
	type args struct {
		input []float32
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test1",
			args: args{input: []float32{1, 2, 3}},
			want: "[1.000000, 2.000000, 3.000000]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EmbeddingToString(tt.args.input); got != tt.want {
				t.Errorf("EmbeddingToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEmbeddingsToString(t *testing.T) {
	type args struct {
		input [][]float32
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test2",
			args: args{input: [][]float32{{1, 2, 3}, {4, 5, 6}}},
			want: "[1.000000, 2.000000, 3.000000] [4.000000, 5.000000, 6.000000]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EmbeddingsToString(tt.args.input); got != tt.want {
				t.Errorf("EmbeddingsToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringToEmbedding(t *testing.T) {
	type args struct {
		input string
	}
	tests := []struct {
		name string
		args args
		want []float32
	}{
		{
			name: "Test3",
			args: args{input: "[1,2,3]"},
			want: []float32{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringToEmbedding(tt.args.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StringToEmbedding() = %v, want %v", got, tt.want)
			}
		})
	}
}
