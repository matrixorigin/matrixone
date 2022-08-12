// Copyright 2022 Matrix Origin
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

package util

import (
	"fmt"
	"testing"
)

func TestCaller(t *testing.T) {
	type args struct {
		depth int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "depth_0", args: args{depth: 0}, want: "stack_test.go:35"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Caller(tt.args.depth); fmt.Sprintf("%v", got) != tt.want {
				t.Errorf("Caller() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCallers(t *testing.T) {
	type args struct {
		depth int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "depth_0", args: args{depth: 0}, want: "\n\tstack_test.go\n\ttesting.go\n\tasm_amd64.s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Callers(tt.args.depth)
			t.Logf("Callers() = %s", got)
			t.Logf("Callers(%%+s) = %+s", got)
			t.Logf("Callers(%%v) = %v", got)
			t.Logf("Callers(%%+v) = %+v", got)
		})
	}
}
