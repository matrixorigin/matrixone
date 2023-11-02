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

package assertx

import "testing"

func TestInEpsilonF64(t *testing.T) {
	type args struct {
		want float64
		got  float64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{

		{
			name: "Test 1",
			args: args{
				want: 2.0,
				got:  2.0 + defaultEpsilon,
			},
			want: false,
		},
		{
			name: "Test 2",
			args: args{
				want: 2.0,
				got:  2.0 + defaultEpsilon/2,
			},
			want: true,
		},
		{
			name: "Test 3",
			args: args{
				want: 2.0,
				got:  2.00000012,
			},
			want: true,
		},
		{
			name: "Test 4",
			args: args{
				want: 2.0,
				got:  2.0000011,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InEpsilonF64(tt.args.want, tt.args.got); got != tt.want {
				t.Errorf("%s InEpsilonF64() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
