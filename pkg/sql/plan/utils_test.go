// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_removeIf(t *testing.T) {
	strs := []string{"abc", "bc", "def"}

	del1 := make(map[string]struct{})
	del1["abc"] = struct{}{}
	res1 := RemoveIf[string](strs, func(t string) bool {
		return Find[string](del1, t)
	})
	assert.Equal(t, []string{"bc", "def"}, res1)

	del2 := make(map[string]struct{})
	for _, str := range strs {
		del2[str] = struct{}{}
	}
	res2 := RemoveIf[string](strs, func(t string) bool {
		return Find[string](del2, t)
	})
	assert.Equal(t, []string{}, res2)

	assert.Equal(t, []string(nil), RemoveIf[string](nil, nil))
}

func Test_handelEscapeCharInWhere(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "t1",
			args: args{
				s: "",
			},
			want: "",
		},
		{
			name: "t2",
			args: args{
				s: "abcd",
			},
			want: "abcd",
		},
		{
			name: "t3",
			args: args{
				s: "abc\\",
			},
			want: "abc\\\\",
		},
		{
			name: "t4",
			args: args{
				s: "abac'",
			},
			want: "abac''",
		},
		{
			name: "t5",
			args: args{
				s: "b'00011011'",
			},
			want: "b''00011011''",
		},
		{
			name: "t6",
			args: args{
				s: "b\\\\a9''",
			},
			want: "b\\\\\\\\a9''''",
		},
		//Following Cases Reference to: https://dev.mysql.com/doc/refman/8.4/en/string-literals.html
		{
			name: "t7",
			args: args{
				s: "\\0", // \0
			},
			want: "\\\\0",
		},
		{
			name: "t8",
			args: args{
				s: "\\'", // \'
			},
			want: "\\\\''",
		},
		{
			name: "t9",
			args: args{
				s: "\\\"", // \"
			},
			want: "\\\\\"",
		},
		{
			name: "t10",
			args: args{
				s: "\\b", // \b
			},
			want: "\\\\b",
		},
		{
			name: "t11",
			args: args{
				s: "\\n", // \n
			},
			want: "\\\\n",
		},
		{
			name: "t12",
			args: args{
				s: "\\r", // \r
			},
			want: "\\\\r",
		},
		{
			name: "t13",
			args: args{
				s: "\\t", // \t
			},
			want: "\\\\t",
		},
		{
			name: "t14",
			args: args{
				s: "\\Z", // \Z
			},
			want: "\\\\Z",
		},
		{
			name: "t15",
			args: args{
				s: "\\\\", // \\
			},
			want: "\\\\\\\\",
		},
		//not in pattern-matching context
		{
			name: "t16",
			args: args{
				s: "\\%", // \%
			},
			want: "\\\\%",
		},
		{
			name: "t17",
			args: args{
				s: "\\_", // \_
			},
			want: "\\\\_",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, HandleEscapeCharInWhereClause(tt.args.s), "HandleEscapeCharInWhereClause(%v)", tt.args.s)
		})
	}
}

func Test_chars(t *testing.T) {
	fmt.Println("\000")
}
