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

package substrindex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubStringIndex(t *testing.T) {
	cases := []struct {
		name  string
		bytes string
		delim string
		count int64
		want  string
	}{
		{
			name:  "TEST01",
			bytes: "www.mysql.com",
			delim: ".",
			count: 0,
			want:  "",
		},
		{
			name:  "TEST02",
			bytes: "www.mysql.com",
			delim: ".",
			count: 1,
			want:  "www",
		},
		{
			name:  "TEST03",
			bytes: "www.mysql.com",
			delim: ".",
			count: 2,
			want:  "www.mysql",
		},
		{
			name:  "TEST04",
			bytes: "www.mysql.com",
			delim: ".",
			count: 3,
			want:  "www.mysql.com",
		},
		{
			name:  "TEST05",
			bytes: "www.mysql.com",
			delim: ".",
			count: -3,
			want:  "www.mysql.com",
		},
		{
			name:  "TEST06",
			bytes: "www.mysql.com",
			delim: ".",
			count: -2,
			want:  "mysql.com",
		},
		{
			name:  "TEST07",
			bytes: "www.mysql.com",
			delim: ".",
			count: -1,
			want:  "com",
		},
		{
			name:  "TEST08",
			bytes: "xyz",
			delim: "abc",
			count: 223372036854775808,
			want:  "xyz",
		},
		{
			name:  "TEST09",
			bytes: "aaa.bbb.ccc.ddd.eee",
			delim: ".",
			count: 9223372036854775807,
			want:  "aaa.bbb.ccc.ddd.eee",
		},
		{
			name:  "TEST10",
			bytes: "aaa.bbb.ccc.ddd.eee",
			delim: ".",
			count: -9223372036854775808,
			want:  "aaa.bbb.ccc.ddd.eee",
		},
		{
			name:  "TEST11",
			bytes: "aaa.bbb.ccc.ddd.eee",
			delim: ".",
			count: int64(922337203685477580),
			want:  "aaa.bbb.ccc.ddd.eee",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := subStrIndex(c.bytes, c.delim, c.count)
			require.Equal(t, c.want, got)
		})
	}
}
