// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package reverse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReverse(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "English",
			args: []string{"HelloWorld"},
			want: []string{"dlroWolleH"},
		},
		{
			name: "Chinese",
			args: []string{"你好世界"},
			want: []string{"界世好你"},
		},
		{
			name: "Englist + Chinese",
			args: []string{"Hello 世界"},
			want: []string{"界世 olleH"},
		},
		{
			name: "three strings",
			args: []string{"Hello", " ", "世界"},
			want: []string{"olleH", " ", "界世"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := make([]string, len(c.args))
			got := Reverse(c.args, out)
			require.Equal(t, c.want, got)
		})
	}

}
