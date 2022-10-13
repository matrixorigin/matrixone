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

package bytejson

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValid(t *testing.T) {
	var kases = []struct {
		path  string
		valid bool
		sz    int
	}{
		{"$", true, 0},
		{"$.", false, 0},
		{"$.*", true, 1},
		{"$.a", true, 1},
		{"$..a.", false, 0},
		{"$.a.b", true, 2},
		{"$.a..b", false, 0},
		{"$[1]", true, 1},
		{"$[1].", false, 1},
		{"$[1].a", true, 2},
		{"$[1]..a", false, 0},
		{"$[1].a.b", true, 3},
		{"$[1].a..b", false, 0},
		{"$[-1]", false, 0},
		{"$[1][2]", true, 2},
		{"$[1][2].", false, 2},
		{"$[1][2].a", true, 3},
		{"$[1][2]..a", false, 0},
		{"$[*]", true, 1},
		{"$[*].", false, 1},
		{"$[*].a", true, 2},
		{`$[*]."a"`, true, 2},
		{"$[*]..a", false, 0},
		{"$[*].a.b", true, 3},
		{"$[*].a..b", false, 0},
		{"$[1][*]", true, 2},
		{"$[1][*].", false, 2},
		{"cscdwg", false, 0},
		{"$.**", false, 0},
		{"$**", false, 0},
		{"$**.a", true, 2},
		{"$**.a[1]", true, 3},
		{"$*1", false, 0},
		{"$*a", false, 0},
		{"$***", false, 0},
		{"$**a", false, 0},
		{"$**[1]", true, 2},
		{"$.a**.1", false, 0},
		{"$.a**.a", true, 3},
		{"$.a**.\"1\"", true, 3},
	}
	for _, kase := range kases {
		v, err := ParseJsonPath(kase.path)
		if kase.valid {
			if err != nil {
				t.Errorf("%s is valid, but error: %s", kase.path, err)
			}
			require.Nil(t, err)
			require.Equal(t, kase.sz, len(v.paths))
		} else {
			if err == nil {
				t.Errorf("%s is invalid, but no error", kase.path)
			}
			require.NotNil(t, err)
		}
	}
}
func TestStar(t *testing.T) {
	var kases = []struct {
		path string
		flag pathFlag
	}{
		{"$[*]", pathFlagSingleStar},
		{"$[*].a", pathFlagSingleStar},
		{`$[*]."a"`, pathFlagSingleStar},
		{"$.a[*]", pathFlagSingleStar},
		{"$.a[*].b", pathFlagSingleStar},
		{`$.a[*]."b"`, pathFlagSingleStar},
		{"$**.a", pathFlagDoubleStar},
		{`$**.a[1]`, pathFlagDoubleStar},
		{`$.a[1]`, 0},
	}
	for _, kase := range kases {
		p, err := ParseJsonPath(kase.path)
		if err != nil {
			t.Errorf("%s is invalid, but no error", kase.path)
		}
		require.Nil(t, err)
		require.Equal(t, kase.flag, p.flag)
	}
}
