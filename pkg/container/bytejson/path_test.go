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
		path   string
		valid  bool
		simple bool
		sz     int
	}{
		{"$", true, true, 0},
		{"$.", false, false, 0},
		{"$.*", true, false, 1},
		{"$.a", true, true, 1},
		{"$..a.", false, false, 0},
		{"$.a.b", true, true, 2},
		{"$.a..b", false, false, 0},
		{"$[1]", true, true, 1},
		{"$[1].", false, false, 1},
		{"$[1].a", true, true, 2},
		{"$[1]..a", false, false, 0},
		{"$[1].a.b", true, true, 3},
		{"$[1].a..b", false, false, 0},
		{"$[-1]", false, false, 0},
		{"$[1][2]", true, true, 2},
		{"$[1][2].", false, false, 2},
		{"$[1][2].a", true, true, 3},
		{"$[1][2]..a", false, false, 0},
		{"$[*]", true, false, 1},
		{"$[*].", false, false, 1},
		{"$[*].a", true, false, 2},
		{`$[*]."a"`, true, false, 2},
		{"$[*]..a", false, false, 0},
		{"$[*].a.b", true, false, 3},
		{"$[*].a..b", false, false, 0},
		{"$[1][*]", true, false, 2},
		{"$[1][*].", false, false, 2},
		{"cscdwg", false, false, 0},
		{"$.**", false, false, 0},
		{"$**", false, false, 0},
		{"$**.a", true, false, 2},
		{"$**.a[1]", true, false, 3},
		{"$*1", false, false, 0},
		{"$*a", false, false, 0},
		{"$***", false, false, 0},
		{"$**a", false, false, 0},
		{"$**[1]", true, false, 2},
		{"$.a**.1", false, false, 0},
		{"$.a**.a", true, false, 3},
		{"$.a**.\"1\"", true, false, 3},
		{"$[last]", true, true, 1},
		{"$[last-1]", true, true, 1},
		{"$[last -1]", true, true, 1},
		{"$[last    -  1]", true, true, 1},
		{"$[  last - 1  ]", true, true, 1},
		{"$[last +1]", false, false, 1},
		{"$[0 to 1]", true, false, 1},
		{"$[last to 0]", true, false, 1},
		{"$[last - 1 to 1]", true, false, 1},
		{"$[last -2 to last -3]", false, false, 1},
		{"$[last -3 to last -2]", true, false, 1},
		{"$[0to1]", false, false, 1},
		{"$[0 to1", false, false, 1},
		{"$[0to 1]", false, false, 1},
		{"$[lastto0]", false, false, 1},
		{"$[last-1to1", false, false, 1},
		{"$[last -1to 1", false, false, 1},
		{"$[last -1t to0", false, false, 1},
	}
	for _, kase := range kases {
		v, err := ParseJsonPath(kase.path)
		if kase.valid {
			if err != nil {
				t.Errorf("%s is valid, but error: %s", kase.path, err)
			}
			require.Nil(t, err)
			require.Equal(t, kase.sz, len(v.paths))
			require.Equal(t, kase.simple, v.IsSimple())
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
