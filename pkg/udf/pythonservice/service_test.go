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

package pythonservice

import "testing"

func TestSplitPath(t *testing.T) {

	var testcases = []struct {
		pathname string
		lv       int
		want     string
	}{
		{
			pathname: "/usr/local/bin/python3.7",
			lv:       1,
			want:     "/usr/local/bin/",
		},
		{
			pathname: "/usr/local/bin/python3.7",
			lv:       2,
			want:     "/usr/local/",
		},
		{
			pathname: "/usr/local/bin/python3.7",
			lv:       3,
			want:     "/usr/",
		},
		{
			pathname: "/usr/local/bin/python3.7",
			lv:       4,
			want:     "",
		},
		{
			pathname: "/usr/local/bin/python3.7",
			lv:       5,
			want:     "not found",
		},
		{
			pathname: "/usr/local/bin/python3.7",
			lv:       6,
			want:     "not found",
		},
		{
			pathname: "/usr/local/bin/python3.7",
			lv:       0,
			want:     "",
		},
	}

	for _, testcase := range testcases {
		got := splitPath(testcase.pathname, testcase.lv)
		if got != testcase.want {
			t.Errorf("TestSplitPath: got:%s, want:%s.", got, testcase.want)
		}
	}
}
