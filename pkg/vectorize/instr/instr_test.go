// Copyright 2022 Matrix Origin
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

package instr

import (
	"testing"
)

func TestSingle(t *testing.T) {
	kases := []struct {
		str string
		sub string
		ret int64
	}{
		{"abc", "bc", 2},
		{"abc", "b", 2},
		{"abc", "abc", 1},
		{"abc", "a", 1},
		{"abc", "dca", 0},
		{"abc", "", 1},
		{"", "abc", 0},
		{"", "", 1},
		{"abc", "abcabc", 0},
		{"abcabc", "abc", 1},
		{"abcabc", "bc", 2},
		{"啊撒撒撒撒", "啊", 1},
		{"啊撒撒撒撒", "撒", 2},
		{"啊撒撒撒撒", "撒撒", 2},
		{"啊撒撒撒撒x的q", "x", 6},
		{"啊撒撒撒撒x的q", "x的", 6},
		{"啊撒撒撒撒x的q", "x的q", 6},
		{"啊撒撒撒撒x的q", "x的q啊", 0},
		{"啊撒撒撒撒x的q", "的", 7},
		{"啊撒撒撒撒x的q", "的q", 7},
		{"啊撒撒撒撒x的q", "的q啊", 0},
		{"啊撒撒撒撒x的q", "q", 8},
	}
	for _, kase := range kases {
		ret := Single(kase.str, kase.sub)
		if ret != kase.ret {
			t.Fatalf("str: %s, sub: %s, ret: %d, want: %d", kase.str, kase.sub, ret, kase.ret)
		}
	}
}
