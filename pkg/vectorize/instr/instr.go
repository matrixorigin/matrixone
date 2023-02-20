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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"strings"
	"unicode"
)

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}

func kmp(r1, r2 []rune) int64 {
	next := make([]int, len(r2))
	next[0] = -1
	for i, j := 0, -1; i < len(r2)-1; {
		if j == -1 || r2[i] == r2[j] {
			i++
			j++
			next[i] = j
		} else {
			j = next[j]
		}
	}
	for i, j := 0, 0; i < len(r1); {
		if j == -1 || r1[i] == r2[j] {
			i++
			j++
		} else {
			j = next[j]
		}
		if j == len(r2) {
			return int64(i - j + 1)
		}
	}
	return 0
}

func Single(str string, substr string) int64 {
	if len(substr) == 0 {
		return 1
	}
	if isASCII(str) {
		if !isASCII(substr) {
			return 0
		}
		return int64(strings.Index(str, substr) + 1)
	}
	r1, r2 := []rune(str), []rune(substr)
	return kmp(r1, r2)
}

func Instr(s1, s2 []string, snsp []*nulls.Nulls, rs []int64, nsp *nulls.Nulls) {
	s1GoOn, s2GoOn := len(s1) > 1, len(s2) > 1
	if s1GoOn && s2GoOn {
		instr3(s1, s2, snsp, rs, nsp)
	} else if s1GoOn {
		instr1(s1, s2, snsp, rs, nsp)
	} else {
		instr2(s1, s2, snsp, rs, nsp)
	}
}

func instr1(s1, s2 []string, snsp []*nulls.Nulls, rs []int64, nsp *nulls.Nulls) {
	substr := s2[0]
	for i, str := range s1 {
		if snsp[0].Contains(uint64(i)) {
			nsp.Set(uint64(i))
			continue
		}
		rs[i] = Single(str, substr)
	}
}
func instr2(s1, s2 []string, snsp []*nulls.Nulls, rs []int64, nsp *nulls.Nulls) {
	str := s1[0]
	for i, substr := range s2 {
		if snsp[1].Contains(uint64(i)) {
			nsp.Set(uint64(i))
			continue
		}
		rs[i] = Single(str, substr)
	}
}

func instr3(s1, s2 []string, snsp []*nulls.Nulls, rs []int64, nsp *nulls.Nulls) {
	for i, str := range s1 {
		if snsp[0].Contains(uint64(i)) || snsp[1].Contains(uint64(i)) {
			nsp.Set(uint64(i))
			continue
		}
		rs[i] = Single(str, s2[i])
	}
}
