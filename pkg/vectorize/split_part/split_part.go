// Copyright 2021 - 2022 Matrix Origin
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

package split_part

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"strings"
)

func SplitSingle(str, sep string, cnt uint32) (string, bool) {
	expectedLen := int(cnt + 1)
	strSlice := strings.SplitN(str, sep, expectedLen)
	if len(strSlice) < int(cnt) || strSlice[cnt-1] == "" {
		return "", true
	}
	return strSlice[cnt-1], false
}

func compute(s1, s2 string, s3 uint32, rs []string, idx int, nsp *nulls.Nulls) {
	res, isNull := SplitSingle(s1, s2, s3)
	if isNull {
		nsp.Set(uint64(idx))
		return
	}
	rs[idx] = res
}

// SplitPart1 is the implementation of split_part(string, string, uint) when the 3rd arguments are not scalar.
func SplitPart1(s1, s2 []string, s3 []uint32, nsps []*nulls.Nulls, rs []string, rnsp *nulls.Nulls) {
	str, sep := s1[0], s2[0]
	for i := 0; i < len(s3); i++ {
		if nsps[2].Contains(uint64(i)) {
			rnsp.Set(uint64(i))
			continue
		}
		count := s3[i]
		compute(str, sep, count, rs, i, rnsp)
	}
}

// SplitPart2 is the implementation of split_part(string, string, uint) when the 2nd argument is not scalar.
func SplitPart2(s1, s2 []string, s3 []uint32, nsps []*nulls.Nulls, rs []string, rnsp *nulls.Nulls) {
	str, count := s1[0], s3[0]
	for i := 0; i < len(s2); i++ {
		if nsps[1].Contains(uint64(i)) {
			rnsp.Set(uint64(i))
			continue
		}
		sep := s2[i]
		compute(str, sep, count, rs, i, rnsp)
	}
}

// SplitPart3 is the implementation of split_part(string, string, uint) when the 2nd and 3rd arguments are not scalar.
func SplitPart3(s1, s2 []string, s3 []uint32, nsps []*nulls.Nulls, rs []string, rnsp *nulls.Nulls) {
	str := s1[0]
	for i := 0; i < len(s2); i++ {
		if nsps[1].Contains(uint64(i)) || nsps[2].Contains(uint64(i)) {
			rnsp.Set(uint64(i))
			continue
		}
		sep, count := s2[i], s3[i]
		compute(str, sep, count, rs, i, rnsp)
	}
}

// SplitPart4 is the implementation of split_part(string, string, uint) when the 1st arguments is not scalar.
func SplitPart4(s1, s2 []string, s3 []uint32, nsps []*nulls.Nulls, rs []string, rnsp *nulls.Nulls) {
	sep, count := s2[0], s3[0]
	for i := 0; i < len(s1); i++ {
		if nsps[0].Contains(uint64(i)) {
			rnsp.Set(uint64(i))
			continue
		}
		str := s1[i]
		compute(str, sep, count, rs, i, rnsp)
	}
}

// SplitPart5 is the implementation of split_part(string, string, uint) when the 1st and 3rd arguments are not scalar.
func SplitPart5(s1, s2 []string, s3 []uint32, nsps []*nulls.Nulls, rs []string, rnsp *nulls.Nulls) {
	sep := s2[0]
	for i := 0; i < len(s1); i++ {
		if nsps[0].Contains(uint64(i)) || nsps[2].Contains(uint64(i)) {
			rnsp.Set(uint64(i))
			continue
		}
		str, count := s1[i], s3[i]
		compute(str, sep, count, rs, i, rnsp)
	}
}

// SplitPart6 is the implementation of split_part(string, string, uint) when the 1st and 2nd arguments are not scalar.
func SplitPart6(s1, s2 []string, s3 []uint32, nsps []*nulls.Nulls, rs []string, rnsp *nulls.Nulls) {
	count := s3[0]
	for i := 0; i < len(s1); i++ {
		if nsps[0].Contains(uint64(i)) || nsps[1].Contains(uint64(i)) {
			rnsp.Set(uint64(i))
			continue
		}
		str, sep := s1[i], s2[i]
		compute(str, sep, count, rs, i, rnsp)
	}
}

// SplitPart7 is the implementation of split_part(string, string, uint) when the 1st, 2nd and 3rd arguments are not scalar.
func SplitPart7(s1, s2 []string, s3 []uint32, nsps []*nulls.Nulls, rs []string, rnsp *nulls.Nulls) {
	for i := 0; i < len(s1); i++ {
		if nsps[0].Contains(uint64(i)) || nsps[1].Contains(uint64(i)) || nsps[2].Contains(uint64(i)) {
			rnsp.Set(uint64(i))
			continue
		}
		str, sep, count := s1[i], s2[i], s3[i]
		compute(str, sep, count, rs, i, rnsp)
	}
}
