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

package left

func LeftAllConst(src []string, length []int64, rs []string) []string {
	rs[0] = evalLeft(src[0], length[0])
	return rs
}

func Left(src []string, length []int64, rs []string) []string {
	for i := range src {
		rs[i] = evalLeft(src[i], length[i])
	}
	return rs
}

func LeftRightConst(src []string, length []int64, rs []string) []string {
	for i := range src {
		rs[i] = evalLeft(src[i], length[0])
	}
	return rs
}

func LeftLeftConst(src []string, length []int64, rs []string) []string {
	for i := range length {
		rs[i] = evalLeft(src[0], length[i])
	}
	return rs
}

func evalLeft(str string, length int64) string {
	runeStr := []rune(str)
	leftLength := int(length)
	if strLength := len(runeStr); leftLength > strLength {
		leftLength = strLength
	} else if leftLength < 0 {
		leftLength = 0
	}
	return string(runeStr[:leftLength])
}
