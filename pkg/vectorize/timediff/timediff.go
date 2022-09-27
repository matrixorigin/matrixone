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
package timediff

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TimeDiff(lv, rv []types.Datetime, rs []string) []string {
	for i := range lv {
		rs[i] = DateTimeToString(lv[i], rv[i])
	}
	return rs
}

func TimeDiffRightConst(lv []types.Datetime, rv types.Datetime, rs []string) []string {
	for i := range lv {
		rs[i] = DateTimeToString(lv[i], rv)
	}
	return rs
}

func TimeDiffLeftConst(lv types.Datetime, rv []types.Datetime, rs []string) []string {
	for i := range rv {
		rs[i] = DateTimeToString(lv, rv[i])
	}
	return rs
}

func TimeDiffAllConst(lv, rv types.Datetime, rs []string) []string {
	rs[0] = DateTimeToString(lv, rv)
	return rs
}

func DateTimeToString(ldate, rdate types.Datetime) string {
	if ldate-rdate < 0 {
		return "-" + (rdate - ldate).String2(6)
	} else {
		return (ldate - rdate).String2(6)
	}
}
