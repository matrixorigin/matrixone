// Copyright 2021 Matrix Origin
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

package common

import "fmt"

type PPLevel int8

const (
	PPL0 PPLevel = iota
	PPL1
	PPL2
	PPL3
)

func RepeatStr(str string, times int) string {
	for i := 0; i < times; i++ {
		str = fmt.Sprintf("%s\t", str)
	}
	return str
}
