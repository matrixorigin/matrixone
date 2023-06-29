// Copyright 2021 - 2023 Matrix Origin
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

package statistic

import (
	"strconv"
)

type StatsArray []uint64

const (
	StatsArrayVersion0 = 0 // raw statistics
	StatsArrayVersion1 = 1 // int64 array
	StatsArrayVersion  = StatsArrayVersion1
)
const (
	StatsArrayIndexVersion = iota
	StatsArrayIndexTimeConsumed
	StatsArrayIndexMemorySize
	StatsArrayIndexS3IOInputCount
	StatsArrayIndexS3IOOutputCount // index: 4

	StatsArrayLength
)

func (s *StatsArray) GetVersion() uint64         { return (*s)[StatsArrayIndexVersion] }
func (s *StatsArray) GetTimeConsumed() uint64    { return (*s)[StatsArrayIndexTimeConsumed] }    // unit: ns
func (s *StatsArray) GetMemorySize() uint64      { return (*s)[StatsArrayIndexMemorySize] }      // unit: byte
func (s *StatsArray) GetS3IOInputCount() uint64  { return (*s)[StatsArrayIndexS3IOInputCount] }  // unit: count
func (s *StatsArray) GetS3IOOutputCount() uint64 { return (*s)[StatsArrayIndexS3IOOutputCount] } // unit: count

func (s *StatsArray) ToJson() []byte {
	return ArrayUint64ToJson((*s)[:])
}

func ArrayUint64ToJson(arr []uint64) []byte {
	buf := make([]byte, 0, 1024)
	buf = append(buf, '[')
	for idx, v := range arr {
		if idx > 0 {
			buf = append(buf, ',')
		}
		buf = strconv.AppendUint(buf, v, 10)
	}
	buf = append(buf, ']')
	return buf
}
