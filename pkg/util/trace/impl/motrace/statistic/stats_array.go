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

type StatsArray [StatsArrayLength]uint64

const (
	StatsArrayVersion = StatsArrayVersion1

	StatsArrayVersion0 = 0 // raw statistics
	StatsArrayVersion1 = 1 // int64 array
)

const (
	StatsArrayIndexVersion = iota
	StatsArrayIndexTimeConsumed
	StatsArrayIndexMemorySize
	StatsArrayIndexS3IOInputCount
	StatsArrayIndexS3IOOutputCount // index: 4

	StatsArrayLength
)

func NewStatsArray() *StatsArray {
	var s StatsArray
	return s.Init()
}

func (s *StatsArray) Init() *StatsArray {
	return s.WithVersion(StatsArrayVersion)
}

func (s *StatsArray) Reset() *StatsArray {
	return s.WithVersion(StatsArrayVersion).WithTimeConsumed(0).WithMemorySize(0).WithS3IOInputCount(0).WithS3IOOutputCount(0)
}

func (s *StatsArray) GetVersion() uint64         { return (*s)[StatsArrayIndexVersion] }
func (s *StatsArray) GetTimeConsumed() uint64    { return (*s)[StatsArrayIndexTimeConsumed] }    // unit: ns
func (s *StatsArray) GetMemorySize() uint64      { return (*s)[StatsArrayIndexMemorySize] }      // unit: byte
func (s *StatsArray) GetS3IOInputCount() uint64  { return (*s)[StatsArrayIndexS3IOInputCount] }  // unit: count
func (s *StatsArray) GetS3IOOutputCount() uint64 { return (*s)[StatsArrayIndexS3IOOutputCount] } // unit: count

func (s *StatsArray) WithVersion(v uint64) *StatsArray { (*s)[StatsArrayIndexVersion] = v; return s }
func (s *StatsArray) WithTimeConsumed(v uint64) *StatsArray {
	(*s)[StatsArrayIndexTimeConsumed] = v
	return s
}
func (s *StatsArray) WithMemorySize(v uint64) *StatsArray {
	(*s)[StatsArrayIndexMemorySize] = v
	return s
}
func (s *StatsArray) WithS3IOInputCount(v uint64) *StatsArray {
	(*s)[StatsArrayIndexS3IOInputCount] = v
	return s
}
func (s *StatsArray) WithS3IOOutputCount(v uint64) *StatsArray {
	(*s)[StatsArrayIndexS3IOOutputCount] = v
	return s
}

func (s *StatsArray) ToJsonString() []byte {
	return ArrayUint64ToJsonString((*s)[:])
}

func (s *StatsArray) Add(src *StatsArray) *StatsArray {
	dstLen := len(*src)
	if len(*s) < len(*src) {
		dstLen = len(*s)
	}
	for idx := 1; idx < dstLen; idx++ {
		(*s)[idx] += (*src)[idx]
	}
	return s
}

// ArrayUint64ToJsonString return json arr format
func ArrayUint64ToJsonString(arr []uint64) []byte {
	// len([1,184467440737095516161,18446744073709551616,18446744073709551616,18446744073709551616]") = 88
	buf := make([]byte, 0, 128)
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

var initStatsArray = StatsArray{}

var DefaultStatsArray = *initStatsArray.Init()

var DefaultStatsArrayJsonString = initStatsArray.Init().ToJsonString()
