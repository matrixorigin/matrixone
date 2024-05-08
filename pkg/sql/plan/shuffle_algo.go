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

package plan

import (
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
)

func NewShuffleRange(isString bool) *pb.ShuffleRange {
	return &pb.ShuffleRange{IsStrType: isString}
}

func ShuffleRangeReEvalUnsigned(ranges []float64, k2 int, nullCnt int64, tableCnt int64) []uint64 {
	k1 := len(ranges)
	if k2 > k1/2 {
		return nil
	}
	result := make([]uint64, k2-1)
	if tableCnt/int64(k2) <= nullCnt {
		result[0] = uint64(ranges[0])
		for i := 1; i <= k2-2; i++ {
			result[i] = uint64(ranges[(i)*(k1-1)/(k2-1)])
		}
	} else {
		for i := 0; i <= k2-2; i++ {
			result[i] = uint64(ranges[(i+1)*k1/k2-1])
		}
	}
	return result
}

func ShuffleRangeReEvalSigned(ranges []float64, k2 int, nullCnt int64, tableCnt int64) []int64 {
	k1 := len(ranges)
	if k2 > k1/2 {
		return nil
	}
	result := make([]int64, k2-1)
	if tableCnt/int64(k2) <= nullCnt {
		result[0] = int64(ranges[0])
		for i := 1; i <= k2-2; i++ {
			result[i] = int64(ranges[(i)*(k1-1)/(k2-1)])
		}
	} else {
		for i := 0; i <= k2-2; i++ {
			result[i] = int64(ranges[(i+1)*k1/k2-1])
		}
	}
	return result
}
