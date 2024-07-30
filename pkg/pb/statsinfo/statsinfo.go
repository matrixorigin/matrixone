// Copyright 2021 - 2024 Matrix Origin
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

package statsinfo

import (
	"math"

	"golang.org/x/exp/constraints"
)

func (sc *StatsInfo) NeedUpdate(currentApproxObjNum int64) bool {
	if sc.ApproxObjectNumber == 0 || sc.AccurateObjectNumber == 0 {
		return true
	}
	if math.Abs(float64(sc.ApproxObjectNumber-currentApproxObjNum)) >= float64(sc.AccurateObjectNumber) {
		return true
	}
	if float64(currentApproxObjNum)/float64(sc.ApproxObjectNumber) > 1.05 || float64(currentApproxObjNum)/float64(sc.ApproxObjectNumber) < 0.95 {
		return true
	}
	return false
}

func (sc *StatsInfo) Merge(newInfo *StatsInfo) {
	if sc == nil || newInfo == nil {
		return
	}
	// TODO: do not handle ShuffleRange for now.
	// sc.ShuffleRangeMap = nil
	sc.NdvMap = mergeMaps(sc.NdvMap, newInfo.NdvMap)
	sc.MinValMap = mergeMaps(sc.MinValMap, newInfo.MinValMap)
	sc.MaxValMap = mergeMaps(sc.MaxValMap, newInfo.MaxValMap)
	sc.DataTypeMap = mergeMaps(sc.DataTypeMap, newInfo.DataTypeMap)
	sc.NullCntMap = mergeMaps(sc.NullCntMap, newInfo.NullCntMap)
	sc.SizeMap = mergeMaps(sc.SizeMap, newInfo.SizeMap)
	sc.BlockNumber += newInfo.BlockNumber
	sc.ApproxObjectNumber += newInfo.ApproxObjectNumber
	sc.TableCnt += newInfo.TableCnt
}

func mergeMaps[K comparable, V constraints.Ordered](m1, m2 map[K]V) map[K]V {
	result := make(map[K]V)
	for key, value := range m1 {
		result[key] = value
	}
	for key, value := range m2 {
		if v, exists := result[key]; exists {
			result[key] = v + value
		} else {
			result[key] = value
		}
	}
	return result
}
