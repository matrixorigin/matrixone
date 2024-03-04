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

import "math"

func (sc *StatsInfo) NeedUpdate(currentApproxObjNum int64) bool {
	if sc.ApproxObjectNumber == 0 || sc.AccurateObjectNumber == 0 {
		return true
	}
	if math.Abs(float64(sc.ApproxObjectNumber-currentApproxObjNum)) >= 10 {
		return true
	}
	if float64(currentApproxObjNum)/float64(sc.ApproxObjectNumber) > 1.05 || float64(currentApproxObjNum)/float64(sc.ApproxObjectNumber) < 0.95 {
		return true
	}
	return false
}
