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

package power

import (
	"math"
)

var (
	PowerScalarLeftConst  func(scalar float64, rv []float64, rs []float64) []float64
	PowerScalarRightConst func(scalar float64, rv []float64, rs []float64) []float64
	Power                 func(lv, rv, rs []float64) []float64
)

func init() {
	PowerScalarLeftConst = powerScalarLeftConst
	PowerScalarRightConst = powerScalarRightConst
	Power = power
}

func powerScalarLeftConst(scalar float64, rv []float64, rs []float64) []float64 {
	for i, x := range rv {
		rs[i] = math.Pow(scalar, x)
	}
	return rs
}

func powerScalarRightConst(scalar float64, rv []float64, rs []float64) []float64 {
	for i, x := range rv {
		rs[i] = math.Pow(x, scalar)
	}
	return rs
}

func power(lv, rv, rs []float64) []float64 {
	for i, x := range lv {
		rs[i] = math.Pow(x, rv[i])
	}
	return rs
}
