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

package overflow

import (
	"golang.org/x/exp/constraints"
)

// ------------------------------------add-------------------------------------------
func OverflowUIntAdd[T constraints.Unsigned](a, b, c T) bool {
	return c < a || c < b
}

func OverflowIntAdd[T constraints.Signed](a, b, c T) bool {
	return (c > a) != (b > 0)
	//or
	//return a > 0 && b > 0 && c < 0 || a < 0 && b < 0 && c > 0
}

//------------------------------------sub-------------------------------------------

func OverflowUIntSub[T constraints.Unsigned](a, b, c T) bool {
	return a < b
}

func OverflowIntSub[T constraints.Signed](a, b, c T) bool {
	return (c < a) != (b > 0)
}
