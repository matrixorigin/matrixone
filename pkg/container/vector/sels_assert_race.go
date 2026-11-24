// Copyright 2026 Matrix Origin
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

//go:build race

package vector

import "fmt"

// assertAscendingSels panics, in race builds only, when sels is not strictly
// increasing and non-negative. Shrink and the ordered null filters assume it
// without checking; a selection moving to a bitmap relies on it too (#33181).
const raceBuild = true

func assertAscendingSels(sels []int64) {
	for i, r := range sels {
		if r < 0 || i > 0 && r <= sels[i-1] {
			panic(fmt.Sprintf("row selection is not strictly increasing at %d: %d", i, r))
		}
	}
}
