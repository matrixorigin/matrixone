// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashtable

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

func TestLimits(t *testing.T) {
	softLimit := uint64(1 << 10)
	hardLimit := uint64(2 * (1 << 10))
	malloc.WithTempDefaultConfig(malloc.Config{
		HashmapSoftLimit: &softLimit,
		HashmapHardLimit: &hardLimit,
	}, func() {

		func() {
			defer func() {
				p := recover()
				if p == nil {
					t.Fatal("should panic")
				}
				msg := fmt.Sprintf("%v", p)
				if !strings.Contains(msg, "exceed hard limit") {
					t.Fatalf("got %v", msg)
				}
			}()

			var hashmap StringHashMap
			hashmap.Init(newAllocator())
			hashmap.ResizeOnDemand(2 * (1 << 10))
		}()

	})
}
