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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"testing"
)

func getLocation(name ObjectName) Location {
	extent := NewExtent(1, 1, 1, 1)
	return BuildLocation(name, extent, 1, 1)
}

func BenchmarkDecode(b *testing.B) {
	var location Location
	uuid, _ := types.BuildUuid()
	name := BuildObjectName(uuid, 1)
	b.Run("build", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			location = getLocation(name)
		}
	})
	b.Run("GetName", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			location.Name().Sid()
			location.Name().Num()
			location.ID()
		}
	})
	b.Log(location.Name().String())
}
