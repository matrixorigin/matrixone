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

package memorytable

import (
	"bytes"
	"testing"
)

func BenchmarkTupleLess(b *testing.B) {
	x := Tuple{Int(1)}
	y := Tuple{Int(2)}
	for i := 0; i < b.N; i++ {
		if y.Less(x) {
			b.Fatal()
		}
	}
}

func BenchmarkBytesCompare(b *testing.B) {
	x := []byte{1}
	y := []byte{2}
	for i := 0; i < b.N; i++ {
		if bytes.Compare(x, y) >= 0 {
			b.Fatal()
		}
	}
}
