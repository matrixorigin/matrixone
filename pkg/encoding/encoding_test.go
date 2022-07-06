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

package encoding

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestEncode(t *testing.T) {
	vs := make([]types.Int32, 10)
	for i := 0; i < 10; i++ {
		vs[i] = types.Int32(i)
	}
	data := EncodeSlice(vs, 8)
	fmt.Printf("data: %v\n", data)
	rs := DecodeSlice[types.Int32](data, 8)
	fmt.Printf("rs: %v\n", rs)
}
