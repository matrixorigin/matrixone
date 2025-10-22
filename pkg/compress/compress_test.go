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

package compress

import (
	"fmt"
	"log"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/pierrec/lz4/v4"
)

func TestLz4(t *testing.T) {
	var err error

	xs := []int64{200, 200, 0, 200, 10, 30, 20, 1111}
	raw := types.EncodeSlice(xs)
	fmt.Printf("raw: %v\n", raw)
	buf := make([]byte, lz4.CompressBlockBound(len(raw)))
	if buf, err = Compress(raw, buf, Lz4); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("buf: %v\n", buf)
	data, err := Decompress(buf, raw, Lz4)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("dat: %v\n", data)
}
