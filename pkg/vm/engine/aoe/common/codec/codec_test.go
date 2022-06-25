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

package codec

import (
	"bytes"
	"fmt"
	"testing"
)

func TestFormat(t *testing.T) {
	{
		data := EncodeKey(1, 3, "x", []byte("g"))
		fmt.Printf("data: %v\n", data)
	}
	{
		data := Encode(1, 3, "x", []byte("g"), uint8(0), uint64(33))
		fmt.Printf("data: %v\n", data)
		vs := Decode(data)
		fmt.Printf("vs: %v\n", vs)
	}
	{
		x, y := Encode(1, "x", 10), Encode(1, "x", 2)
		fmt.Printf("x: %v, y: %v - :%v\n", x, y, bytes.Compare(x, y))
	}
}
