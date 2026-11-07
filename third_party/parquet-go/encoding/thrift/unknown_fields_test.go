// Copyright 2026 Matrix Origin
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

package thrift_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
)

func TestUnknownBooleanFieldsAndCollections(t *testing.T) {
	type inner struct {
		A bool  `thrift:"1"`
		B bool  `thrift:"2"`
		N int32 `thrift:"3"`
	}
	type input struct {
		A        bool              `thrift:"1"`
		B        bool              `thrift:"2"`
		Inner    inner             `thrift:"3"`
		List     []bool            `thrift:"4"`
		Set      map[bool]struct{} `thrift:"5"`
		Map      map[bool]bool     `thrift:"6"`
		Sentinel string            `thrift:"7"`
	}
	type output struct {
		Sentinel string `thrift:"7"`
	}
	value := input{true, false, inner{true, false, 42}, []bool{true, false}, map[bool]struct{}{true: {}, false: {}}, map[bool]bool{true: false, false: true}, "sentinel"}
	for name, p := range map[string]thrift.Protocol{"compact": &thrift.CompactProtocol{}, "binary": &thrift.BinaryProtocol{}} {
		t.Run(name, func(t *testing.T) {
			b, err := thrift.Marshal(p, &value)
			if err != nil {
				t.Fatal(err)
			}
			r := bytes.NewReader(b)
			var got output
			if err := thrift.NewDecoder(p.NewReader(r)).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if got.Sentinel != value.Sentinel || r.Len() != 0 {
				t.Fatalf("got %q, %d bytes remaining", got.Sentinel, r.Len())
			}
			if err := thrift.Unmarshal(p, b[:len(b)/2], &got); err == nil {
				t.Fatal("truncated collection decoded successfully")
			}
		})
	}
}
