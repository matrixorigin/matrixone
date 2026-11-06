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

package parquettest

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/stretchr/testify/require"
)

func TestUnknownThriftFields(t *testing.T) {
	type nested struct {
		A bool  `thrift:"1"`
		B bool  `thrift:"2"`
		N int32 `thrift:"3"`
	}
	type source struct {
		A        bool              `thrift:"1"`
		B        bool              `thrift:"2"`
		Nested   nested            `thrift:"3"`
		List     []bool            `thrift:"4"`
		Set      map[bool]struct{} `thrift:"5"`
		Map      map[bool]bool     `thrift:"6"`
		Sentinel string            `thrift:"7"`
	}
	type target struct {
		Sentinel string `thrift:"7"`
	}
	value := source{true, false, nested{true, false, 42}, []bool{true, false}, map[bool]struct{}{true: {}, false: {}}, map[bool]bool{true: false, false: true}, "sentinel"}
	for name, protocol := range map[string]thrift.Protocol{"compact": &thrift.CompactProtocol{}, "binary": &thrift.BinaryProtocol{}} {
		t.Run(name, func(t *testing.T) {
			encoded, err := thrift.Marshal(protocol, &value)
			require.NoError(t, err)
			r := bytes.NewReader(encoded)
			var got target
			require.NoError(t, thrift.NewDecoder(protocol.NewReader(r)).Decode(&got))
			require.Equal(t, value.Sentinel, got.Sentinel)
			require.Zero(t, r.Len())
			var truncated target
			require.Error(t, thrift.Unmarshal(protocol, encoded[:len(encoded)/2], &truncated))
		})
	}
}
