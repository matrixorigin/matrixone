// Copyright 2025 Matrix Origin
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

package frontend

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestBufferPool(t *testing.T) {
	buf := acquireBuffer()
	buf.WriteString("data")
	releaseBuffer(buf)

	buf2 := acquireBuffer()
	require.Equal(t, 0, buf2.Len())
	releaseBuffer(buf2)
}

func TestFormatValIntoString_StringEscaping(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val := "a'b\"c\\\n\t\r\x1a\x00"
	formatValIntoString(ses, val, types.New(types.T_varchar, 0, 0), &buf)
	require.Equal(t, `'a\'b\"c\\\n\t\r\Z\0'`, buf.String())
}

func TestFormatValIntoString_ByteEscaping(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val := []byte{'x', 0x00, '\\', 0x07, '\''}
	formatValIntoString(ses, val, types.New(types.T_varbinary, 0, 0), &buf)
	require.Equal(t, `'x\0\\\x07\''`, buf.String())
}

func TestFormatValIntoString_JSONEscaping(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val := `{"k":"` + string([]byte{0x01, '\n'}) + `"}`
	formatValIntoString(ses, val, types.New(types.T_json, 0, 0), &buf)
	require.Equal(t, `'{"k":"\\u0001\\u000a"}'`, buf.String())
}

func TestFormatValIntoString_JSONByteJson(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	bj, err := types.ParseStringToByteJson(`{"a":1}`)
	require.NoError(t, err)

	formatValIntoString(ses, bj, types.New(types.T_json, 0, 0), &buf)
	require.Equal(t, `'{"a": 1}'`, buf.String())
}

func TestFormatValIntoString_Nil(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	formatValIntoString(ses, nil, types.New(types.T_varchar, 0, 0), &buf)
	require.Equal(t, "NULL", buf.String())
}

func TestFormatValIntoString_UnsupportedType(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	require.Panics(t, func() {
		formatValIntoString(ses, true, types.New(types.T_bool, 0, 0), &buf)
	})
}
