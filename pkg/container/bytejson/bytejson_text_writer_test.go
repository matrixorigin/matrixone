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

package bytejson

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteJSONTextMatchesMarshalJSON(t *testing.T) {
	nested, err := CreateByteJSON(map[string]any{
		"array": []any{nil, true, int64(-7), uint64(9), 1.25, "a\n\"中"},
		"empty": map[string]any{},
	})
	require.NoError(t, err)
	raw := []byte{0, 1, 2, 3, 250, 251, 252}
	values := []ByteJson{
		nested,
		{Type: TpCodeOpaque, Data: appendBinaryString(nil, string(raw))},
		{Type: TpCodeBit, Data: appendBinaryString(nil, string(raw))},
		{
			Type: TpCodeBlob,
			Data: appendBinaryString(
				nil,
				persistedBitPrefix+base64.StdEncoding.EncodeToString(raw),
			),
		},
		{
			Type: TpCodeBlob,
			Data: appendBinaryString(nil, persistedBitPrefix+"not-base64!"),
		},
	}
	for _, value := range values {
		want, err := value.MarshalJSON()
		require.NoError(t, err)
		var got bytes.Buffer
		require.NoError(t, WriteJSONText(&got, value))
		require.Equal(t, want, got.Bytes())
	}
}

func TestWriteJSONStringRejectsInvalidUTF8(t *testing.T) {
	var output bytes.Buffer
	require.Error(t, WriteJSONString(&output, []byte{'a', 0xff}))
}
