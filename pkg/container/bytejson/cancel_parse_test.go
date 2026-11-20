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

package bytejson

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

type cancelAfterJSONChecks struct {
	context.Context
	cancel    context.CancelFunc
	remaining int
}

func (c *cancelAfterJSONChecks) Err() error {
	c.remaining--
	if c.remaining == 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestCancellableJSONParseMatchesLegacy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, input := range []string{
		`{"z":0,"a":{"t":true,"f":false,"n":null,"arr":[-0,18446744073709551615,-9223372036854775809,1e30]},"z":42}`,
		`{"z":1e999,"z":-17,"a":[{},[],"\/\t\r\n\f\b\uD834\uDD1E\uD834\u0000"]}`,
		`{"a":"` + strings.Repeat("x", jsonParseChunk-7) + `\uD834\uDD1E` + strings.Repeat("ξ", jsonParseChunk) + `"}`,
		`{"a":"` + strings.Repeat("x", jsonParseChunk) + "\xff" + `"}`,
		`{"a":0.` + strings.Repeat("0", jsonParseChunk) + `123450e65536}`,
		`{"a":1.25e-` + strings.Repeat("0", jsonParseChunk) + `001}`,
		`{"a":1.` + strings.Repeat("2", jsonParseChunk) + `}`,
	} {
		input = strings.Repeat(" ", jsonParseFastLimit) + input
		old, err := ParseFromByteSlice([]byte(input))
		require.NoError(t, err)
		got, err := ParseFromByteSliceWithCancellation(ctx, []byte(input))
		require.NoError(t, err)
		require.Equal(t, old, got)
	}
	for _, input := range []string{
		`{"a":0,"b":1,}`, `[1,]`, `{} {}`, `[01]`, `{"a":-}`,
		`{"a":1.}`, `{"a":1e+}`, `{"a":"\u012G"}`, `{"a":"\h"}`, `{"a":1e999}`,
		strings.Repeat("[", jsonParseDepthLimit+1) + "0" + strings.Repeat("]", jsonParseDepthLimit+1),
	} {
		input = strings.Repeat(" ", jsonParseFastLimit) + input
		_, err := ParseFromByteSlice([]byte(input))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		_, err = ParseFromByteSliceWithCancellation(ctx, []byte(input))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	}
}

func TestCancellableJSONParseStopsWithinTokenAndContainer(t *testing.T) {
	for _, test := range []struct {
		name       string
		input      string
		wantBefore int
	}{
		{"plain string", `"` + strings.Repeat("x", 3*jsonParseChunk) + "bad suffix", 2 * jsonParseChunk},
		{"escaped string", `"` + strings.Repeat(`\n`, 2*jsonParseChunk) + "bad suffix", 2 * jsonParseChunk},
		{"number", "0." + strings.Repeat("2", 3*jsonParseChunk) + "bad suffix", 2 * jsonParseChunk},
		{"array", "[" + strings.Repeat("1,", 2*jsonParseItems+jsonParseChunk) + "bad suffix", 4 * jsonParseItems},
	} {
		t.Run(test.name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &cancelAfterJSONChecks{parent, cancel, 1}
			p := cancelJSONParser{ctx: ctx, data: []byte(test.input), nextCheck: jsonParseChunk}
			_, err := p.value(0)
			require.ErrorIs(t, err, context.Canceled)
			require.Less(t, p.pos, test.wantBefore)
			require.Less(t, p.pos, len(p.data))
		})
	}
}

func TestCancellableJSONParseStopsWhileWriting(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &cancelAfterJSONChecks{parent, cancel, 2}
	doc := strings.Repeat("x", 4*jsonParseChunk)
	writer := byteJsonWriter{buf: make([]byte, 0, len(doc)+16), cancel: &jsonWriteBudget{}}
	_, _, err := writer.writeNodeWithContext(ctx, true, Node{doc})
	require.ErrorIs(t, err, context.Canceled)
	require.GreaterOrEqual(t, len(writer.buf), jsonParseChunk)
	require.Less(t, len(writer.buf), 2*jsonParseChunk)
}
