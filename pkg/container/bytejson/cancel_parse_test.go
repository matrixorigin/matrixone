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
	"bytes"
	"context"
	"fmt"
	"sort"
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
		checks     int
		wantAfter  int
		wantBefore int
	}{
		{"plain string", `"` + strings.Repeat("x", 3*jsonParseChunk) + "bad suffix", 1, jsonParseChunk, 2 * jsonParseChunk},
		{"escaped string", `"` + strings.Repeat(`\n`, 2*jsonParseChunk) + "bad suffix", 2, jsonParseChunk, 2 * jsonParseChunk},
		{"number", "0." + strings.Repeat("2", 3*jsonParseChunk) + "bad suffix", 1, jsonParseChunk, 2 * jsonParseChunk},
		{"array", "[" + strings.Repeat("1,", 2*jsonParseItems+jsonParseChunk) + "bad suffix", 1, jsonParseItems, 4 * jsonParseItems},
	} {
		t.Run(test.name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &cancelAfterJSONChecks{parent, cancel, test.checks}
			p := cancelJSONParser{ctx: ctx, data: []byte(test.input), nextCheck: jsonParseChunk}
			_, err := p.value(0)
			require.ErrorIs(t, err, context.Canceled)
			require.GreaterOrEqual(t, p.pos, test.wantAfter)
			require.Less(t, p.pos, test.wantBefore)
			require.Less(t, p.pos, len(p.data))
		})
	}
}

func TestCancellableJSONParseErrorOrder(t *testing.T) {
	wideString := `"` + strings.Repeat("a", 2*jsonParseChunk) + `"`
	for _, test := range []struct {
		name  string
		input string
		want  error
	}{
		{"invalid prefix", `[!` + wideString + `]`, nil},
		{"invalid suffix", `[` + wideString + `,!]`, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := []byte(test.input)
			_, err := ParseFromByteSlice(data)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))

			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err = ParseFromByteSliceWithCancellation(parent, data)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))

			ctx := &cancelAfterJSONChecks{parent, cancel, 2}
			_, err = ParseFromByteSliceWithCancellation(ctx, data)
			if test.want == nil {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				require.NoError(t, parent.Err())
			} else {
				require.ErrorIs(t, err, test.want)
			}
		})
	}
}

func TestCancellableJSONParseDeepAndLongNumberBoundaries(t *testing.T) {
	// This decimal is halfway between 1 and the next representable float64.
	const midpoint = "1.00000000000000011102230246251565404236316680908203125"
	zeros := strings.Repeat("0", jsonParseChunk)
	for _, test := range []struct {
		name  string
		input string
	}{
		{"round to even", midpoint + zeros},
		{"round above midpoint", midpoint + zeros + "1"},
		{"capped exponent", "0.1e-" + strings.Repeat("9", jsonParseChunk)},
		{"max nesting", strings.Repeat("[", jsonParseDepthLimit) + "null" + strings.Repeat("]", jsonParseDepthLimit)},
		{"decoded duplicate key", `{"` + strings.Repeat("ξ", jsonParseChunk/4) + `":1e999,"` + strings.Repeat(`\u03be`, jsonParseChunk/4) + `":[]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := []byte(test.input)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			legacy, err := ParseFromByteSlice(data)
			require.NoError(t, err)
			got, err := ParseFromByteSliceWithCancellation(ctx, data)
			require.NoError(t, err)
			require.True(t, legacy.Type == got.Type && bytes.Equal(legacy.Data, got.Data), "binary JSON differs from the legacy parser")
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

func TestCancellableJSONParseStopsWhileSorting(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &cancelAfterJSONChecks{parent, cancel, 1}
	const entries = 4 * jsonParseItems
	group := &Group{Obj: true, Keys: make([]string, entries), Values: make([]Node, entries)}
	for i := range group.Keys {
		group.Keys[i] = fmt.Sprintf("key%04d", i)
		group.Values[i] = Node{group.Keys[i]}
	}
	last := group.Keys[entries-1]
	w := byteJsonWriter{buf: make([]byte, 0, jsonParseChunk), cancel: &jsonWriteBudget{}}
	// The first pair is an inversion, so the sort immediately builds its heap.
	(*groupSortKeys)(group).Swap(0, 1)
	_, _, err := w.writeNodeWithContext(ctx, true, Node{group})
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, w.buf, 1+headerSize)
	require.NotEqual(t, last, group.Keys[entries-1], "heap construction must have started")
	require.False(t, sort.StringsAreSorted(group.Keys), "cancellation must interrupt the sort")
	for i, key := range group.Keys {
		require.Equal(t, key, group.Values[i].V)
	}
}

func TestCancellableJSONKeySortKeepsValuesWithKeys(t *testing.T) {
	const entries = 257
	for _, stride := range []int{1, entries - 1, 79} {
		g := &Group{Obj: true, Keys: make([]string, entries), Values: make([]Node, entries)}
		for i := range g.Keys {
			value := (i * stride) % entries
			g.Keys[i] = fmt.Sprintf("key%03d", value)
			g.Values[i] = Node{value}
		}
		require.NoError(t, sortJSONKeysCancellable(t.Context(), &jsonWriteBudget{}, g))
		for i := range g.Keys {
			require.Equal(t, fmt.Sprintf("key%03d", i), g.Keys[i])
			require.Equal(t, i, g.Values[i].V)
		}
	}
}
