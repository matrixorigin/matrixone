// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquetvariant

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/stretchr/testify/require"
)

func TestDocumentSharedBudgetAndCancellation(t *testing.T) {
	d, err := NewDocument(context.Background(), []byte{1, 1, 0, 1, 'a'})
	require.NoError(t, err)
	ok, err := d.HasField("a")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = d.HasField("missing")
	require.NoError(t, err)
	require.False(t, ok)
	before := d.d.remaining
	_, err = d.HasField("a")
	require.NoError(t, err)
	require.Equal(t, before, d.d.remaining)
	// Residual fragments share one budget; independently decoding them would
	// incorrectly accept every fragment with a fresh allowance.
	d.d.remaining = 21
	_, err = d.DecodeValue([]byte{5, 'x'}, 1)
	require.NoError(t, err)
	_, err = d.DecodeValue([]byte{5, 'y'}, 1)
	require.ErrorContains(t, err, "limit")
	for _, n := range []int{-1, maxDecodedBytes/64 + 1} {
		require.Error(t, d.CheckElementCount(n))
	}
	require.Error(t, d.Reserve(-1))
	require.Error(t, d.Reserve(maxDecodedBytes))
	require.Error(t, func() error { _, e := d.DecodeValue([]byte{0}, -1); return e }())
	require.Error(t, func() error { _, e := d.DecodeValue([]byte{0}, 101); return e }())
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := fmt.Errorf("stop row")
	cancel(cause)
	_, err = NewDocument(ctx, emptyMetadata)
	require.ErrorIs(t, err, cause)
	d, err = NewDocument(context.Background(), emptyMetadata)
	require.NoError(t, err)
	d.d.ctx = ctx
	require.ErrorIs(t, d.Reserve(0), cause)
	_, err = d.Array(nil, 0)
	require.ErrorIs(t, err, cause)
}

func TestDocumentContainerBoundaries(t *testing.T) {
	d, err := NewDocument(context.Background(), emptyMetadata)
	require.NoError(t, err)
	_, err = d.Array(nil, 100)
	require.ErrorContains(t, err, "100")
	_, err = d.Object(nil, 100)
	require.ErrorContains(t, err, "100")
	_, err = d.Object(map[string]any{string([]byte{255}): bytejson.Null}, 0)
	require.Error(t, err)
	_, err = d.Object(map[string]any{strings.Repeat("a", 65536): bytejson.Null}, 0)
	require.Error(t, err)
	require.NoError(t, d.CheckElementCount(2))
	got, err := d.Array([]any{bytejson.Null, bytejson.Null}, 99)
	require.NoError(t, err)
	require.Equal(t, "[null, null]", got.String())
	got, err = d.Object(map[string]any{"A": bytejson.Null, "a": bytejson.Null, "": bytejson.Null}, 0)
	require.NoError(t, err)
	require.Equal(t, `{"": null, "A": null, "a": null}`, got.String())
	d.d.remaining = 1
	_, err = d.Object(map[string]any{"a": bytejson.Null}, 0)
	require.Error(t, err)
	_, err = NewDocument(context.Background(), nil)
	require.Error(t, err)
}

func TestDocumentRejectsNestedCopyAmplification(t *testing.T) {
	// The wire input is approximately 1 MiB, but repeatedly assembling the
	// nested binary JSON would copy over 64 MiB. Charge those copies as well.
	bj, err := bytejson.CreateByteJSON(strings.Repeat("x", 1<<20))
	require.NoError(t, err)
	d, err := NewDocument(context.Background(), emptyMetadata)
	require.NoError(t, err)
	rejected := false
	for i := 0; i < 100; i++ {
		bj, err = d.Array([]any{bj}, i)
		if err != nil {
			require.ErrorContains(t, err, "limit")
			rejected = true
			break
		}
	}
	require.True(t, rejected)
	text := strings.Repeat("[", 80) + `"` + strings.Repeat("x", 1<<20) + `"` + strings.Repeat("]", 80)
	bj, err = bytejson.ParseFromStringExact(text)
	require.NoError(t, err)
	_, _, err = Encode(context.Background(), bj)
	require.ErrorContains(t, err, "expansion")
	// Independent nested wire uses 4-byte offset tables, so no encoder
	// admission rule can mask a decoder allocation-budget error.
	wire := binary.LittleEndian.AppendUint32([]byte{16 << 2}, 1<<20)
	wire = append(wire, strings.Repeat("x", 1<<20)...)
	for i := 0; i < 80; i++ {
		header := binary.LittleEndian.AppendUint32([]byte{15, 1}, 0)
		header = binary.LittleEndian.AppendUint32(header, uint32(len(wire)))
		wire = append(header, wire...)
	}
	_, err = Decode(context.Background(), emptyMetadata, wire)
	require.ErrorContains(t, err, "limit")
}
