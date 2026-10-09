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

package encoding

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

func newTestPool(t testing.TB) *mpool.MPool {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	return mp
}

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		charset collation.Charset
		input   string
		valid   bool
	}{
		{"binary", collation.CharsetBinary, "\x00\xff", true},
		{"ascii-boundary", collation.CharsetASCII, "\x00\x7f", true},
		{"ascii-high-bit", collation.CharsetASCII, "\x80", false},
		{"mb3-boundary", collation.CharsetUTF8MB3, "\uffff", true},
		{"mb3-supplementary", collation.CharsetUTF8MB3, "\U00010000", false},
		{"mb4-supplementary", collation.CharsetUTF8MB4, "😀", true},
		{"replacement-rune", collation.CharsetUTF8MB4, "\ufffd", true},
		{"invalid-utf8", collation.CharsetUTF8MB4, "\xff", false},
		{"incomplete-utf8", collation.CharsetUTF8MB4, "\xe2\x82", false},
		{"empty", collation.CharsetUTF8MB4, "", true},
		{"disabled", collation.CharsetGBK, "", false},
		{"unspecified", collation.CharsetUnspecified, "a", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(context.Background(), tc.charset, []byte(tc.input))
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
			}
		})
	}
}

func TestValidateUTF8Chunks(t *testing.T) {
	for _, prefix := range []int{4092, 4093, 4094, 4095, 4096} {
		for _, r := range []string{"é", "你", "😀"} {
			input := append(bytes.Repeat([]byte("a"), prefix), []byte(r+"z")...)
			require.NoError(t, Validate(context.Background(), collation.CharsetUTF8MB4, input))
			input = append(input, 0x80)
			require.Error(t, Validate(context.Background(), collation.CharsetUTF8MB4, input))
		}
	}
	require.Error(t, Validate(context.Background(), collation.CharsetUTF8MB4, bytes.Repeat([]byte{0x80}, 8192)))
	ctx := &cancelOnCheck{Context: context.Background(), cancelAt: 2}
	require.ErrorIs(t, Validate(ctx, collation.CharsetUTF8MB3, bytes.Repeat([]byte("a"), 8192)), context.Canceled)
}

func TestConvertOwnershipAndPolicies(t *testing.T) {
	mp := newTestPool(t)
	for _, tc := range []struct {
		name            string
		src, dst        collation.Charset
		policy          Policy
		input, expected string
		owned, null     bool
	}{
		{"invalid-identity", collation.CharsetUTF8MB4, collation.CharsetUTF8MB4, Identity, "\xff", "\xff", false, false},
		{"invalid-connection-identity", collation.CharsetASCII, collation.CharsetASCII, Connection, "\xc3\xa9", "\xc3\xa9", false, false},
		{"invalid-result-identity", collation.CharsetUTF8MB4, collation.CharsetUTF8MB4, Result, "\xff", "\xff", false, false},
		{"binary-result", collation.CharsetBinary, collation.CharsetASCII, Result, "\x00\xff", "\x00\xff", false, false},
		{"binary-destination", collation.CharsetUTF8MB4, collation.CharsetBinary, ConvertUsing, "\xff", "\xff", false, false},
		{"convert-invalid", collation.CharsetBinary, collation.CharsetUTF8MB4, ConvertUsing, "\xff", "", false, true},
		{"convert-binary-ascii-replacement", collation.CharsetBinary, collation.CharsetASCII, ConvertUsing, "\x80", "?", true, false},
		{"convert-binary-ascii-byte-units", collation.CharsetBinary, collation.CharsetASCII, ConvertUsing, "é😀", "??????", true, false},
		{"convert-binary-ascii-boundary", collation.CharsetBinary, collation.CharsetASCII, ConvertUsing, "\x00\x7f\xff", "\x00\x7f?", true, false},
		{"convert-binary-ascii-borrow", collation.CharsetBinary, collation.CharsetASCII, ConvertUsing, "a\x00\x7f", "a\x00\x7f", false, false},
		{"convert-binary-ascii-empty", collation.CharsetBinary, collation.CharsetASCII, ConvertUsing, "", "", false, false},
		{"convert-valid", collation.CharsetBinary, collation.CharsetUTF8MB4, ConvertUsing, "é😀", "é😀", false, false},
		{"connection-replacement", collation.CharsetUTF8MB4, collation.CharsetASCII, Connection, "é😀", "??", true, false},
		{"result-replacement", collation.CharsetUTF8MB4, collation.CharsetASCII, Result, "é😀", "??", true, false},
		{"ascii-source", collation.CharsetASCII, collation.CharsetUTF8MB4, Connection, "\x7f\x80\xff", "\x7f??", true, false},
		{"cross-identity", collation.CharsetASCII, collation.CharsetUTF8MB4, Connection, "a\x00", "a\x00", false, false},
		{"incomplete-tail", collation.CharsetUTF8MB4, collation.CharsetASCII, Result, "a\xe2\x82", "a", false, false},
		{"empty-not-null", collation.CharsetBinary, collation.CharsetUTF8MB4, ConvertUsing, "", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := []byte(tc.input)
			original := bytes.Clone(input)
			output, owned, null, err := Convert(context.Background(), mp, tc.src, tc.dst, tc.policy, input, int64(len(input)))
			require.NoError(t, err)
			if owned {
				defer mp.Free(output)
			}
			require.Equal(t, tc.owned, owned)
			require.Equal(t, tc.null, null)
			require.Equal(t, tc.expected, string(output))
			require.True(t, bytes.Equal(original, input), "input was modified")
			if owned {
				require.Positive(t, mp.CurrNB())
			}
			if !owned && !null && len(output) > 0 {
				require.Equal(t, &input[0], &output[0])
			}
		})
		require.Zero(t, mp.CurrNB())
	}
}

// Deterministic cancellation at a scan/allocation/publication boundary, without
// sleeps or a concurrent race against the allocator.
type cancelOnCheck struct {
	context.Context
	checks, cancelAt int
}

func (ctx *cancelOnCheck) Err() error {
	ctx.checks++
	if ctx.checks >= ctx.cancelAt {
		return context.Canceled
	}
	return nil
}

func TestConvertFailureCleanup(t *testing.T) {
	mp := newTestPool(t)
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		pool     *mpool.MPool
		src, dst collation.Charset
		policy   Policy
		input    string
		limit    int64
	}{
		{"negative-limit", context.Background(), mp, collation.CharsetUTF8MB4, collation.CharsetASCII, Result, "a", -1},
		{"identity-limit", context.Background(), mp, collation.CharsetBinary, collation.CharsetBinary, Identity, "ab", 1},
		{"conversion-limit", context.Background(), mp, collation.CharsetUTF8MB4, collation.CharsetASCII, Result, "éé", 1},
		{"binary-ascii-conversion-limit", context.Background(), mp, collation.CharsetBinary, collation.CharsetASCII, ConvertUsing, "\x80", 0},
		{"binary-ascii-nil-pool", context.Background(), nil, collation.CharsetBinary, collation.CharsetASCII, ConvertUsing, "\x80", 1},
		{"binary-ascii-cancel-after-allocation", &cancelOnCheck{Context: context.Background(), cancelAt: 4}, mp, collation.CharsetBinary, collation.CharsetASCII, ConvertUsing, "\x80", 1},
		{"nil-pool", context.Background(), nil, collation.CharsetUTF8MB4, collation.CharsetASCII, Result, "é", 2},
		{"unsupported-source", context.Background(), mp, collation.CharsetGBK, collation.CharsetASCII, Result, "", 0},
		{"unsupported-target", context.Background(), mp, collation.CharsetBinary, collation.CharsetUTF8MB3, ConvertUsing, "", 0},
		{"invalid-policy", context.Background(), mp, collation.CharsetBinary, collation.CharsetBinary, Policy(255), "", 0},
		{"wrong-identity", context.Background(), mp, collation.CharsetUTF8MB4, collation.CharsetASCII, Identity, "", 0},
		{"already-canceled", &cancelOnCheck{Context: context.Background(), cancelAt: 1}, mp, collation.CharsetBinary, collation.CharsetBinary, Identity, "a", 1},
		{"cancel-validation", &cancelOnCheck{Context: context.Background(), cancelAt: 2}, mp, collation.CharsetBinary, collation.CharsetUTF8MB4, ConvertUsing, "a", 1},
		{"cancel-borrow", &cancelOnCheck{Context: context.Background(), cancelAt: 2}, mp, collation.CharsetBinary, collation.CharsetBinary, Identity, "a", 1},
		{"cancel-before-allocation", &cancelOnCheck{Context: context.Background(), cancelAt: 3}, mp, collation.CharsetUTF8MB4, collation.CharsetASCII, Result, "é", 2},
		{"cancel-after-allocation", &cancelOnCheck{Context: context.Background(), cancelAt: 4}, mp, collation.CharsetUTF8MB4, collation.CharsetASCII, Result, "é", 2},
		{"cancel-before-publication", &cancelOnCheck{Context: context.Background(), cancelAt: 5}, mp, collation.CharsetUTF8MB4, collation.CharsetASCII, Result, "é", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, owned, null, err := Convert(tc.ctx, tc.pool, tc.src, tc.dst, tc.policy, []byte(tc.input), tc.limit)
			require.Error(t, err)
			require.Nil(t, output)
			require.False(t, owned)
			require.False(t, null)
			require.Zero(t, mp.CurrNB())
		})
	}
	limited, err := mpool.NewMPool("encoding-oom", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(limited)
	input := bytes.Repeat([]byte{0x80}, (1<<20)+1)
	output, owned, null, err := Convert(context.Background(), limited, collation.CharsetASCII, collation.CharsetUTF8MB4, Connection, input, int64(len(input)))
	if owned {
		defer limited.Free(output)
	}
	require.Error(t, err) // One byte above the smallest supported pool capacity.
	require.Nil(t, output)
	require.False(t, owned)
	require.False(t, null)
	require.Zero(t, limited.CurrNB())
}

func TestConvertCancellationAndBounds(t *testing.T) {
	input := bytes.Repeat([]byte("a"), 8192)
	ctx := &cancelOnCheck{Context: context.Background(), cancelAt: 2}
	require.ErrorIs(t, Validate(ctx, collation.CharsetUTF8MB4, input), context.Canceled)
	ctx = &cancelOnCheck{Context: context.Background(), cancelAt: 2}
	require.ErrorIs(t, Validate(ctx, collation.CharsetASCII, input), context.Canceled)
	ctx = &cancelOnCheck{Context: context.Background(), cancelAt: 2}
	output, owned, null, err := Convert(ctx, nil, collation.CharsetUTF8MB4, collation.CharsetASCII, Result, input, 8192)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, output)
	require.False(t, owned)
	require.False(t, null)
	output, owned, null, err = Convert(context.Background(), nil, collation.CharsetBinary, collation.CharsetBinary, Identity, []byte("a"), 1<<62)
	require.NoError(t, err)
	require.Equal(t, "a", string(output))
	require.False(t, owned)
	require.False(t, null)
	// Budget applies to output, not the larger input before contraction.
	mp := newTestPool(t)
	output, owned, null, err = Convert(context.Background(), mp, collation.CharsetUTF8MB4, collation.CharsetASCII, Result, []byte("é😀"), 2)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, "??", string(output))
	mp.Free(output)
	require.False(t, null)
	require.Zero(t, mp.CurrNB())
}

func TestConvertMySQLOracle(t *testing.T) {
	data, err := os.ReadFile("testdata/mysql-8.4.11-codec.json")
	require.NoError(t, err)
	var fixture struct {
		Version string `json:"version"`
		Cases   []struct {
			Name     string `json:"name"`
			Src, Dst collation.Charset
			Policy   Policy
			Input    string   `json:"input_hex"`
			Output   *string  `json:"output_hex"`
			Query    string   `json:"query_hex"`
			Packets  []string `json:"response_payloads_hex"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.Equal(t, "8.4.11", fixture.Version)
	require.NotEmpty(t, fixture.Cases)
	mp := newTestPool(t)
	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			input, err := hex.DecodeString(tc.Input)
			require.NoError(t, err)
			query := append(append([]byte("SELECT '"), input...), '\'')
			if tc.Policy == ConvertUsing {
				if tc.Src == collation.CharsetBinary {
					query = []byte("SELECT CONVERT(_binary x'" + tc.Input + "' USING " + tc.Dst.Name() + ")")
				} else {
					query = append([]byte("SELECT CONVERT(_"+tc.Src.Name()+"'"), input...)
					query = append(query, []byte("' USING "+tc.Dst.Name()+")")...)
				}
			}
			require.Equal(t, hex.EncodeToString(query), tc.Query, "oracle request pairing")
			require.GreaterOrEqual(t, len(tc.Packets), 5)
			row, err := hex.DecodeString(tc.Packets[len(tc.Packets)-2])
			require.NoError(t, err)
			if tc.Output == nil {
				require.Equal(t, []byte{251}, row, "oracle NULL packet")
			} else {
				expected, err := hex.DecodeString(*tc.Output)
				require.NoError(t, err)
				require.Equal(t, append([]byte{byte(len(expected))}, expected...), row, "oracle row pairing")
			}
			output, owned, null, err := Convert(context.Background(), mp, tc.Src, tc.Dst, tc.Policy, input, int64(len(input)))
			require.NoError(t, err)
			if owned {
				defer mp.Free(output)
			}
			require.Equal(t, tc.Output == nil, null)
			if tc.Output != nil {
				require.Equal(t, *tc.Output, hex.EncodeToString(output))
			}
		})
		require.Zero(t, mp.CurrNB())
	}
}

func BenchmarkConvert(b *testing.B) {
	for _, tc := range []struct {
		name     string
		src, dst collation.Charset
		policy   Policy
		input    []byte
	}{
		{"identity", collation.CharsetUTF8MB4, collation.CharsetUTF8MB4, Identity, bytes.Repeat([]byte("a"), 4096)},
		{"validate-convert", collation.CharsetBinary, collation.CharsetUTF8MB4, ConvertUsing, bytes.Repeat([]byte("a"), 4096)},
		{"ascii-result", collation.CharsetUTF8MB4, collation.CharsetASCII, Result, bytes.Repeat([]byte("é"), 2048)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			mp := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(mp)
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.input)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				output, owned, _, err := Convert(context.Background(), mp, tc.src, tc.dst, tc.policy, tc.input, int64(len(tc.input)))
				if err != nil {
					b.Fatal(err)
				}
				if owned {
					mp.Free(output)
				}
			}
		})
	}
}
