// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package types

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFrozenTupleIndependentOracle(t *testing.T) {
	f, err := os.Open("../../common/collation/testdata/mysql_8_0_45_extended.json.gz")
	require.NoError(t, err)
	defer f.Close()
	z, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer z.Close()
	var o struct {
		Values []string
		Order  map[string][]byte
	}
	require.NoError(t, json.NewDecoder(z).Decode(&o))
	for name, charset := range map[string]uint8{"utf8mb4_general_ci": CharsetUTF8, "utf8mb4_bin": CharsetUTF8MB4Bin, "utf8mb4_0900_ai_ci": CharsetUTF8MB40900AI, "utf8mb4_0900_bin": CharsetUTF8MB40900Bin} {
		t.Run(name, func(t *testing.T) {
			part, err := ResolveStringKeyPart(NewWithCharset(T_varchar, 0, 0, charset), PADSpaceKeyV1)
			require.NoError(t, err)
			p := NewPacker()
			defer p.Close()
			tuples := make([][]byte, len(o.Values))
			var scratch []byte
			for i, v := range o.Values {
				p.Reset()
				p.EncodeInt64(-42)
				start := len(p.GetBuf())
				scratch, err = part.Encode(p, scratch, []byte(v))
				require.NoError(t, err)
				end := len(p.GetBuf())
				p.EncodeInt64(9)
				tuples[i] = p.Bytes()
				decoded, n, err := part.Decode(p.GetBuf()[start:])
				require.NoError(t, err)
				require.Equal(t, end-start, n)
				require.True(t, decoded.Opaque)
				expected, err := part.Key(nil, []byte(v))
				require.NoError(t, err)
				require.Equal(t, len(expected), len(decoded.Bytes))
				require.True(t, bytes.Equal(expected, decoded.Bytes))
				p.Reset()
				p.EncodeStringType([]byte("overwrite"))
				require.True(t, bytes.Equal(expected, decoded.Bytes))
				// An exact capacity succeeds and one byte less fails without partial output.
				needed := end - start
				for _, capacity := range []int{needed - 1, needed, needed + 1} {
					q := NewPackerWithFixedBuffer(make([]byte, capacity+1))
					q.EncodeNull()
					before := q.Bytes()
					_, e := part.Encode(q, nil, []byte(v))
					if capacity < needed {
						require.ErrorIs(t, e, ErrPackerCapacity)
						require.Equal(t, before, q.GetBuf())
					} else {
						require.NoError(t, e)
						require.Equal(t, needed+1, len(q.GetBuf()))
					}
					q.Close()
				}
			}
			n := len(tuples)
			require.Len(t, o.Order[name], n*n)
			for i, a := range tuples {
				for j, b := range tuples {
					if got, want := bytes.Compare(a, b), int(o.Order[name][i*n+j])-1; got != want {
						t.Fatalf("pair %d/%d got %d want %d", i, j, got, want)
					}
				}
			}
		})
	}
}

func TestFrozenTupleGolden(t *testing.T) {
	for _, tc := range []struct {
		charset    uint8
		input, hex string
	}{
		{CharsetUTF8, "Alpha", "4601444f534b442200"},
		{CharsetUTF8, "alpha ", "4601444f534b442200"},
		{CharsetUTF8, "a \x00", "46014421012200"},
		{CharsetUTF8MB40900AI, "Alpha", "46011c471d771e0c1d181c4700"},
		{CharsetUTF8MB40900AI, "é", "46011caa00"},
		{CharsetUTF8MB40900AI, "e\u0301", "46011caa00"},
		{CharsetUTF8MB40900AI, "", "460100"},
		{CharsetUTF8MB40900Bin, "a\x00", "46016100ff00"},
		{CharsetUTF8MB40900Bin, "Alpha ", "4601416c7068612000"},
	} {
		part, err := ResolveStringKeyPart(NewWithCharset(T_varchar, 0, 0, tc.charset), PADSpaceKeyV1)
		require.NoError(t, err)
		p := NewPacker()
		_, err = part.Encode(p, nil, []byte(tc.input))
		require.NoError(t, err)
		require.Equal(t, tc.hex, hex.EncodeToString(p.GetBuf()))
		p.Close()
	}
}

func TestFrozenTupleRejectsTruncatedNativeWeight(t *testing.T) {
	part, err := ResolveStringKeyPart(NewWithCharset(T_varchar, 0, 0, CharsetUTF8MB40900AI), PADSpaceKeyV1)
	require.NoError(t, err)
	// Correct outer framing cannot make half of a uint16 UCA weight valid.
	_, _, err = part.Decode([]byte{0x46, 0x01, 0x1c, 0x00})
	require.Error(t, err)
}

func TestFrozenTupleTruncationAndCompositeSuffix(t *testing.T) {
	for _, charset := range []uint8{CharsetBinary, CharsetUTF8, CharsetUTF8MB4Bin, CharsetUTF8MB40900AI, CharsetUTF8MB40900Bin} {
		part, err := ResolveStringKeyPart(NewWithCharset(T_varchar, 0, 0, charset), PADSpaceKeyV1)
		require.NoError(t, err)
		p := NewPacker()
		_, err = part.Encode(p, nil, []byte("a\x00é😀"))
		require.NoError(t, err)
		full := p.Bytes()
		p.Close()
		for n := 0; n < len(full); n++ {
			_, consumed, e := part.Decode(full[:n])
			// A zero which is followed by FF in the complete encoding is a valid
			// terminator if viewed alone. Whole-tuple field counts/suffixes must detect
			// that separate truncation; a prefix decoder cannot guess missing bytes.
			if e == nil {
				require.Equal(t, n, consumed)
				require.Equal(t, byte(0), full[n-1])
				require.Equal(t, byte(0xff), full[n])
			}
		}
		var encoded [][]byte
		for _, suffix := range []int64{-1, 0, 1} {
			p := NewPacker()
			_, err = part.Encode(p, nil, []byte("a"))
			require.NoError(t, err)
			p.EncodeInt64(suffix)
			encoded = append(encoded, p.Bytes())
			p.Close()
		}
		require.Less(t, bytes.Compare(encoded[0], encoded[1]), 0)
		require.Less(t, bytes.Compare(encoded[1], encoded[2]), 0)
	}
}
