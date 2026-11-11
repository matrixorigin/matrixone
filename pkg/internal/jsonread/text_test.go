// Copyright 2026 Matrix Origin
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

package jsonread

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/stretchr/testify/require"
)

func TestCopyTextPreservesFormattingAndReservation(t *testing.T) {
	for _, input := range []string{`""`, `"a\\n猫"`, `123`, `1.25`, `true`, `[1,"x",null]`, `{"a":"\\u0000"}`} {
		t.Run(input, func(t *testing.T) {
			value, err := bytejson.ParseFromString(input)
			require.NoError(t, err)
			want, err := value.Unquote()
			require.NoError(t, err)
			var live uint64
			reserve := func(n uint64) (func(), error) {
				live += n
				return func() { live -= n }, nil
			}
			got, release, err := CopyText(value, false, reserve)
			require.NoError(t, err)
			require.NotNil(t, release)
			defer func() {
				if release != nil {
					release()
				}
			}()
			require.Equal(t, want, got)
			expectedCharge := uint64(len(got))
			if value.Type == bytejson.TpCodeInt64 || value.Type == bytejson.TpCodeUint64 || value.Type == bytejson.TpCodeFloat64 || value.Type == bytejson.TpCodeLiteral {
				expectedCharge = 128
			}
			require.Equal(t, expectedCharge, live)
			release()
			release = nil
			require.Zero(t, live)
		})
	}
}

func TestDirectTextMatchesUnquote(t *testing.T) {
	for _, input := range []string{`""`, `"a\\n猫"`, `-123`, `18446744073709551615`, `1.25`, `1e-20`, `true`, `false`} {
		t.Run(input, func(t *testing.T) {
			value, err := bytejson.ParseFromString(input)
			require.NoError(t, err)
			for _, legacy := range []bool{false, true} {
				var scratch [128]byte
				got, ok, err := DirectText(value, legacy, 512, scratch[:0])
				require.NoError(t, err)
				require.True(t, ok)
				var want string
				if legacy {
					want, err = value.UnquoteWithLegacyFloat()
				} else {
					want, err = value.Unquote()
				}
				require.NoError(t, err)
				require.Equal(t, want, string(got))
			}
		})
	}
	value, err := bytejson.ParseFromString(`"12345"`)
	require.NoError(t, err)
	_, ok, err := DirectText(value, false, 4, nil)
	require.NoError(t, err)
	require.False(t, ok, "values beyond the byte bound require the normal width check")
	value, err = bytejson.ParseFromString(`[1,2]`)
	require.NoError(t, err)
	_, ok, err = DirectText(value, false, -1, nil)
	require.NoError(t, err)
	require.False(t, ok, "composite formatting must retain its admitted backing")
}

func TestCopyTextReleasesPartialAdmission(t *testing.T) {
	value, err := bytejson.ParseFromString(`[1,2]`)
	require.NoError(t, err)
	want := errors.New("memory denied")
	for failAt := 1; failAt <= 2; failAt++ {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			calls := 0
			var live uint64
			reserve := func(n uint64) (func(), error) {
				calls++
				live += n
				release := func() { live -= n }
				if calls == failAt {
					return release, want
				}
				return release, nil
			}
			_, release, err := CopyText(value, false, reserve)
			require.ErrorIs(t, err, want)
			require.False(t, IsConversionError(err))
			require.Nil(t, release)
			require.Zero(t, live)
		})
	}
}

func TestCopyTextBinaryDecimalAndTemporalValues(t *testing.T) {
	for _, test := range []struct {
		name string
		typ  bytejson.TpCode
		raw  []byte
		want string
	}{
		{"decimal", bytejson.TpCodeDecimal, []byte("-12.3400"), "-12.3400"},
		{"blob", bytejson.TpCodeBlob, []byte{0, 0xff}, string([]byte{0, 0xff})},
		{"opaque", bytejson.TpCodeOpaque, []byte{0, 0xff}, "AP8="},
		{"bit", bytejson.TpCodeBit, []byte{0, 0xff}, "AP8="},
		{"date", bytejson.TpCodeDate, []byte(`"2026-01-01"`), "2026-01-01"},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := append([]byte{byte(len(test.raw))}, test.raw...)
			value := bytejson.ByteJson{Type: test.typ, Data: data}
			for _, legacy := range []bool{false, true} {
				var live uint64
				reserve := func(n uint64) (func(), error) {
					live += n
					return func() { live -= n }, nil
				}
				text, release, err := CopyText(value, legacy, reserve)
				require.NoError(t, err)
				require.Equal(t, test.want, text)
				require.NotNil(t, release)
				release()
				require.Zero(t, live)
			}
		})
	}
}

func TestCopyTextDistinguishesConversionErrors(t *testing.T) {
	raw := []byte(`"\uZZZZ"`)
	data := append([]byte{byte(len(raw))}, raw...)
	value := bytejson.ByteJson{Type: bytejson.TpCodeDate, Data: data}
	var live uint64
	reserve := func(n uint64) (func(), error) {
		live += n
		return func() { live -= n }, nil
	}
	_, release, err := CopyText(value, false, reserve)
	require.Error(t, err)
	require.True(t, IsConversionError(err))
	require.Nil(t, release)
	require.Zero(t, live)
}
