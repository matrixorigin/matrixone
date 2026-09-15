// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"net"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestInet6NtoaString(t *testing.T) {
	tests := []struct {
		name string
		hex  string
		want string
		ok   bool
	}{
		{name: "compatible dotted loopback", hex: "0000000000000000000000007f000001", want: "::127.0.0.1", ok: true},
		{name: "compatible dotted documentation address", hex: "000000000000000000000000c0000201", want: "::192.0.2.1", ok: true},
		{name: "compatible dotted max", hex: "000000000000000000000000ffffffff", want: "::255.255.255.255", ok: true},
		{name: "compatible low tail stays hexadecimal", hex: "00000000000000000000000000000001", want: "::1", ok: true},
		{name: "compatible second low tail stays hexadecimal", hex: "00000000000000000000000000000100", want: "::100", ok: true},
		{name: "compatible seventh hextet enables dotted tail", hex: "00000000000000000000000000010000", want: "::0.1.0.0", ok: true},
		{name: "mapped address", hex: "00000000000000000000ffffc0000201", want: "::ffff:192.0.2.1", ok: true},
		{name: "ordinary IPv6", hex: "20010db885a3000000008a2e03707334", want: "2001:db8:85a3::8a2e:370:7334", ok: true},
		{name: "all zero IPv6", hex: "00000000000000000000000000000000", want: "::", ok: true},
		{name: "IPv4", hex: "c0000201", want: "192.0.2.1", ok: true},
		{name: "invalid length", hex: "01", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input, err := hex.DecodeString(tt.hex)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := inet6NtoaString(input)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIPv4ParsingPolicies(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		maxDigits int
		want      [4]byte
		ok        bool
	}{
		{name: "strict leading zeroes", input: "010.000.005.009", maxDigits: 3, want: [4]byte{10, 0, 5, 9}, ok: true},
		{name: "four digit octet only allowed for inet aton", input: "0001.2.3.4", maxDigits: 0, want: [4]byte{1, 2, 3, 4}, ok: true},
		{name: "unbounded leading zeroes remain decimal", input: "0000000000000000000000000000000001.2.3.4", maxDigits: 0, want: [4]byte{1, 2, 3, 4}, ok: true},
		{name: "strict parser rejects four digit octet", input: "0001.2.3.4", maxDigits: 3},
		{name: "must have four components", input: "192.168.1", maxDigits: 0},
		{name: "reject overflow", input: "192.168.1.256", maxDigits: 0},
		{name: "reject sign", input: "+1.2.3.4", maxDigits: 0},
		{name: "reject whitespace", input: "1.2.3.4 ", maxDigits: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseIPv4DottedQuad(tt.input, tt.maxDigits)
			if ok != tt.ok || (ok && got != tt.want) {
				t.Fatalf("parseIPv4DottedQuad(%q, %d) = %v, %v; want %v, %v", tt.input, tt.maxDigits, got, ok, tt.want, tt.ok)
			}
		})
	}

	got, ok := inetAtonValue("0001.2.3.4")
	if !ok || got != 16909060 {
		t.Fatalf("inetAtonValue long zero-padded octet = %d, %v; want 16909060, true", got, ok)
	}
	if _, ok := parseIPv4DottedQuad("0001.2.3.4", 3); ok {
		t.Fatal("strict IPv4 consumer accepted a four-digit octet")
	}

	atonTests := []struct {
		name  string
		input string
		want  uint64
		ok    bool
	}{
		{name: "one part", input: "1", want: 1, ok: true},
		{name: "two parts", input: "127.1", want: 2130706433, ok: true},
		{name: "three parts", input: "10.1.2", want: 167837698, ok: true},
		{name: "three parts with leading zeroes", input: "010.001.002", want: 167837698, ok: true},
		{name: "short form with four digit zero padding", input: "0001.2.3", want: 16908291, ok: true},
		{name: "four parts", input: "192.168.1.1", want: 3232235777, ok: true},
		{name: "four parts with leading zeroes", input: "010.000.005.009", want: 167773449, ok: true},
		{name: "one part above byte", input: "256", ok: false},
		{name: "two part above byte", input: "127.256", ok: false},
		{name: "three part above byte", input: "10.1.256", ok: false},
		{name: "empty part", input: "10..1", ok: false},
		{name: "trailing dot", input: "10.1.", ok: false},
		{name: "too many parts", input: "1.2.3.4.5", ok: false},
		{name: "sign rejected", input: "+1.2", ok: false},
		{name: "whitespace rejected", input: "1.2 ", ok: false},
	}
	for _, tt := range atonTests {
		t.Run("inet_aton/"+tt.name, func(t *testing.T) {
			_, count, ok := parseIPv4AtonParts(tt.input)
			if ok != tt.ok {
				t.Fatalf("parseIPv4AtonParts(%q) ok = %v, want %v", tt.input, ok, tt.ok)
			}
			if !ok {
				return
			}
			got, ok := inetAtonValue(tt.input)
			if !ok || got != tt.want {
				t.Fatalf("inetAtonValue(%q) = %d, %v; want %d, true (parts=%d)", tt.input, got, ok, tt.want, count)
			}
		})
	}
	longZeroes := strings.Repeat("0", 1024) + "1"
	longGot, longOK := inetAtonValue(longZeroes)
	if !longOK || longGot != 1 {
		t.Fatalf("inetAtonValue(%q...) = %d, %v; want 1, true", longZeroes[:16], longGot, longOK)
	}
}

func TestInet6AtonAddressPreservesAddressFamily(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantHex  string
		wantSize int
		wantOK   bool
	}{
		{name: "plain IPv4 with leading zeroes", input: "010.000.005.009", wantHex: "0a000509", wantSize: net.IPv4len, wantOK: true},
		{name: "short IPv4 form remains strict", input: "127.1"},
		{name: "too many digits in plain IPv4", input: "0001.2.3.4"},
		{name: "mapped IPv6 remains sixteen bytes", input: "::ffff:192.168.1.1", wantHex: "00000000000000000000ffffc0a80101", wantSize: net.IPv6len, wantOK: true},
		{name: "mapped IPv6 with zero-padded tail", input: "::ffff:192.168.001.001", wantHex: "00000000000000000000ffffc0a80101", wantSize: net.IPv6len, wantOK: true},
		{name: "embedded four digit component rejected", input: "::ffff:0001.2.3.4"},
		{name: "zone rejected", input: "fe80::1%lo0"},
		{name: "overlong malformed input rejected", input: "2001:" + string(make([]byte, 64))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, size, ok := inet6AtonAddress(tt.input)
			if ok != tt.wantOK {
				t.Fatalf("inet6AtonAddress(%q) ok = %v, want %v", tt.input, ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if size != tt.wantSize || hex.EncodeToString(got[:size]) != tt.wantHex {
				t.Fatalf("inet6AtonAddress(%q) = %x (%d bytes), want %s (%d bytes)", tt.input, got[:size], size, tt.wantHex, tt.wantSize)
			}
		})
	}
}

func TestIsIPv4CompatBoundaries(t *testing.T) {
	tests := []struct {
		name string
		hex  string
		want bool
	}{
		{name: "unspecified", hex: "00000000000000000000000000000000"},
		{name: "loopback excluded", hex: "00000000000000000000000000000001"},
		{name: "first compatible payload", hex: "00000000000000000000000000000002", want: true},
		{name: "regular compatible payload", hex: "000000000000000000000000c0000201", want: true},
		{name: "mapped is not compatible", hex: "00000000000000000000ffffc0000201"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input, err := hex.DecodeString(tt.hex)
			if err != nil {
				t.Fatal(err)
			}
			if got := isIPv4Compat(net.IP(input)); got != tt.want {
				t.Fatalf("isIPv4Compat(%s) = %v, want %v", tt.hex, got, tt.want)
			}
		})
	}
}

func TestInetNtoaNumericRoundingAndRange(t *testing.T) {
	tests := []struct {
		name    string
		convert func() (string, error)
		want    string
		wantErr bool
	}{
		{name: "uint32 maximum", convert: func() (string, error) { return inetNtoaUnsigned(maxInetNtoaValue) }, want: "255.255.255.255"},
		{name: "uint64 does not wrap", convert: func() (string, error) { return inetNtoaUnsigned(maxInetNtoaValue + 1) }, wantErr: true},
		{name: "negative signed integer", convert: func() (string, error) { return inetNtoaSigned(-1) }, wantErr: true},
		{name: "signed integer does not wrap", convert: func() (string, error) { return inetNtoaSigned(int64(maxInetNtoaValue + 1)) }, wantErr: true},
		{name: "double ties to even down", convert: func() (string, error) { return inetNtoaReal(0.5) }, want: "0.0.0.0"},
		{name: "double ties to even up", convert: func() (string, error) { return inetNtoaReal(1.5) }, want: "0.0.0.2"},
		{name: "double odd tie down", convert: func() (string, error) { return inetNtoaReal(2.5) }, want: "0.0.0.2"},
		{name: "double rounds before range check", convert: func() (string, error) { return inetNtoaReal(4294967294.5) }, want: "255.255.255.254"},
		{name: "double rounded overflow", convert: func() (string, error) { return inetNtoaReal(4294967295.5) }, wantErr: true},
		{name: "negative half rounds to zero", convert: func() (string, error) { return inetNtoaReal(-0.5) }, want: "0.0.0.0"},
		{name: "nan rejected", convert: func() (string, error) { return inetNtoaReal(math.NaN()) }, wantErr: true},
		{name: "infinity rejected", convert: func() (string, error) { return inetNtoaReal(math.Inf(1)) }, wantErr: true},
		{name: "decimal half away at one half", convert: func() (string, error) { return inetNtoaDecimal64(5, 1) }, want: "0.0.0.1"},
		{name: "decimal half away at one and a half", convert: func() (string, error) { return inetNtoaDecimal64(15, 1) }, want: "0.0.0.2"},
		{name: "decimal half away at two and a half", convert: func() (string, error) { return inetNtoaDecimal64(25, 1) }, want: "0.0.0.3"},
		{name: "decimal negative fraction rounds to zero", convert: func() (string, error) { return inetNtoaDecimal64(types.Decimal64(4).Minus(), 1) }, want: "0.0.0.0"},
		{name: "decimal negative half rejected after rounding", convert: func() (string, error) { return inetNtoaDecimal64(types.Decimal64(5).Minus(), 1) }, wantErr: true},
		{name: "decimal maximum after rounding", convert: func() (string, error) { return inetNtoaDecimal64(42949672945, 1) }, want: "255.255.255.255"},
		{name: "decimal rounded overflow", convert: func() (string, error) { return inetNtoaDecimal64(42949672955, 1) }, wantErr: true},
		{name: "decimal128 exact scaling", convert: func() (string, error) { return inetNtoaDecimal128(types.Decimal128FromInt64(25), 1) }, want: "0.0.0.3"},
		{name: "decimal256 exact scaling", convert: func() (string, error) { return inetNtoaDecimal256(types.Decimal256FromInt64(25), 1) }, want: "0.0.0.3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.convert()
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIPFunctionRegisteredExecutors(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	makeInput := func(typ types.Type, values any, flags []bool, constant bool) *vector.Vector {
		var nsp *nulls.Nulls
		if len(flags) > 0 {
			nsp = nulls.NewWithSize(len(flags))
			for i, isNull := range flags {
				if isNull {
					nsp.Add(uint64(i))
				}
			}
		}
		input := newVectorByType(mp, typ, values, nsp)
		if constant {
			input.SetClass(vector.CONSTANT)
		}
		return input
	}
	run := func(t *testing.T, name string, typ types.Type, values any, flags []bool, length int, constant bool) *vector.Vector {
		t.Helper()
		resolved, err := GetFunctionByName(proc.Ctx, name, []types.Type{typ})
		require.NoError(t, err)
		input := makeInput(typ, values, flags, constant)
		t.Cleanup(func() {
			input.Free(mp)
		})
		out, err := RunFunctionDirectly(proc, resolved.GetEncodedOverloadID(), []*vector.Vector{input}, length)
		require.NoError(t, err)
		t.Cleanup(func() {
			out.Free(mp)
		})
		return out
	}
	assertStrings := func(t *testing.T, out *vector.Vector, want []string, wantNull []bool) {
		t.Helper()
		param := vector.GenerateFunctionStrParameter(out)
		for i, expected := range want {
			got, isNull := param.GetStrValue(uint64(i))
			require.Equal(t, wantNull[i], isNull, "row %d", i)
			if !isNull {
				require.Equal(t, expected, string(got), "row %d", i)
			}
		}
	}

	t.Run("inet_aton vector and null", func(t *testing.T) {
		out := run(t, "inet_aton", types.T_varchar.ToType(),
			[]string{"127.0.0.1", "127.1", "10.1.2", "1", "010.000.005.009", "bad", ""},
			[]bool{false, false, false, false, false, false, true}, 7, false)
		require.Equal(t, []uint64{2130706433, 2130706433, 167837698, 1, 167773449, 0, 0}, vector.MustFixedColWithTypeCheck[uint64](out))
		require.True(t, out.IsNull(5))
		require.True(t, out.IsNull(6))
	})

	t.Run("inet_aton constant", func(t *testing.T) {
		out := run(t, "inet_aton", types.T_varchar.ToType(), []string{"127.0.0.1"}, nil, 3, true)
		require.True(t, out.IsConst())
		require.Equal(t, []uint64{2130706433}, vector.MustFixedColWithTypeCheck[uint64](out))
	})

	t.Run("inet6_aton preserves mapped family", func(t *testing.T) {
		out := run(t, "inet6_aton", types.T_varchar.ToType(),
			[]string{"::ffff:192.0.2.1", "010.000.005.009", "bad", ""},
			[]bool{false, false, false, true}, 4, false)
		param := vector.GenerateFunctionStrParameter(out)
		want := [][]byte{
			{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 0, 2, 1},
			{10, 0, 5, 9}, nil, nil,
		}
		for i, expected := range want {
			got, isNull := param.GetStrValue(uint64(i))
			require.Equal(t, expected == nil, isNull, "row %d", i)
			if expected != nil {
				require.True(t, bytes.Equal(expected, got), "row %d: got %x want %x", i, got, expected)
			}
		}
	})

	t.Run("inet6_aton constant", func(t *testing.T) {
		out := run(t, "inet6_aton", types.T_varchar.ToType(), []string{"::1"}, nil, 2, true)
		param := vector.GenerateFunctionStrParameter(out)
		for i := 0; i < 2; i++ {
			got, isNull := param.GetStrValue(uint64(i))
			require.False(t, isNull)
			require.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, got)
		}
	})

	t.Run("inet6_ntoa mapped and invalid", func(t *testing.T) {
		mapped := string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 0, 2, 1})
		out := run(t, "inet6_ntoa", types.T_varbinary.ToType(),
			[]string{mapped, string([]byte{192, 0, 2, 1}), "x", ""},
			[]bool{false, false, false, true}, 4, false)
		assertStrings(t, out, []string{"::ffff:192.0.2.1", "192.0.2.1", "", ""}, []bool{false, false, true, true})
	})

	t.Run("is_ipv4 leading zero policy", func(t *testing.T) {
		out := run(t, "is_ipv4", types.T_varchar.ToType(),
			[]string{"010.000.005.009", "0001.2.3.4", "127.1", "192.0.2.1", ""},
			[]bool{false, false, false, false, true}, 5, false)
		require.Equal(t, []int32{1, 0, 0, 1, 0}, vector.MustFixedColWithTypeCheck[int32](out))
		require.True(t, out.IsNull(4))
	})

	t.Run("is_ipv6 accepts mapped text", func(t *testing.T) {
		out := run(t, "is_ipv6", types.T_varchar.ToType(),
			[]string{"::ffff:192.0.2.1", "::192.0.2.1", "192.0.2.1", "bad", ""},
			[]bool{false, false, false, false, true}, 5, false)
		require.Equal(t, []int32{1, 1, 0, 0, 0}, vector.MustFixedColWithTypeCheck[int32](out))
		require.True(t, out.IsNull(4))
	})

	t.Run("is_ipv4_compat reserved boundaries", func(t *testing.T) {
		out := run(t, "is_ipv4_compat", types.T_varbinary.ToType(), []string{
			string(make([]byte, 16)),
			string(append(make([]byte, 15), 1)),
			string(append(make([]byte, 15), 2)),
			string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 0, 2, 1}),
			"x", "",
		}, []bool{false, false, false, false, false, true}, 6, false)
		require.Equal(t, []int32{0, 0, 1, 0, 0, 0}, vector.MustFixedColWithTypeCheck[int32](out))
		require.True(t, out.IsNull(5))
	})

	for _, tc := range []struct {
		name  string
		typ   types.Type
		value any
		flags []bool
		want  []string
		nulls []bool
	}{
		{name: "float64", typ: types.T_float64.ToType(), value: []float64{1.5, 4294967295.5, math.NaN(), 16909060}, want: []string{"0.0.0.2", "", "", "1.2.3.4"}, nulls: []bool{false, true, true, false}},
		{name: "float32", typ: types.T_float32.ToType(), value: []float32{1.5, -1}, want: []string{"0.0.0.2", ""}, nulls: []bool{false, true}},
		{name: "decimal64", typ: types.New(types.T_decimal64, 18, 1), value: []types.Decimal64{5, 15, 42949672945, 42949672955}, want: []string{"0.0.0.1", "0.0.0.2", "255.255.255.255", ""}, nulls: []bool{false, false, false, true}},
		{name: "decimal128", typ: types.New(types.T_decimal128, 38, 1), value: []types.Decimal128{types.Decimal128FromInt64(25)}, want: []string{"0.0.0.3"}, nulls: []bool{false}},
		{name: "decimal256", typ: types.New(types.T_decimal256, 76, 1), value: []types.Decimal256{types.Decimal256FromInt64(25)}, want: []string{"0.0.0.3"}, nulls: []bool{false}},
	} {
		t.Run("inet_ntoa/"+tc.name, func(t *testing.T) {
			out := run(t, "inet_ntoa", tc.typ, tc.value, tc.flags, len(tc.want), false)
			assertStrings(t, out, tc.want, tc.nulls)
		})
	}

	t.Run("inet_ntoa dynamic source families", func(t *testing.T) {
		out := run(t, "inet_ntoa", types.T_varchar.ToType(), []string{"1.6", "4294967295.9", "-1", "abc"}, nil, 4, false)
		assertStrings(t, out, []string{"0.0.0.1", "255.255.255.255", "", "0.0.0.0"}, []bool{false, false, true, false})

		out = run(t, "inet_ntoa", types.T_bool.ToType(), []bool{true, false}, nil, 2, false)
		assertStrings(t, out, []string{"0.0.0.1", "0.0.0.0"}, []bool{false, false})

		date, err := types.ParseDateCast("2024-01-02")
		require.NoError(t, err)
		out = run(t, "inet_ntoa", types.T_date.ToType(), []types.Date{date}, nil, 1, false)
		assertStrings(t, out, []string{"1.52.214.230"}, []bool{false})

		clock, err := types.ParseTime("00:00:02", 0)
		require.NoError(t, err)
		out = run(t, "inet_ntoa", types.T_time.ToType(), []types.Time{clock}, nil, 1, false)
		assertStrings(t, out, []string{"0.0.0.2"}, []bool{false})

		encodeJSON := func(value any) string {
			bj, err := bytejson.CreateByteJSON(value)
			require.NoError(t, err)
			encoded, err := types.EncodeJson(bj)
			require.NoError(t, err)
			return string(encoded)
		}
		out = run(t, "inet_ntoa", types.T_json.ToType(), []string{encodeJSON(float64(1.6)), encodeJSON(int64(2)), encodeJSON(true), ""}, []bool{false, false, false, true}, 4, false)
		assertStrings(t, out, []string{"0.0.0.2", "0.0.0.2", "", ""}, []bool{false, false, true, true})

		timeValues := []types.Time{
			types.TimeFromClock(false, 0, 0, 0, 499999),
			types.TimeFromClock(false, 0, 0, 0, 500000),
			types.TimeFromClock(false, 0, 0, 1, 500000),
			types.TimeFromClock(true, 0, 0, 0, 500000),
		}
		out = run(t, "inet_ntoa", types.New(types.T_time, 0, 6), timeValues, nil, len(timeValues), false)
		assertStrings(t, out,
			[]string{"0.0.0.0", "0.0.0.1", "0.0.0.2", ""},
			[]bool{false, false, false, true})

		encodeDecimalJSON := func(value string) string {
			data := make([]byte, binary.MaxVarintLen64+len(value))
			n := binary.PutUvarint(data, uint64(len(value)))
			copy(data[n:], value)
			bj := bytejson.ByteJson{Type: bytejson.TpCodeDecimal, Data: data[:n+len(value)]}
			encoded, err := types.EncodeJson(bj)
			require.NoError(t, err)
			return string(encoded)
		}
		out = run(t, "inet_ntoa", types.T_json.ToType(), []string{
			encodeDecimalJSON("4294967295.4999999"),
			encodeDecimalJSON("4294967295.5"),
			encodeDecimalJSON("0.5"),
			encodeDecimalJSON("-0.4999999"),
			encodeDecimalJSON("-0.5"),
		}, nil, 5, false)
		assertStrings(t, out,
			[]string{"255.255.255.255", "", "0.0.0.1", "0.0.0.0", ""},
			[]bool{false, true, false, false, true})
	})
}

func TestIPFunctionsSelectionMasksInvalidRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selected := &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}}

	tests := []struct {
		name   string
		input  FunctionTestInput
		result FunctionTestResult
		fn     fEvalFn
	}{
		{
			name:  "inet_aton",
			input: NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-an-ip", "127.0.0.1"}, nil),
			result: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{0, 2130706433}, []bool{true, false}),
			fn: InetAton,
		},
		{
			name:  "inet_ntoa",
			input: NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, 16909060}, nil),
			result: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"", "1.2.3.4"}, []bool{true, false}),
			fn: InetNtoa,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			caseRun := NewFunctionTestCase(proc, []FunctionTestInput{tc.input}, tc.result, tc.fn).
				WithSelectList(selected)
			ok, info := caseRun.Run()
			require.True(t, ok, info)
		})
	}
}
