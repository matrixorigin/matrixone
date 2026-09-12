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
	"encoding/hex"
	"math"
	"net"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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
