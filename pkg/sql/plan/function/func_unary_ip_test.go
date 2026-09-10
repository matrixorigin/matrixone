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
	"testing"
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
		{name: "mapped address", hex: "00000000000000000000ffffc0000201", want: "192.0.2.1", ok: true},
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
