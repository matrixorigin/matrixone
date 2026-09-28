// Copyright 2021 - 2026 Matrix Origin
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

package proxy

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func TestExtraInfoDecodeLengthBoundary(t *testing.T) {
	for _, prefix := range [][]byte{{0xfe, 0xff}, {0xff, 0xff}} {
		var info ExtraInfo
		if err := info.Decode(bufio.NewReader(bytes.NewReader(prefix))); err != io.ErrUnexpectedEOF {
			t.Fatalf("prefix %x: got %v, want unexpected EOF", prefix, err)
		}
	}

	wire, err := (&ExtraInfo{Salt: []byte("12345678901234567890")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	var decoded ExtraInfo
	if err := decoded.Decode(bufio.NewReader(bytes.NewReader(wire))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Salt, []byte("12345678901234567890")) {
		t.Fatalf("decoded salt = %q", decoded.Salt)
	}
}
