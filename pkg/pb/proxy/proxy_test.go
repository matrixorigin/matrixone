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
	"strings"
	"testing"
)

func TestExtraInfoDecodeLengthBoundary(t *testing.T) {
	for _, prefix := range [][]byte{{0xfe, 0xff}, {0xff, 0xff}} {
		var info ExtraInfo
		err := info.Decode(bufio.NewReader(bytes.NewReader(prefix)))
		if err != io.ErrUnexpectedEOF {
			t.Fatalf("prefix %x: got %v, want unexpected EOF", prefix, err)
		}
	}
}

func TestExtraInfoEncodeLengthBoundary(t *testing.T) {
	maxSize := ExtraInfo{ClientAddr: strings.Repeat("x", 0xffff-4)}
	wire, err := maxSize.Encode()
	if err != nil {
		t.Fatalf("encode max-size extra info: %v", err)
	}
	if len(wire) != 0xffff+2 {
		t.Fatalf("wire length = %d, want %d", len(wire), 0xffff+2)
	}
	var decoded ExtraInfo
	if err := decoded.Decode(bufio.NewReader(bytes.NewReader(wire))); err != nil {
		t.Fatalf("decode max-size extra info: %v", err)
	}
	if decoded.ClientAddr != maxSize.ClientAddr {
		t.Fatal("max-size extra info did not round-trip")
	}

	oversized := ExtraInfo{ClientAddr: strings.Repeat("x", 0x10000)}
	if _, err := oversized.Encode(); err == nil {
		t.Fatal("expected oversized extra info to be rejected")
	}
}
