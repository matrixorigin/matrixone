// Copyright 2021 Matrix Origin
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

package lengthutf8

import (
	"bytes"
	"fmt"
	"testing"
)

// This independent oracle preserves the historical step rule for malformed
// sequences; utf8.RuneCount would change the existing contract.
func scanReference(s []byte) uint64 {
	var count uint64
	for i := 0; i < len(s); count++ {
		b := s[i]
		step := 1
		if b >= 0xc0 && b <= 0xdf {
			step = 2
		} else if b >= 0xe0 && b <= 0xef {
			step = 3
		} else if b >= 0xf0 && b <= 0xf7 {
			step = 4
		}
		i += step
	}
	return count
}

func TestScanFollowupUTF8(t *testing.T) {
	for _, n := range []int{0, 1, 7, 8, 9, 15, 16, 17, 23, 24, 31, 32, 33, 63, 64, 65, 129} {
		for _, seed := range [][]byte{{'a'}, {0}, {0x7f}, {0x80}, {0xff}, []byte("中文"), []byte("😀"), {'a', 0xc0, 0x80, 'b'}, {0xf0, 0x9f}} {
			s := bytes.Repeat(seed, (n+len(seed)-1)/len(seed))
			s = s[:n]
			for offset := 0; offset < 8; offset++ {
				buf := bytes.Repeat([]byte{'x'}, offset+n+8)
				copy(buf[offset:], s)
				view := buf[offset : offset+n]
				before := bytes.Clone(buf)
				if got, want := CountUTF8CodePoints(view), scanReference(view); got != want {
					t.Fatalf("n=%d offset=%d data=%x: got%d want%d", n, offset, view, got, want)
				}
				if !bytes.Equal(before, buf) {
					t.Fatal("modified source bytes")
				}
			}
		}
	}
	// Every single byte at each word boundary and tail, including truncated
	// leading bytes, must retain the scalar rule.
	for n := 0; n <= 40; n++ {
		for at := 0; at < n; at++ {
			for value := 0; value < 256; value++ {
				s := bytes.Repeat([]byte{'a'}, n)
				s[at] = byte(value)
				if got, want := CountUTF8CodePoints(s), scanReference(s); got != want {
					t.Fatalf("n=%d at=%d byte=%x: got%d want%d", n, at, value, got, want)
				}
			}
		}
	}
}

var scanCount uint64

func BenchmarkScanFollowupUTF8(b *testing.B) {
	patterns := []struct {
		name string
		data []byte
	}{
		{"ascii", []byte("abcdefgh")}, {"nul", []byte{0}}, {"chinese", []byte("中文测试")}, {"emoji", []byte("😀🚀")},
		{"mixed", []byte("abc中文😀def")}, {"ascii-prefix", append(bytes.Repeat([]byte{'a'}, 32), []byte("中文")...)},
		{"nonascii-prefix", append([]byte("中文"), bytes.Repeat([]byte{'a'}, 32)...)}, {"continuation", []byte{0x80}}, {"invalid", []byte{0xff, 0xc0, 0x80, 0xe0, 0x80, 'a'}},
	}
	for _, p := range patterns {
		for _, n := range []int{1, 8, 15, 16, 17, 23, 24, 31, 32, 33, 64, 2048} {
			b.Run(fmt.Sprintf("%s/%d", p.name, n), func(b *testing.B) {
				s := bytes.Repeat(p.data, (n+len(p.data)-1)/len(p.data))
				s = s[:n]
				b.ReportAllocs()
				b.SetBytes(int64(n))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					scanCount = CountUTF8CodePoints(s)
				}
			})
		}
	}
}
