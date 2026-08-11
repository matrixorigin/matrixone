// Copyright 2024 - 2022 Matrix Origin
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

package types

import (
	"errors"
	"testing"
)

func TestPacker(t *testing.T) {
	packer := NewPacker()
	defer packer.Close()
	for i := 0; i < 65536; i++ {
		packer.EncodeInt64(int64(i))
	}
	bs := packer.Bytes()
	if len(bs) != 261887 {
		t.Fatalf("got %v", len(bs))
	}
}

func TestClosedPackerIsOK(t *testing.T) {
	packer := NewPacker()
	packer.Close()
	for i := 0; i < 65536; i++ {
		packer.EncodeInt64(int64(i))
	}
	bs := packer.Bytes()
	if len(bs) != 261887 {
		t.Fatalf("got %v", len(bs))
	}
	packer.Close()
}

func TestFixedBufferPackerNeverAllocatesPastCapacity(t *testing.T) {
	storage := make([]byte, 0, 3)
	packer := NewPackerWithFixedBuffer(storage)
	packer.EncodeBool(true)
	packer.EncodeNull()
	if err := packer.Err(); err != nil {
		t.Fatal(err)
	}
	if len(packer.GetBuf()) != 2 {
		t.Fatalf("encoded length = %d", len(packer.GetBuf()))
	}
	packer.EncodeInt64(42)
	if !errors.Is(packer.Err(), ErrPackerCapacity) {
		t.Fatalf("overflow error = %v", packer.Err())
	}
	if len(packer.GetBuf()) > cap(storage) {
		t.Fatal("fixed packer exceeded caller-owned storage")
	}
	packer.Reset()
	packer.EncodeBool(true)
	if err := packer.Err(); err != nil {
		t.Fatalf("reset fixed packer error = %v", err)
	}
}

func BenchmarkPacker(b *testing.B) {
	for i := 0; i < b.N; i++ {
		packer := NewPacker()
		packer.EncodeInt64(42)
		packer.Close()
	}
}

func BenchmarkPackerEncode(b *testing.B) {
	for _, fixed := range []bool{false, true} {
		mode := "allocator-backed"
		if fixed {
			mode = "fixed-buffer"
		}
		b.Run(mode, func(b *testing.B) {
			var packer *Packer
			if fixed {
				packer = NewPackerWithFixedBuffer(make([]byte, 16))
			} else {
				packer = NewPacker()
				defer packer.Close()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				packer.EncodeInt64(42)
				packer.Reset()
			}
			b.StopTimer()
			if err := packer.Err(); err != nil {
				b.Fatal(err)
			}
		})
	}
}
