// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource

import (
	"encoding/binary"
	"testing"
)

func FuzzReducer(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		var values [8]uint64
		for i := range values {
			start := i * 8
			if start >= len(data) {
				break
			}
			var word [8]byte
			copy(word[:], data[start:min(start+8, len(data))])
			values[i] = binary.LittleEndian.Uint64(word[:])
		}
		left := Usage{
			ExclusiveActiveNS: values[0],
			S3ReadBytes:       values[1],
			ClientEgressBytes: values[2],
		}
		right := Usage{
			ExclusiveActiveNS: values[3],
			S3ReadBytes:       values[4],
			ClientEgressBytes: values[5],
		}
		leftFirst, rightFirst := left, right
		leftFlags := MergeUsage(&leftFirst, right)
		rightFlags := MergeUsage(&rightFirst, left)
		if leftFirst != rightFirst || leftFlags != rightFlags {
			t.Fatalf("merge is order-dependent: left=%+v/%v right=%+v/%v",
				leftFirst, leftFlags, rightFirst, rightFlags)
		}
	})
}

func FuzzAttemptLifecycle(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3})
	f.Fuzz(func(t *testing.T, operations []byte) {
		attempt := NewAttempt()
		for _, operation := range operations {
			switch operation & 3 {
			case 0:
				attempt.AddLocal(Delta{Usage: Usage{ExclusiveActiveNS: uint64(operation)}})
			case 1:
				attempt.BeginClosing()
			case 2:
				attempt.Seal(uint64(operation), OutcomeError)
			case 3:
				_ = attempt.State()
			}
		}
		first := attempt.Seal(100, OutcomeSuccess)
		if second := attempt.Seal(200, OutcomePanic); second != first {
			t.Fatalf("seal is not idempotent: first=%+v second=%+v", first, second)
		}
		if attempt.State() != AttemptSealed {
			t.Fatalf("terminal state is %v", attempt.State())
		}
	})
}

func BenchmarkResourceMerge(b *testing.B) {
	delta := Usage{
		ExclusiveActiveNS: 10,
		S3ReadBytes:       20,
		S3WriteBytes:      30,
		ClientEgressBytes: 40,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var total Usage
		MergeUsage(&total, delta)
	}
}

func BenchmarkResourceAttemptTerminal(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		attempt := NewAttempt()
		attempt.AddLocal(Delta{Usage: Usage{ExclusiveActiveNS: 1}})
		attempt.BeginClosing()
		attempt.Seal(1, OutcomeSuccess)
	}
}
