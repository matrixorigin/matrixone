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
	f.Add(uint64(1), []byte{0, 1, 2, 3, 4, 5})
	f.Add(uint64(9), []byte{5, 5, 4, 3, 2, 1, 0})
	f.Fuzz(func(t *testing.T, generation uint64, operations []byte) {
		attempt := NewAttempt(generation, 4, 2)
		for _, operation := range operations {
			slot := int(operation>>4) % 4
			switch operation & 7 {
			case 0:
				attempt.MarkFragmentDispatched(slot)
			case 1:
				attempt.PublishFragment(generation, slot, Delta{Usage: Usage{ExclusiveActiveNS: uint64(operation)}})
			case 2:
				attempt.PublishFragment(generation+1, slot, Delta{})
			case 3:
				attempt.MarkFragmentSendFailed(slot)
			case 4:
				attempt.MarkMemoryDomainDispatched(slot % 2)
			case 5:
				attempt.PublishMemoryDomain(generation, slot%2, MemoryDomainSummary{})
			case 6:
				attempt.BeginClosing()
			case 7:
				attempt.Seal(uint64(operation), OutcomeError)
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
		attempt := NewAttempt(1, 1, 0)
		attempt.MarkFragmentDispatched(0)
		attempt.BeginClosing()
		attempt.PublishFragment(1, 0, Delta{Usage: Usage{ExclusiveActiveNS: 1}})
		attempt.Seal(1, OutcomeSuccess)
	}
}
