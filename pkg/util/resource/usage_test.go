// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource

import (
	"math"
	"testing"
	"unsafe"
)

func TestExclusiveActive(t *testing.T) {
	tests := []struct {
		name                 string
		wall, wait, child    uint64
		want                 uint64
		wantInvariantFailure bool
	}{
		{name: "active", wall: 100, wait: 20, child: 30, want: 50},
		{name: "all blocked", wall: 100, wait: 70, child: 30, want: 0},
		{name: "invalid subtraction", wall: 100, wait: 80, child: 30, wantInvariantFailure: true},
		{name: "addition overflow", wall: math.MaxUint64, wait: math.MaxUint64, child: 1, wantInvariantFailure: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, flags := ExclusiveActive(test.wall, test.wait, test.child)
			if got != test.want {
				t.Fatalf("active: got %d, want %d", got, test.want)
			}
			if hasInvariant := flags&QualityInvariantFailure != 0; hasInvariant != test.wantInvariantFailure {
				t.Fatalf("invariant flag: got %v, want %v", hasInvariant, test.wantInvariantFailure)
			}
		})
	}
}

func TestMergeUsageAssociativeForExactValues(t *testing.T) {
	a := Usage{ExclusiveActiveNS: 11, S3ReadBytes: 13}
	b := Usage{ExclusiveActiveNS: 17, S3ReadBytes: 19}
	c := Usage{ExclusiveActiveNS: 23, S3ReadBytes: 29}
	a.WaitNS[WaitLock] = 31
	b.WaitNS[WaitLock] = 37
	c.WaitNS[WaitLock] = 41

	left := a
	if flags := MergeUsage(&left, b); flags != 0 {
		t.Fatalf("unexpected flags: %v", flags)
	}
	if flags := MergeUsage(&left, c); flags != 0 {
		t.Fatalf("unexpected flags: %v", flags)
	}

	rightDelta := b
	if flags := MergeUsage(&rightDelta, c); flags != 0 {
		t.Fatalf("unexpected flags: %v", flags)
	}
	right := a
	if flags := MergeUsage(&right, rightDelta); flags != 0 {
		t.Fatalf("unexpected flags: %v", flags)
	}
	if left != right {
		t.Fatalf("merge is not associative: left=%+v right=%+v", left, right)
	}
}

func TestMergeUsageOverflowSaturates(t *testing.T) {
	dst := Usage{S3WriteBytes: math.MaxUint64}
	flags := MergeUsage(&dst, Usage{S3WriteBytes: 1})
	if dst.S3WriteBytes != math.MaxUint64 {
		t.Fatalf("overflow wrapped to %d", dst.S3WriteBytes)
	}
	if flags&QualityInvariantFailure == 0 {
		t.Fatal("overflow was not flagged")
	}
}

func TestLocalRecorder(t *testing.T) {
	var recorder LocalRecorder
	recorder.AddActiveInterval(100, 10, 20)
	recorder.AddWait(WaitLock, 10)
	recorder.AddS3Request(S3Get, 2)
	recorder.AddS3ReadBytes(1024)
	recorder.AddClientEgressBytes(128)
	recorder.AddSpillBytes(256)

	delta := recorder.Snapshot()
	if delta.Quality != 0 || delta.Usage.ExclusiveActiveNS != 70 ||
		delta.Usage.WaitNS[WaitLock] != 10 || delta.Usage.S3Requests[S3Get] != 2 ||
		delta.Usage.S3ReadBytes != 1024 || delta.Usage.ClientEgressBytes != 128 ||
		delta.Usage.SpillBytes != 256 {
		t.Fatalf("unexpected snapshot: %+v", delta)
	}
}

func TestHotPathStructureSizes(t *testing.T) {
	if got := unsafe.Sizeof(Usage{}); got > 160 {
		t.Fatalf("Usage is %d bytes, limit is 160", got)
	}
	if got := unsafe.Sizeof(LocalRecorder{}); got > 160 {
		t.Fatalf("LocalRecorder is %d bytes, limit is 160", got)
	}
	if got := unsafe.Sizeof(Delta{}); got > 192 {
		t.Fatalf("Delta is %d bytes, limit is 192", got)
	}
	if got := unsafe.Sizeof(MemoryDomainSummary{}); got > 40 {
		t.Fatalf("MemoryDomainSummary is %d bytes, limit is 40", got)
	}
}
