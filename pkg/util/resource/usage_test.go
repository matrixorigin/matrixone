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
	"encoding/json"
	"math"
	"reflect"
	"testing"
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

func TestAllocationAccountTotalsMergeAndFailureQuality(t *testing.T) {
	var valid AllocationAccountTotals
	if quality := valid.AddGeneration(10, 0, true); quality != 0 {
		t.Fatalf("valid generation quality = %v", quality)
	}
	if quality := valid.AddOwnerGeneration(1, 7, 0); quality != 0 {
		t.Fatalf("valid owner quality = %v", quality)
	}
	var failed AllocationAccountTotals
	quality := failed.AddGeneration(20, 5, false)
	if quality&QualityInvariantFailure == 0 ||
		quality&QualityNonZeroLiveAtSeal == 0 {
		t.Fatalf("failure quality = %v", quality)
	}
	quality = failed.AddOwnerGeneration(1, 12, 5)
	if quality&QualityInvariantFailure == 0 ||
		quality&QualityNonZeroLiveAtSeal == 0 {
		t.Fatalf("failure owner quality = %v", quality)
	}
	if quality := failed.AddOwnerGeneration(63, 2, 0); quality != 0 {
		t.Fatalf("unknown bounded owner quality = %v", quality)
	}

	quality = MergeAllocationAccountTotals(&valid, failed)
	if valid.GenerationCount != 2 || valid.ValidGenerationCount != 1 ||
		valid.FailedGenerationCount != 1 || valid.MaxGenerationPeak != 20 ||
		valid.SumGenerationPeak != 30 || valid.LiveBytesAtTerminal != 5 {
		t.Fatalf("merged allocation totals = %+v", valid)
	}
	if quality&QualityInvariantFailure == 0 ||
		quality&QualityNonZeroLiveAtSeal == 0 {
		t.Fatalf("merge quality = %v", quality)
	}
	owner, ok := valid.Owner(1)
	if !ok || owner.MaxGenerationPeak != 12 ||
		owner.SumGenerationPeak != 19 || owner.LiveBytesAtTerminal != 5 {
		t.Fatalf("merged owner totals = %+v, present=%v", owner, ok)
	}
	unknown, ok := valid.Owner(63)
	if !ok || unknown.MaxGenerationPeak != 2 ||
		unknown.SumGenerationPeak != 2 || unknown.LiveBytesAtTerminal != 0 {
		t.Fatalf("merged unknown owner totals = %+v, present=%v", unknown, ok)
	}
}

func TestAllocationAccountOwnerAttributionCoverage(t *testing.T) {
	var totals AllocationAccountTotals
	if !totals.OwnerAttributionCoversTotals() {
		t.Fatal("empty totals should need no owner evidence")
	}
	if quality := totals.AddGeneration(100, 0, true); quality != 0 {
		t.Fatalf("generation quality = %v", quality)
	}
	if totals.OwnerAttributionCoversTotals() {
		t.Fatal("owner-less nonzero totals reported complete")
	}
	if quality := totals.AddOwnerGeneration(1, 1, 0); quality != 0 {
		t.Fatalf("partial owner quality = %v", quality)
	}
	if totals.OwnerAttributionCoversTotals() {
		t.Fatal("partial owner totals reported complete")
	}
	if quality := totals.AddOwnerGeneration(2, 99, 0); quality != 0 {
		t.Fatalf("complete owner quality = %v", quality)
	}
	if !totals.OwnerAttributionCoversTotals() {
		t.Fatal("complete owner totals reported incomplete")
	}
}

func TestAllocationAccountTotalsSparseJSONRoundTrip(t *testing.T) {
	var source AllocationAccountTotals
	if quality := source.AddGeneration(20, 0, true); quality != 0 {
		t.Fatalf("generation quality = %v", quality)
	}
	if quality := source.AddOwnerGeneration(1, 10, 0); quality != 0 {
		t.Fatalf("owner 1 quality = %v", quality)
	}
	if quality := source.AddOwnerGeneration(63, 20, 0); quality != 0 {
		t.Fatalf("owner 63 quality = %v", quality)
	}

	data, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Owners []struct {
			Owner uint8
		}
	}
	if err = json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.Owners) != 2 || wire.Owners[0].Owner != 1 ||
		wire.Owners[1].Owner != 63 {
		t.Fatalf("non-sparse owner wire form: %s", data)
	}

	var decoded AllocationAccountTotals
	if err = json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.GenerationCount != source.GenerationCount ||
		decoded.MaxGenerationPeak != source.MaxGenerationPeak {
		t.Fatalf("decoded totals = %+v", decoded)
	}
	for _, ownerID := range []uint8{1, 63} {
		got, ok := decoded.Owner(ownerID)
		want, wantOK := source.Owner(ownerID)
		if ok != wantOK || got != want {
			t.Fatalf("owner %d: got %+v/%v want %+v/%v", ownerID, got, ok, want, wantOK)
		}
	}
}

func TestAllocationAccountTotalsJSONCompatibility(t *testing.T) {
	legacy := []byte(`{"GenerationCount":1,"ValidGenerationCount":1,"MaxGenerationPeak":7,"SumGenerationPeak":7}`)
	var totals AllocationAccountTotals
	if err := json.Unmarshal(legacy, &totals); err != nil {
		t.Fatal(err)
	}
	if _, ok := totals.Owner(1); ok || totals.Validate() != 0 {
		t.Fatalf("legacy summary gained owner state: %+v", totals)
	}

	for _, malformed := range [][]byte{
		[]byte(`{"Owners":[{"Owner":0}]}`),
		[]byte(`{"Owners":[{"Owner":64}]}`),
		[]byte(`{"Owners":[{"Owner":1},{"Owner":1}]}`),
	} {
		if err := json.Unmarshal(malformed, &totals); err == nil {
			t.Fatalf("accepted malformed owners: %s", malformed)
		}
	}
	dense := allocationAccountTotalsJSON{
		Owners: make([]allocationOwnerTotalsJSON, AllocationOwnerMaxID+1),
	}
	data, err := json.Marshal(dense)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &totals); err == nil {
		t.Fatal("accepted more than 63 owner entries")
	}
}

func TestAllocationOwnerTotalsRetryAggregationAndOverflow(t *testing.T) {
	newAttempt := func(accountPeak, ownerPeak uint64) AttemptSummary {
		var totals AllocationAccountTotals
		if quality := totals.AddGeneration(accountPeak, 0, true); quality != 0 {
			t.Fatalf("generation quality = %v", quality)
		}
		if quality := totals.AddOwnerGeneration(1, ownerPeak, 0); quality != 0 {
			t.Fatalf("owner quality = %v", quality)
		}
		return AttemptSummary{Allocation: totals, WallNS: 3}
	}

	var execution ExecutionSummary
	execution.AddAttempt(newAttempt(10, 7), true)
	execution.AddAttempt(newAttempt(20, 12), false)
	owner, ok := execution.Allocation.Owner(1)
	if !ok || owner.MaxGenerationPeak != 12 || owner.SumGenerationPeak != 19 {
		t.Fatalf("retry owner totals = %+v, present=%v", owner, ok)
	}
	if execution.AttemptCount != 2 || execution.RetryWallNS != 3 ||
		execution.Quality != 0 {
		t.Fatalf("execution summary = %+v", execution)
	}

	var overflow AllocationAccountTotals
	overflow.GenerationCount = 1
	overflow.ValidGenerationCount = 1
	overflow.MaxGenerationPeak = math.MaxUint64
	overflow.SumGenerationPeak = math.MaxUint64
	if quality := overflow.AddOwnerGeneration(1, math.MaxUint64, 0); quality != 0 {
		t.Fatalf("initial overflow owner quality = %v", quality)
	}
	quality := MergeAllocationAccountTotals(&overflow, newAttempt(1, 1).Allocation)
	owner, _ = overflow.Owner(1)
	if owner.SumGenerationPeak != math.MaxUint64 ||
		quality&QualityInvariantFailure == 0 {
		t.Fatalf("overflow owner totals = %+v, quality=%v", owner, quality)
	}
}

func TestAllocationOwnerTotalsCopiesAreImmutable(t *testing.T) {
	var source AllocationAccountTotals
	if quality := source.AddGeneration(20, 0, true); quality != 0 {
		t.Fatalf("generation quality = %v", quality)
	}
	if quality := source.AddOwnerGeneration(1, 7, 0); quality != 0 {
		t.Fatalf("owner quality = %v", quality)
	}
	copied := source
	if quality := source.AddOwnerGeneration(1, 5, 0); quality != 0 {
		t.Fatalf("updated owner quality = %v", quality)
	}
	original, ok := copied.Owner(1)
	if !ok || original.SumGenerationPeak != 7 {
		t.Fatalf("copied owner was mutated: %+v, present=%v", original, ok)
	}
	updated, ok := source.Owner(1)
	if !ok || updated.SumGenerationPeak != 12 {
		t.Fatalf("updated owner = %+v, present=%v", updated, ok)
	}

	var merged AllocationAccountTotals
	if quality := MergeAllocationAccountTotals(&merged, copied); quality != 0 {
		t.Fatalf("merge quality = %v", quality)
	}
	shared := merged
	if quality := merged.AddOwnerGeneration(2, 3, 0); quality != 0 {
		t.Fatalf("new owner quality = %v", quality)
	}
	if _, present := shared.Owner(2); present {
		t.Fatal("copy-on-write merge leaked a new owner into a prior summary")
	}
}

func TestAllocationOwnerTotalsRejectsEmptyEvidence(t *testing.T) {
	var emptyOwner AllocationAccountTotals
	if quality := emptyOwner.AddGeneration(10, 0, true); quality != 0 {
		t.Fatalf("generation quality = %v", quality)
	}
	if quality := emptyOwner.AddOwnerGeneration(1, 0, 0); quality != 0 {
		t.Fatalf("owner insertion quality = %v", quality)
	}
	if quality := emptyOwner.Validate(); quality&QualityInvariantFailure == 0 {
		t.Fatalf("empty owner evidence quality = %v", quality)
	}

	var ownerWithoutGeneration AllocationAccountTotals
	if quality := ownerWithoutGeneration.AddOwnerGeneration(1, 1, 0); quality != 0 {
		t.Fatalf("owner insertion quality = %v", quality)
	}
	if quality := ownerWithoutGeneration.Validate(); quality&QualityInvariantFailure == 0 {
		t.Fatalf("owner without generation quality = %v", quality)
	}

	peakWithoutGeneration := AllocationAccountTotals{
		MaxGenerationPeak: 1,
		SumGenerationPeak: 1,
	}
	if quality := peakWithoutGeneration.Validate(); quality&QualityInvariantFailure == 0 {
		t.Fatalf("peak without generation quality = %v", quality)
	}
}

func TestLocalRecorder(t *testing.T) {
	var recorder LocalRecorder
	recorder.AddActiveInterval(100, 10, 20)
	recorder.AddWait(WaitLock, 10)
	recorder.AddS3Request(S3Get, 2)

	delta := recorder.Snapshot()
	if delta.Quality != 0 || delta.Usage.ExclusiveActiveNS != 70 ||
		delta.Usage.WaitNS[WaitLock] != 10 || delta.Usage.S3Requests[S3Get] != 2 {
		t.Fatalf("unexpected snapshot: %+v", delta)
	}
}

func TestHotPathStructureSizes(t *testing.T) {
	if got := reflect.TypeOf(Usage{}).Size(); got > 160 {
		t.Fatalf("Usage is %d bytes, limit is 160", got)
	}
	if got := reflect.TypeOf(LocalRecorder{}).Size(); got > 160 {
		t.Fatalf("LocalRecorder is %d bytes, limit is 160", got)
	}
	if got := reflect.TypeOf(Delta{}).Size(); got > 192 {
		t.Fatalf("Delta is %d bytes, limit is 192", got)
	}
	if got := reflect.TypeOf(MemoryDomainSummary{}).Size(); got > 40 {
		t.Fatalf("MemoryDomainSummary is %d bytes, limit is 40", got)
	}
}

func TestAllocationAccountTotalsBoundedSize(t *testing.T) {
	if got := reflect.TypeOf(AllocationAccountTotals{}).Size(); got > 64 {
		t.Fatalf("AllocationAccountTotals is %d bytes, limit is 64", got)
	}
}

func TestAllocationOwnerTotalsInvalidShapeAndJSONBoundaries(t *testing.T) {
	var nilTotals *AllocationAccountTotals
	if quality := nilTotals.AddOwnerGeneration(1, 1, 0); quality&QualityInvariantFailure == 0 {
		t.Fatalf("nil totals quality = %v", quality)
	}
	if _, ok := nilTotals.Owner(1); ok || nilTotals.OwnerCount() != 0 ||
		nilTotals.Validate()&QualityInvariantFailure == 0 {
		t.Fatal("nil totals accepted owner state")
	}

	invalid := AllocationAccountTotals{
		owners: &allocationOwnerSet{mask: 1, values: []AllocationOwnerTotals{{}}},
	}
	if quality := invalid.AddOwnerGeneration(1, 1, 0); quality&QualityInvariantFailure == 0 {
		t.Fatalf("invalid add quality = %v", quality)
	}
	if _, ok := invalid.Owner(1); ok || invalid.OwnerCount() != 0 ||
		invalid.Validate()&QualityInvariantFailure == 0 {
		t.Fatal("invalid sparse owner shape was observable")
	}
	if _, err := json.Marshal(invalid); err == nil {
		t.Fatal("invalid sparse owner shape was serialized")
	}
	if quality := mergeAllocationOwnerTotals(&invalid, &AllocationAccountTotals{}); quality&QualityInvariantFailure == 0 {
		t.Fatalf("invalid merge quality = %v", quality)
	}
	if quality := mergeAllocationOwnerTotals(nil, &AllocationAccountTotals{}); quality&QualityInvariantFailure == 0 {
		t.Fatalf("nil merge quality = %v", quality)
	}
	if nilTotals.OwnerAttributionCoversTotals() {
		t.Fatal("nil totals covered aggregate peaks")
	}
	if clone := (AllocationAccountTotals{}).Clone(); clone.owners != nil {
		t.Fatal("empty clone gained owner storage")
	}
	var liveMismatch AllocationAccountTotals
	liveMismatch.GenerationCount = 1
	liveMismatch.ValidGenerationCount = 1
	liveMismatch.MaxGenerationPeak = 2
	liveMismatch.SumGenerationPeak = 2
	_ = liveMismatch.AddOwnerGeneration(1, 2, 1)
	liveMismatch.LiveBytesAtTerminal = 0
	if quality := liveMismatch.Validate(); quality&QualityInvariantFailure == 0 {
		t.Fatalf("owner live mismatch quality = %v", quality)
	}

	var source AllocationAccountTotals
	if quality := source.AddOwnerGeneration(63, math.MaxUint64, 0); quality != 0 {
		t.Fatalf("owner 63 quality = %v", quality)
	}
	if quality := source.AddOwnerGeneration(1, 1, 0); quality != 0 {
		t.Fatalf("owner 1 quality = %v", quality)
	}
	source.GenerationCount = 1
	source.ValidGenerationCount = 1
	source.MaxGenerationPeak = math.MaxUint64
	source.SumGenerationPeak = math.MaxUint64
	if !source.OwnerAttributionCoversTotals() {
		t.Fatal("saturating owner coverage did not cover aggregate peak")
	}
	clone := source.Clone()
	if clone.owners == source.owners || clone.OwnerCount() != 2 {
		t.Fatal("clone did not detach sparse owner storage")
	}
	if _, ok := clone.Owner(0); ok {
		t.Fatal("owner zero unexpectedly present")
	}
	if _, ok := clone.Owner(AllocationOwnerMaxID + 1); ok {
		t.Fatal("out-of-range owner unexpectedly present")
	}

	for _, payload := range []string{
		`{"Owners":{}}`,
		`{"Owners":[`,
	} {
		var decoded AllocationAccountTotals
		if err := json.Unmarshal([]byte(payload), &decoded); err == nil {
			t.Fatalf("accepted malformed owner JSON: %s", payload)
		}
	}
	for _, payload := range []string{"", `[{"Owner":`} {
		var list allocationOwnerTotalsJSONList
		if err := list.UnmarshalJSON([]byte(payload)); err == nil {
			t.Fatalf("accepted malformed owner list: %s", payload)
		}
	}
	var nullOwners AllocationAccountTotals
	if err := json.Unmarshal([]byte(`{"GenerationCount":0,"Owners":null}`), &nullOwners); err != nil {
		t.Fatal(err)
	}
	var sorted AllocationAccountTotals
	if err := json.Unmarshal([]byte(`{"GenerationCount":1,"ValidGenerationCount":1,"MaxGenerationPeak":3,"SumGenerationPeak":3,"Owners":[{"Owner":3,"MaxGenerationPeak":2,"SumGenerationPeak":2},{"Owner":1,"MaxGenerationPeak":1,"SumGenerationPeak":1}]}`), &sorted); err != nil {
		t.Fatal(err)
	}
	if first, ok := sorted.Owner(1); !ok || first.SumGenerationPeak != 1 {
		t.Fatalf("owner sort result = %+v, present=%v", first, ok)
	}
}
