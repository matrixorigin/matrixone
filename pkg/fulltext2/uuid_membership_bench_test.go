// Copyright 2026 Matrix Origin
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

package fulltext2

import "testing"

var benchmarkUUIDMembershipSink uint64

// BenchmarkLoadedUUIDMembership measures the warmed loaded UUID wrapper with a
// deterministic exact filter. The serial hit/miss/mixed cells isolate ParseBytes
// and the query-owned scratch from the real C/Bloom filter's own allocation and
// variability; run this benchmark with GOMAXPROCS=1 and 10 for the two requested
// scheduler settings.
func BenchmarkLoadedUUIDMembership(b *testing.B) {
	u0 := mustTestUUID(b, "00000000-0000-0000-0000-000000000001")
	u1 := mustTestUUID(b, "00000000-0000-0000-0000-000000000002")
	u2 := mustTestUUID(b, "00000000-0000-0000-0000-000000000003")
	loaded := loadedUUIDSegment(b,
		uuidMembershipDoc{pk: u0.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u1.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u2.String(), words: []string{"alpha"}},
	)

	cases := []struct {
		name   string
		filter uuidSetMembershipFilter
		ord    func(int) int64
	}{
		{name: "hit", filter: newUUIDSetMembershipFilter(u0), ord: func(int) int64 { return 0 }},
		{name: "miss", filter: newUUIDSetMembershipFilter(u0), ord: func(int) int64 { return 1 }},
		{name: "mixed", filter: newUUIDSetMembershipFilter(u0, u2), ord: func(i int) int64 {
			return int64(i % 3)
		}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			membership := &docFilterMembership{seg: loaded, f: tc.filter}
			membership.Contains(tc.ord(0)) // warm the loaded path before timing
			b.ReportAllocs()
			b.SetBytes(16)
			b.ResetTimer()
			var last bool
			for i := 0; i < b.N; i++ {
				last = membership.Contains(tc.ord(i))
			}
			benchmarkUUIDMembershipSink = uint64(boolToInt(last))
		})
	}
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
