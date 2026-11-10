// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtailreplay

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectListNameClassification(t *testing.T) {
	for _, tc := range []struct {
		name            string
		data, tombstone bool
	}{
		{"", false, false}, {"mo_tables", false, false},
		{"_0_data_meta", true, false}, {"_1_tombstone_meta", false, true}, {"_0001_tombstone_meta", false, true},
		{"prefix_123_data_meta_suffix", true, false},
		{"_" + strings.Repeat("9", 100) + "_data_meta", true, false},
		{"_data_meta", false, false}, {"_+1_data_meta", false, false},
		{"_-1_tombstone_meta", false, false}, {"123_data_meta", false, false},
		{"_١_data_meta", false, false}, {"_１_data_meta", false, false},
		{"_1x_data_meta", false, false}, {"_1_data_metaX", true, false},
		{"_x_data_meta_2_data_meta", true, false}, {"x123_data_meta_4_data_meta", true, false},
		{"_data_meta_3_tombstone_meta", false, true},
		{"_1_data_meta_2_tombstone_meta", true, true},
		{"\xff_1_data_meta\xfe", true, false},
		{"_1_DATA_META", false, false}, {"_1_data_met", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.data, IsDataObjectList(tc.name))
			require.Equal(t, tc.tombstone, IsTombstoneObjectList(tc.name))
			require.Equal(t, tc.data || tc.tombstone, IsMetaEntry(tc.name))
		})
	}
}

func TestObjectListNameClassificationMatchesRE2(t *testing.T) {
	data := regexp.MustCompile(`_\d+_data_meta`)
	tombstone := regexp.MustCompile(`_\d+_tombstone_meta`)
	for _, prefix := range []string{"", "_", "x_", "\xff_", "\x00_"} {
		for _, digits := range []string{"", "0", "00123", strings.Repeat("9", 100), "+1", "١", "１", "1x2"} {
			for _, suffix := range []string{"_data_meta", "_tombstone_meta", "_data_met", "_data_meta_4_data_meta", "_data_meta_5_tombstone_meta"} {
				for _, tail := range []string{"", "tail", "\xff", "_6_data_meta"} {
					name := prefix + digits + suffix + tail
					wantData, wantTombstone := data.MatchString(name), tombstone.MatchString(name)
					if IsDataObjectList(name) != wantData || IsTombstoneObjectList(name) != wantTombstone || IsMetaEntry(name) != (wantData || wantTombstone) {
						t.Fatalf("classification differs for %q", name)
					}
				}
			}
		}
	}
}

func FuzzObjectListNameClassification(f *testing.F) {
	for _, name := range []string{"", "mo_tables", "_123_data_meta", "_4_tombstone_meta", "x_data_meta_00_data_meta_tail", "_١_data_meta", "\xff_999_tombstone_meta"} {
		f.Add(name)
	}
	data := regexp.MustCompile(`_\d+_data_meta`)
	tombstone := regexp.MustCompile(`_\d+_tombstone_meta`)
	f.Fuzz(func(t *testing.T, name string) {
		wantData, wantTombstone := data.MatchString(name), tombstone.MatchString(name)
		if IsDataObjectList(name) != wantData || IsTombstoneObjectList(name) != wantTombstone || IsMetaEntry(name) != (wantData || wantTombstone) {
			t.Fatalf("classification differs for %q", name)
		}
	})
}

func BenchmarkObjectListNameClassification(b *testing.B) {
	for _, name := range []string{"mo_tables", "_123_data_meta", "_123_tombstone_meta", "prefix_00_data_meta_suffix", "_" + strings.Repeat("9", 100) + "_data_meta", "_x_data_meta_2_data_meta", "_data_meta_data_meta"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				IsMetaEntry(name)
			}
		})
	}
}
