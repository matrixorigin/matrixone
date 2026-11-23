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

package plan

import "testing"

func TestCatalogZeroDefault(t *testing.T) {
	cases := []struct {
		name  string
		value *Default
		want  bool
	}{
		{"nil", nil, false},
		{"zero", &Default{}, true},
		{"nullable", &Default{NullAbility: true}, true},
		{"expression", &Default{Expr: &Expr{}}, false},
		{"origin", &Default{OriginString: "0"}, false},
		{"empty unknown", &Default{XXX_unrecognized: []byte{}}, false},
		{"unknown", &Default{XXX_unrecognized: []byte{8, 1}}, false},
		{"cached size", &Default{XXX_sizecache: 1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CatalogZeroDefault(tc.value); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
