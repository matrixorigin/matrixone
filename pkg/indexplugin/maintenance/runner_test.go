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

package maintenance

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestSelectDefinitions(t *testing.T) {
	def := &plan.IndexDef{IndexName: "idx"}
	group := map[string]*plan.IndexDef{"meta": def, "unused": {}, "nil": nil}
	for _, tc := range []struct {
		name    string
		roles   []string
		invalid bool
	}{
		{"subset", []string{"meta"}, false},
		{"nil roles", nil, false},
		{"empty roles", []string{}, false},
		{"empty role", []string{""}, true},
		{"duplicate", []string{"meta", "meta"}, true},
		{"missing", []string{"missing"}, true},
		{"nil definition", []string{"nil"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := SelectDefinitions(group, tc.roles)
			if tc.invalid {
				require.Nil(t, selected)
			} else {
				require.NotNil(t, selected)
				require.Len(t, selected, len(tc.roles))
				if len(tc.roles) > 0 {
					require.Same(t, def, selected["meta"])
					delete(selected, "meta")
				}
			}
			require.Len(t, group, 3)
			require.Same(t, def, group["meta"])
		})
	}
}
