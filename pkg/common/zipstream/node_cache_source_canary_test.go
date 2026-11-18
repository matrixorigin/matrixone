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

package zipstream

import "testing"

// This isolated source-change canary changes one test compilation unit while
// leaving production behavior, package membership and coverage blocks intact.
// It is not part of a production rollout or a replacement for the full suite.
func TestNodeCacheControlledSourceVersion(t *testing.T) {
	for _, test := range []struct {
		name       string
		flags      uint16
		loadable   bool
		descriptor bool
	}{
		{"payload.txt", 0, true, false},
		{"directory/", zipFlagDescriptor, false, true},
		{".hidden", zipFlagEncrypted, false, false},
		{"nested/payload.txt", zipFlagDescriptor | zipFlagEncrypted, true, true},
	} {
		header := zipLocalHeader{name: test.name, flags: test.flags}
		if header.loadable() != test.loadable || header.hasDescriptor() != test.descriptor {
			t.Fatalf("controlled source canary failed for %q", test.name)
		}
	}
}
