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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// json_extract_string_internal / json_extract_float64_internal are byte-identical twins of the public
// json_extract_string / json_extract_float64, used only in the self-completing json probe's fallback
// SQL (so a base-table scan can push the predicate without re-triggering the probe rewrite). This
// guards that (1) they resolve by name -- else the fallback SQL would fail to plan -- and (2) they
// resolve to the SAME overload/return type as their public counterparts, so results cannot diverge.
func TestJSONExtractInternalMirrorsPublic(t *testing.T) {
	ctx := context.Background()
	args := []types.Type{types.T_json.ToType(), types.T_varchar.ToType()}

	cases := []struct {
		public, internal string
		wantFid          int32
	}{
		{"json_extract_string", "json_extract_string_internal", JSON_EXTRACT_STRING_INTERNAL},
		{"json_extract_float64", "json_extract_float64_internal", JSON_EXTRACT_FLOAT64_INTERNAL},
	}
	for _, c := range cases {
		pub, err := GetFunctionByName(ctx, c.public, args)
		require.NoError(t, err, "public %s must resolve", c.public)
		intl, err := GetFunctionByName(ctx, c.internal, args)
		require.NoError(t, err, "internal %s must resolve (fallback SQL depends on it)", c.internal)

		require.Equal(t, c.wantFid, functionIdRegister[c.internal],
			"%s must map to its dedicated function id", c.internal)
		// Same return type => the fallback predicate evaluates identically to the tail/base predicate.
		require.Equal(t, pub.GetReturnType(), intl.GetReturnType(),
			"%s must return the same type as %s", c.internal, c.public)
	}
}
