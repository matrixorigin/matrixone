// Copyright 2022 Matrix Origin
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

package sqlexec

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// TestExactPkFilterBitHostileBytes guards the small-key "pk IN (...)" path
// against BIT primary keys whose raw bytes contain SQL-hostile sequences:
// ' (0x27), \ (0x5c), and -- (0x2d2d). They must render as safe hex literals
// (x'..'), never as raw bytes inside a quoted string that could break out of
// the literal and alter the interpolated SQL.
func TestExactPkFilterBitHostileBytes(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.New(types.T_bit, 64, 0))
	defer vec.Free(mp)

	// Big-endian bytes (after encode + reverse) embed ', \, and --.
	vals := []uint64{0x27272727275c2d2d, 0x2700270027002700}
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}

	out, err := BuildExactPkFilter(context.Background(), vec)
	require.NoError(t, err)

	// Exact expected rendering: hex literals, comma-separated.
	require.Equal(t, "x'27272727275c2d2d',x'2700270027002700'", out)

	// No raw hostile byte leaked into the SQL, and every term is a pure-hex
	// x'..' literal (so any single-quote only ever delimits a hex literal).
	require.NotContains(t, out, `\`)
	for _, part := range strings.Split(out, ",") {
		require.True(t, strings.HasPrefix(part, "x'") && strings.HasSuffix(part, "'"), "term %q not an x'..' literal", part)
		_, derr := hex.DecodeString(part[2 : len(part)-1])
		require.NoError(t, derr, "term body must be pure hex: %s", part)
	}
}
