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

package colexec

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestNotEqualZoneMapWithAmbiguousByteMax(t *testing.T) {
	prefix := bytes.Repeat([]byte{0xff}, 30)
	lower := append(bytes.Clone(prefix), 1)
	upper := append(bytes.Clone(prefix), 2)
	lhs := index.BuildZM(types.T_varbinary, lower)
	rhs := index.BuildZM(types.T_varbinary, upper)
	mayMatch, known := anyNotEqualZoneMap(lhs, rhs)
	require.True(t, known)
	require.True(t, mayMatch, "two distinct values must not become one exact point")

	short := index.BuildZM(types.T_varbinary, []byte("same"))
	mayMatch, known = anyNotEqualZoneMap(short, short)
	require.True(t, known)
	require.False(t, mayMatch, "an exact short singleton remains prunable")
}
