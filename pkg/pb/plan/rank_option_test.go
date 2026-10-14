// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRankOptionWire(t *testing.T) {
	option := &RankOption{Mode: "post", Nprobe: 80000}
	wire, err := option.Marshal()
	require.NoError(t, err)
	require.Len(t, wire, option.ProtoSize())
	var decoded RankOption
	require.NoError(t, decoded.Unmarshal(wire))
	require.Equal(t, option, &decoded)
	// Historical mode-only field 1 has no query override.
	oldWire := []byte{0x0a, 4, 'p', 'o', 's', 't'}
	var legacy RankOption
	require.NoError(t, legacy.Unmarshal(oldWire))
	require.Equal(t, "post", legacy.Mode)
	require.Zero(t, legacy.Nprobe)
	encoded, err := legacy.Marshal()
	require.NoError(t, err)
	require.Equal(t, oldWire, encoded)
}
