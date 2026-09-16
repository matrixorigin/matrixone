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

package index

import (
	"bytes"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestCollationKeysThroughSerializedFilters(t *testing.T) {
	part, err := types.ResolveStringKeyPart(types.NewWithCharset(types.T_varchar, 0, 0, types.CharsetUTF8), types.PADSpaceKeyV1)
	require.NoError(t, err)
	p := types.NewPacker()
	defer p.Close()
	encode := func(value string) []byte {
		p.Reset()
		_, err := part.Encode(p, nil, []byte(value))
		require.NoError(t, err)
		return p.Bytes()
	}
	// A single-value block makes an unconverted probe a deterministic ZM
	// false negative. Longer cases exercise the 30-byte summary truncation.
	for _, stored := range []string{"Alpha", "a\x00", "a \x00", strings.Repeat("Alpha", 20), strings.Repeat("中", 20), "😀"} {
		key := encode(stored)
		data := containers.MakeVector(types.T_varbinary.ToType(), common.DefaultAllocator)
		data.Append(key, false)
		filter, err := NewBloomFilter(data, nil, nil, nil)
		require.NoError(t, err)
		data.Close()
		serialized, err := filter.Marshal()
		require.NoError(t, err)
		restored := NewEmptyBloomFilter()
		require.NoError(t, restored.Unmarshal(serialized))
		zm := BuildZM(types.T_varbinary, key)
		zbytes, err := zm.Marshal()
		require.NoError(t, err)
		zrestored := ZM(bytes.Clone(zbytes))
		for _, probe := range []string{stored, stored + " ", strings.ToLower(stored)} {
			needle := encode(probe)
			require.True(t, zrestored.ContainsKey(needle), "ZM dropped true match %q", probe)
			maybe, err := restored.MayContainsKey(needle)
			require.NoError(t, err)
			require.True(t, maybe, "Bloom dropped true match %q", probe)
		}
		if stored == "Alpha" {
			p.Reset()
			p.EncodeStringType([]byte("alpha"))
			require.False(t, zrestored.ContainsKey(p.GetBuf()), "observer must catch omitted probe transformation")
		}
	}
}
