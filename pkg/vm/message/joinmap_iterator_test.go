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

package message

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestJoinMapIteratorDistinguishesMissingAndEmptyIndex(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, kind := range []string{"none", "int", "string"} {
		var ihm *hashmap.IntHashMap
		var shm *hashmap.StrHashMap
		var err error
		if kind == "int" {
			ihm, err = hashmap.NewIntHashMap(false, mp)
		}
		if kind == "string" {
			shm, err = hashmap.NewStrHashMap(false, mp)
		}
		require.NoError(t, err)
		jm := NewJoinMap(GroupSels{}, ihm, shm, nil, nil, mp)
		jm.IncRef(1)
		require.Equal(t, kind != "none", jm.NewIterator() != nil)
		jm.Free()
		require.Zero(t, mp.CurrNB())
	}
}
