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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringShuffleHashContractMetadataRoundTrip(t *testing.T) {
	processInfo := &ProcessInfo{StringShuffleHashAlgorithm: 1}
	data, err := processInfo.Marshal()
	require.NoError(t, err)
	restoredProcessInfo := new(ProcessInfo)
	require.NoError(t, restoredProcessInfo.Unmarshal(data))
	require.Equal(t, uint32(1), restoredProcessInfo.GetStringShuffleHashAlgorithm())
	var nilProcessInfo *ProcessInfo
	require.Zero(t, nilProcessInfo.GetStringShuffleHashAlgorithm())

	shuffle := &Shuffle{StringHashKey: true}
	data, err = shuffle.Marshal()
	require.NoError(t, err)
	restoredShuffle := new(Shuffle)
	require.NoError(t, restoredShuffle.Unmarshal(data))
	require.True(t, restoredShuffle.GetStringHashKey())
	var nilShuffle *Shuffle
	require.False(t, nilShuffle.GetStringHashKey())

	data, err = new(Shuffle).Marshal()
	require.NoError(t, err)
	restoredDefault := new(Shuffle)
	require.NoError(t, restoredDefault.Unmarshal(data))
	require.False(t, restoredDefault.GetStringHashKey())
}
