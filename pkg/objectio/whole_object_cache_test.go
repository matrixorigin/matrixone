// Copyright 2022 Matrix Origin
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

package objectio

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountCachedObjectFilesAdmission(t *testing.T) {
	id := NewObjectid()
	for _, tc := range []struct {
		name string
		ids  [][]byte
	}{
		{name: "too many", ids: make([][]byte, MaxObjectCacheProbeIDs+1)},
		{name: "not a fixed-size ID", ids: [][]byte{[]byte("../path")}},
		{name: "duplicate", ids: [][]byte{id[:], id[:]}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count, err := CountCachedObjectFiles(t.Context(), nil, tc.ids)
			require.Error(t, err)
			require.Zero(t, count)
		})
	}
	count, err := CountCachedObjectFiles(t.Context(), nil, [][]byte{id[:]})
	require.NoError(t, err)
	require.Zero(t, count)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = CountCachedObjectFiles(ctx, nil, [][]byte{id[:]})
	require.ErrorIs(t, err, context.Canceled)
}
