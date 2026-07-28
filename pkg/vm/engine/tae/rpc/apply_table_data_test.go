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

package rpc

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestPrepareObjectStatsForApply(t *testing.T) {
	id := objectio.NewObjectid()
	flushed := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(flushed, 2))

	prepared, createLiveAppendable, err := prepareObjectStatsForApply(*flushed, true)
	require.NoError(t, err)
	require.False(t, createLiveAppendable)
	require.False(t, prepared.GetAppendable())
	require.Equal(t, uint32(2), prepared.Rows())
	// Preparing the restored catalog state must not mutate the dump metadata.
	require.True(t, flushed.GetAppendable())

	_, _, err = prepareObjectStatsForApply(*flushed, false)
	require.ErrorContains(t, err, "live appendable object")

	live := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
	prepared, createLiveAppendable, err = prepareObjectStatsForApply(*live, false)
	require.NoError(t, err)
	require.True(t, createLiveAppendable)
	require.True(t, prepared.GetAppendable())
}
