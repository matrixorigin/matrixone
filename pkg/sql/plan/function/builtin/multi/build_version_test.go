// Copyright 2021 - 2022 Matrix Origin
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

package multi

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBuildVersion(t *testing.T) {
	proc := testutil.NewProc()
	res, err := BuildVersion([]*vector.Vector{}, proc)
	require.NoError(t, err)

	got := vector.MustFixedCol[types.Timestamp](res)
	require.Equal(t, []types.Timestamp{0}, got)

	t.Run("parseBuildTime", func(t *testing.T) {
		ts := parseBuildTime("2023-03-24T21:55:01+08:00")
		require.Equal(t, ts, types.Timestamp(63815262901000000))
	})
}
