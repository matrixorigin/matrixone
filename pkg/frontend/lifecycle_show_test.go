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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestLifecycleShowPageIsBounded(t *testing.T) {
	limit, offset, err := lifecycleShowPage(
		context.Background(),
		&tree.ShowLifecycle{},
	)
	require.NoError(t, err)
	require.Equal(t, lifecycleShowDefaultLimit, limit)
	require.Zero(t, offset)

	limit, offset, err = lifecycleShowPage(
		context.Background(),
		&tree.ShowLifecycle{
			Page: tree.NewLimit(
				tree.NewNumVal(int64(2500), "2500", false, tree.P_int64),
				tree.NewNumVal(int64(100), "100", false, tree.P_int64),
			),
		},
	)
	require.NoError(t, err)
	require.Equal(t, int64(100), limit)
	require.Equal(t, int64(2500), offset)

	for _, statement := range []*tree.ShowLifecycle{
		{Page: tree.NewLimit(nil, tree.NewNumVal(int64(0), "0", false, tree.P_int64))},
		{Page: tree.NewLimit(nil, tree.NewNumVal(lifecycleShowMaxLimit+1, "1001", false, tree.P_int64))},
		{Page: tree.NewLimit(
			tree.NewNumVal(lifecycleShowMaxWindow, "1000000", false, tree.P_int64),
			tree.NewNumVal(int64(1), "1", false, tree.P_int64),
		)},
		{Page: tree.NewLimit(nil, tree.NewNumVal(float64(1.5), "1.5", false, tree.P_float64))},
	} {
		_, _, err = lifecycleShowPage(context.Background(), statement)
		require.Error(t, err)
	}
}
