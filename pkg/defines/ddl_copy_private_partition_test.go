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

package defines

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrivateCopyPartitionGrant(t *testing.T) {
	parent, err := WithPrivateCopyPartitionParent(context.Background(), "db", "Replica")
	require.NoError(t, err)
	require.True(t, HasPrivateCopyPartitionParent(parent, "db", "replica"))
	require.False(t, HasPrivateCopyPartitionParent(parent, "other", "replica"))
	require.False(t, IsPrivateCopyPartitionChild(parent, "db", "replica", "p0"))
	children := []string{"p0", "p1"}
	derived, err := WithPrivateCopyPartitionChildren(parent, "db", "replica", children)
	require.NoError(t, err)
	children[0] = "other"
	require.True(t, IsPrivateCopyPartitionChild(derived, "db", "replica", "P0"))
	require.False(t, IsPrivateCopyPartitionChild(derived, "db", "other", "p0"))
	require.False(t, IsPrivateCopyPartitionChild(derived, "other", "replica", "p0"))
	require.False(t, IsPrivateCopyPartitionChild(derived, "db", "replica", "other"))
	require.False(t, HasPrivateCopyPartitionParent(derived, "db", "replica"))
	_, err = WithPrivateCopyPartitionChildren(derived, "db", "replica", []string{"p2"})
	require.Error(t, err)
	for _, invalid := range [][]string{{""}, {"replica"}, {"p0", "P0"}} {
		_, err = WithPrivateCopyPartitionChildren(parent, "db", "replica", invalid)
		require.Error(t, err)
	}
	_, err = WithPrivateCopyPartitionParent(nil, "db", "replica")
	require.Error(t, err)
}
