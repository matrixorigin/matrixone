// Copyright 2021 Matrix Origin
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

package txnentries

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestTransferredDeleteSet(t *testing.T) {
	objectID1 := types.NewObjectid()
	objectID2 := types.NewObjectid()
	row1 := types.NewRowIDWithObjectIDBlkNumAndRowID(objectID1, 0, 7)
	row2 := types.NewRowIDWithObjectIDBlkNumAndRowID(objectID1, 1, 7)
	row3 := types.NewRowIDWithObjectIDBlkNumAndRowID(objectID2, 0, 7)

	set := make(transferredDeleteSet)
	require.False(t, set.contains(row1))
	set.add(row1)
	require.True(t, set.contains(row1))
	require.False(t, set.contains(row2))
	require.False(t, set.contains(row3))

	other := make(transferredDeleteSet)
	other.add(row2)
	other.add(row3)
	set.merge(other)
	require.True(t, set.contains(row1))
	require.True(t, set.contains(row2))
	require.True(t, set.contains(row3))
}
