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

package process

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestViewSchemaResourcesBorrowWithoutClosingParent(t *testing.T) {
	parent := NewTopProcess(t.Context(), mpool.MustNewZero(), nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer parent.Free()
	parent.Base.Lim.Size = 1024
	generation, err := parent.GetExecutionResourceBudget()
	require.NoError(t, err)
	defer generation.Close()
	child := parent.NewViewBindingProcess(t.Context())
	defer child.Free()
	require.NoError(t, child.BorrowViewSchemaResources(parent, generation))
	same, err := child.GetExecutionResourceBudget()
	require.NoError(t, err)
	require.Same(t, generation, same)
	lease, err := same.ReserveTransientMemory(1024)
	require.NoError(t, err)
	_, err = generation.ReserveTransientMemory(1)
	require.Error(t, err)
	lease.Release()
	child.SetStmtProfile(&StmtProfile{})
	require.False(t, generation.Closed())
	require.False(t, child.UsesExecutionResourceGeneration(generation))
	lease, err = generation.ReserveTransientMemory(1024)
	require.NoError(t, err)
	lease.Release()
	require.Error(t, parent.BorrowViewSchemaResources(parent, generation))
	parent.SetStmtProfile(&StmtProfile{})
	require.True(t, generation.Closed())
	require.Error(t, child.BorrowViewSchemaResources(parent, generation))
}
