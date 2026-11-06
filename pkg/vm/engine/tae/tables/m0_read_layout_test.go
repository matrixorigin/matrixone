//go:build mo_m0

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

package tables

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestM0PersistentLayoutRequiresOwnerAndAuthority(t *testing.T) {
	loc := objectio.NewRandomLocation(0, 1)
	_, err := persistentM0ReadLayout(t.Context(), nil, nil, loc, nil)
	require.Error(t, err)
	s := catalog.NewEmptySchema("ordinary")
	id := &common.ID{DbID: 7, TableID: 42}
	layout, err := persistentM0ReadLayout(t.Context(), s, id, loc, nil)
	require.NoError(t, err)
	require.False(t, layout.Evolution)
	require.Equal(t, loc.Name().String(), layout.ObjectName)
	// Contradictory authority is rejected before any object I/O; it is never
	// treated as the ordinary physical-only result above.
	s.Extra.M0Evolution = &api.M0EvolutionContract{FormatVersion: 99}
	_, err = persistentM0ReadLayout(t.Context(), s, id, loc, nil)
	require.Error(t, err)
}
