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

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestIndexVisibilityCompatibility(t *testing.T) {
	legacy := &plan.IndexDef{Visible: false}
	visible, isSet := GetIndexVisibility(legacy)
	require.True(t, visible)
	require.False(t, isSet)

	SetIndexVisibility(legacy, false)
	visible, isSet = GetIndexVisibility(legacy)
	require.False(t, visible)
	require.True(t, isSet)
	require.False(t, legacy.Visible)
	require.Equal(t, plan.IndexOption_VISIBILITY_INVISIBLE, legacy.Option.Visibility)

	SetIndexVisibility(legacy, true)
	visible, isSet = GetIndexVisibility(legacy)
	require.True(t, visible)
	require.True(t, isSet)
	require.True(t, legacy.Visible)
	require.Equal(t, plan.IndexOption_VISIBILITY_VISIBLE, legacy.Option.Visibility)

	// A context-free consumer must preserve the explicit marker. The raw bool is
	// a proto3 compatibility field and cannot establish which state is newer.
	legacy.Visible = false
	visible, isSet = GetIndexVisibility(legacy)
	require.True(t, isSet)
	require.True(t, visible)

	legacy.Option.Visibility = plan.IndexOption_VISIBILITY_INVISIBLE
	legacy.Visible = true
	visible, isSet = GetIndexVisibility(legacy)
	require.True(t, isSet)
	require.False(t, visible)
}

func TestMoTablesLogicalIDIndexHasExplicitVisibility(t *testing.T) {
	defines := NewDefines()
	constraint := new(engine.ConstraintDef)
	require.NoError(t, constraint.UnmarshalBinary(defines.MoTableConstraint))

	for _, ct := range constraint.Cts {
		indexConstraint, ok := ct.(*engine.IndexDef)
		if !ok {
			continue
		}
		for _, indexDef := range indexConstraint.Indexes {
			if indexDef.IndexName != "idx_rel_logical_id" {
				continue
			}
			visible, isSet := GetIndexVisibility(indexDef)
			require.True(t, isSet)
			require.True(t, visible)
			return
		}
	}
	require.Fail(t, "idx_rel_logical_id is missing from the mo_tables constraint")
}
