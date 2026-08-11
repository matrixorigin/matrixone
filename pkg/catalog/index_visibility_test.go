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

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIndexVisibilityCompatibility(t *testing.T) {
	legacy := &plan.IndexDef{}
	require.True(t, IsIndexVisible(legacy))
	require.True(t, IsIndexOptimizerEligible(legacy))

	SetIndexVisibility(legacy, false)
	require.True(t, legacy.VisibilitySet)
	require.False(t, IsIndexVisible(legacy))
	require.False(t, IsIndexOptimizerEligible(legacy))

	SetIndexVisibility(legacy, true)
	require.True(t, IsIndexVisible(legacy))
	require.True(t, IsIndexOptimizerEligible(legacy))
	require.False(t, IsIndexVisible(nil))
	require.False(t, IsIndexOptimizerEligible(nil))
}
