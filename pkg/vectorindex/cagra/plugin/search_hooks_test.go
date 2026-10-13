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

package plugin

import (
	"testing"

	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPluginHasSearchHooks(t *testing.T) {
	hooks := New().Search()
	require.NotNil(t, hooks)
	explainer, ok := hooks.(searchplugin.ExplainHooks)
	require.True(t, ok)
	got, err := explainer.ExplainSettings(&plan.IndexSearchScan{}, true)
	require.NoError(t, err)
	require.Equal(t, []string{"ThreadsSearch: 0", "BatchWindow: 0", "GpuMultiSimulation: 0", "KeyPartType: 0"}, got)
}
