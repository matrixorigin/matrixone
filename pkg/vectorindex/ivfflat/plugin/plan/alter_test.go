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

package plan

import (
	"testing"

	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestUpdateColumnRequiresIndexRewriteDelegatesIncludeHook(t *testing.T) {
	old := planplugin.IncludedColumnAffected
	defer func() { planplugin.IncludedColumnAffected = old }()

	var gotCol string
	planplugin.IncludedColumnAffected = func(_ *planpb.IndexDef, colName string) (bool, error) {
		gotCol = colName
		return colName == "payload", nil
	}

	needsRewrite, err := Hooks{}.UpdateColumnRequiresIndexRewrite(&planpb.TableDef{}, &planpb.IndexDef{}, "payload")
	require.NoError(t, err)
	require.True(t, needsRewrite)
	require.Equal(t, "payload", gotCol)

	needsRewrite, err = Hooks{}.UpdateColumnRequiresIndexRewrite(&planpb.TableDef{}, &planpb.IndexDef{}, "other")
	require.NoError(t, err)
	require.False(t, needsRewrite)
}
