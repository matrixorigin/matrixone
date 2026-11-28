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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestRestoredViewCreationContextCarriesAuthoredCTEGeneration(t *testing.T) {
	legacy := `{"Stmt":"create view v as with src as (select 9 as id) select * from (with a as (select id from src), src as (select 1 as id) select id from a) as d","DefaultDatabase":"db"}`
	recorded := `{"Stmt":"create view v as select 9 as id","DefaultDatabase":"db","cte_scope_protocol":1}`
	for _, tc := range []struct {
		name       string
		definition string
		wantLegacy bool
	}{
		{"release-authored marker-free definition rebinding stays legacy", legacy, true},
		{"current-generation recorded definition rebinding stays corrected", recorded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockCompilerContext(true, newPlanTestProcess(t))
			base := mock.GetContext()
			if base == nil {
				base = context.Background()
			}
			ctx := WithRestoredViewDefinition(base, "db", "v", tc.definition, tc.definition)
			restored := WithRestoredViewDefinition(ctx, "db", "v", tc.definition, tc.definition)
			carrier := &restoredGenerationCarrier{MockCompilerContext: mock, ctx: restored}
			creation, err := restoredViewCreationContext(carrier, "db", "v")
			require.NoError(t, err)
			require.Equal(t, tc.wantLegacy, defines.IsLegacyViewCTEScope(creation.GetContext()))
		})
	}
}

type restoredGenerationCarrier struct {
	*MockCompilerContext
	ctx context.Context
}

func (c *restoredGenerationCarrier) GetContext() context.Context { return c.ctx }

// The persisted definition's marker-or-absence travels with the original
// object alone: a rebuilt current-format definition must not stamp its own
// generation onto a release-authored persisted definition being restored.
func TestPreserveRestoredViewDefinitionKeepsMarkerAbsence(t *testing.T) {
	legacy := `{"Stmt":"create view v as select 1","DefaultDatabase":"db"}`
	rebuilt := `{"Stmt":"create view v as select 1","DefaultDatabase":"db","cte_scope_protocol":1}`
	ctx := WithRestoredViewDefinition(context.Background(), "db", "v", legacy, legacy)
	out, err := preserveRestoredViewDefinition(ctx, "db", "v", rebuilt)
	require.NoError(t, err)
	require.NotContains(t, out, "cte_scope_protocol")
	recordedSource := `{"Stmt":"create view v as select 1","DefaultDatabase":"db","cte_scope_protocol":2}`
	ctx2 := WithRestoredViewDefinition(context.Background(), "db", "v", recordedSource, recordedSource)
	out2, err2 := preserveRestoredViewDefinition(ctx2, "db", "v", rebuilt)
	require.NoError(t, err2)
	require.Contains(t, out2, `"cte_scope_protocol":2`)
}
