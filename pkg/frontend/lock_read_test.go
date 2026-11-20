// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/stretchr/testify/require"
)

func TestLockReadLegacyResourceProjectionRefreshesCachedPlan(t *testing.T) {
	ctx := context.Background()
	h := &jsonPlanHandler{marshalHandler: &marshalPlanHandler{marshalPlan: &models.ExplainData{}}}
	defer h.Free()
	require.NotContains(t, string(h.Marshal(ctx)), "lock_read")
	diagnostic := &resource.LockReadDiagnostic{StatementID: uuid.New(), AttemptCount: 2, Seen: 1, AcquisitionNS: 100, Outcome: "failed", LastIncomplete: "acquisition"}
	h.SetResourceSummary(resource.StatementResourceSummary{AttemptCount: 2, LockRead: diagnostic})
	var payload models.ExplainData
	require.NoError(t, json.Unmarshal(h.Marshal(ctx), &payload))
	require.NotNil(t, payload.PhyPlan.Resource)
	require.Equal(t, diagnostic, payload.PhyPlan.Resource.LockRead)
}
