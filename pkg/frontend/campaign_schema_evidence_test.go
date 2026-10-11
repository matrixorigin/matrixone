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

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestCampaignEvidenceSessionScopeAndConversion(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	require.False(t, ses.campaignEvidenceEnabled.Load())
	for _, setting := range []any{int64(1), "ON", true} {
		require.NoError(t, ses.SetSessionSysVar(context.Background(), "mo_sirius_campaign_evidence", setting))
		require.True(t, ses.campaignEvidenceEnabled.Load())
	}
	require.Error(t, ses.SetSessionSysVar(context.Background(), "mo_sirius_campaign_evidence", int64(2)))
	require.True(t, ses.campaignEvidenceEnabled.Load(), "rejected SET cannot change the cached flag")
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "mo_sirius_campaign_evidence", int64(0)))
	require.False(t, ses.campaignEvidenceEnabled.Load())
	require.Equal(t, ScopeSession, gSysVarsDefs["mo_sirius_campaign_evidence"].Scope)
	require.False(t, gSysVarsDefs["mo_sirius_campaign_evidence"].SetVarHintApplies)
}
