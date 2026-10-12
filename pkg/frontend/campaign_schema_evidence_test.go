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
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/siriusbridge"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
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

func TestCampaignMetadataRejectsInvalidDescriptorsAndKeepsHeadingsPrivate(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	ses.SetStmtId(uuid.New())
	var mu sync.Mutex
	var events []map[string]any
	previous := logutil.GetReportZapFunc()
	logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(encoder zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
		if entry.Message == "MO campaign result schema" {
			object := zapcore.NewMapObjectEncoder()
			for _, field := range fields {
				field.AddTo(object)
			}
			mu.Lock()
			events = append(events, object.Fields)
			mu.Unlock()
		}
		return previous(encoder, entry, fields)
	}})
	t.Cleanup(func() { logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previous}) })
	column := &plan.ColDef{Name: "private heading", Typ: plan.Type{Id: int32(types.T_decimal256), Width: 8, Scale: 2}}
	recordCampaignResultMetadata(ses, []*plan.ColDef{column})
	ses.campaignEvidenceEnabled.Store(true)
	for _, columns := range [][]*plan.ColDef{nil, make([]*plan.ColDef, 1025), {nil}, {{Name: strings.Repeat("x", (1<<20)+1)}}, {{Typ: plan.Type{Id: -1}}}} {
		recordCampaignResultMetadata(ses, columns)
	}
	snapshot := func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]any(nil), events...)
	}
	require.Empty(t, snapshot(), "disabled or invalid metadata cannot produce evidence")
	recordCampaignResultMetadata(ses, []*plan.ColDef{column})
	observed := snapshot()
	require.Len(t, observed, 1)
	event := observed[0]
	require.Equal(t, ses.GetStmtId().String(), event["query_id"])
	require.EqualValues(t, ses.GetAccountId(), event["account_id"])
	require.Equal(t, []siriusbridge.ColumnEvidence{{OID: int32(types.T_decimal256), Precision: 8, Scale: 2, Nullable: true}}, event["output_schema"])
	encoded, err := json.Marshal(event)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), column.Name)
	_, expectedDigest, err := siriusbridge.SchemaEvidence([]siriusbridge.Column{{OID: uint32(types.T_decimal256), Width: 8, Scale: 2, Nullable: true, Name: column.Name}})
	require.NoError(t, err)
	require.Equal(t, expectedDigest, event["output_schema_sha256"])
}
