// Copyright 2026 Matrix Origin
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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestBindBackExecSession(t *testing.T) {
	clientSessionID := uuid.New()
	backSessionID := uuid.New()
	clientSession := &Session{
		feSessionImpl: feSessionImpl{uuid: clientSessionID},
		tempTables:    make(map[string]string),
		tempTablesRev: make(map[string]string),
	}
	backSes := &backSession{
		feSessionImpl: feSessionImpl{
			uuid:     backSessionID,
			upstream: clientSession,
		},
	}
	proc := &process.Process{Base: &process.BaseProcess{}}

	bindBackExecSession(proc, backSes)

	require.Same(t, backSes, proc.GetSession())
	require.Equal(t, clientSessionID, proc.Base.SessionInfo.SessionId)
	proc.GetSession().AddTempTable("db1", "tmp1", "real_tmp1")
	realName, ok := clientSession.GetTempTable("db1", "tmp1")
	require.True(t, ok)
	require.Equal(t, "real_tmp1", realName)
}

func TestBindBackExecSessionWithoutUpstream(t *testing.T) {
	backSessionID := uuid.New()
	backSes := &backSession{
		feSessionImpl: feSessionImpl{uuid: backSessionID},
	}
	proc := &process.Process{Base: &process.BaseProcess{}}

	bindBackExecSession(proc, backSes)

	require.Nil(t, proc.GetSession())
	require.Equal(t, uuid.Nil, proc.Base.SessionInfo.SessionId)
}

func TestInstallBackExecStatsInfoPreservesRootAndClaimsOnce(t *testing.T) {
	root := resource.NewRoot(resource.ConnInternal)
	parent := resource.ContextWithRoot(context.Background(), root)
	start := time.Unix(123, 456)
	duration := 17 * time.Millisecond

	firstCtx, firstStats := installBackExecStatsInfo(parent, start, duration)
	secondCtx, secondStats := installBackExecStatsInfo(parent, start, duration)

	if firstStats == secondStats {
		t.Fatal("successive substatements must use distinct StatsInfo values")
	}
	if resource.RootFromContext(firstCtx) != root || resource.RootFromContext(secondCtx) != root {
		t.Fatal("substatement context must preserve the parent resource root")
	}
	if statistic.StatsInfoFromContext(firstCtx) != firstStats {
		t.Fatal("first substatement context does not contain its StatsInfo")
	}
	if statistic.StatsInfoFromContext(secondCtx) != secondStats {
		t.Fatal("second substatement context does not contain its StatsInfo")
	}
	if firstStats.ParseStage.ParseStartTime != start || firstStats.ParseStage.ParseDuration != duration {
		t.Fatal("first StatsInfo parse timing was not installed")
	}
	if secondStats.ParseStage.ParseStartTime != start || secondStats.ParseStage.ParseDuration != duration {
		t.Fatal("second StatsInfo parse timing was not installed")
	}
	if _, ok := firstStats.ClaimRootPhaseResource(); !ok {
		t.Fatal("first StatsInfo claim should succeed")
	}
	if _, ok := firstStats.ClaimRootPhaseResource(); ok {
		t.Fatal("first StatsInfo claim should not succeed twice")
	}
	if _, ok := secondStats.ClaimRootPhaseResource(); !ok {
		t.Fatal("second StatsInfo claim should succeed")
	}
	if _, ok := secondStats.ClaimRootPhaseResource(); ok {
		t.Fatal("second StatsInfo claim should not succeed twice")
	}
}
