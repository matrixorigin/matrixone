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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

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
