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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/require"
)

type migrationDisposalProtocol struct {
	*testMysqlWriter
	controller *migrateController
	closed     bool
}

func (p *migrationDisposalProtocol) Close() {
	// Closing may synchronously wait for operation admission to be released.
	p.controller.waitAndClose()
	p.closed = true
}

func TestMigrationReplayFailureDisposesTarget(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	p := &migrationDisposalProtocol{testMysqlWriter: &testMysqlWriter{}}
	rt := NewRoutine(t.Context(), p, &config.FrontendParameters{})
	p.controller = rt.mc
	rt.setSession(ses)
	t.Cleanup(rt.cancelRoutineFunc)
	// Invalid typed state fails after replay has started.
	req := &query.MigrateConnToRequest{LastInsertIDExported: true, LastInsertID: 91,
		UserDefinedVarsExported: true, UserDefinedVars: []*query.MigrateUserDefinedVar{nil}}
	require.Error(t, rt.migrateConnectionTo(t.Context(), req))
	require.True(t, p.closed)
	require.True(t, rt.shouldCloseConnection())
	require.False(t, rt.mc.tryBeginOperation())
	req.UserDefinedVars = nil
	require.Error(t, rt.migrateConnectionTo(t.Context(), req))
}

func TestMigrationReplayPanicDisposesTarget(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	tenant := ses.GetTenantInfo()
	ses.SetTenantInfo(nil)
	t.Cleanup(func() { ses.SetTenantInfo(tenant) })
	p := &migrationDisposalProtocol{testMysqlWriter: &testMysqlWriter{}}
	rt := NewRoutine(t.Context(), p, &config.FrontendParameters{})
	p.controller = rt.mc
	rt.setSession(ses)
	t.Cleanup(rt.cancelRoutineFunc)
	req := &query.MigrateConnToRequest{LastInsertIDExported: true}
	require.Panics(t, func() { _ = rt.migrateConnectionTo(t.Context(), req) })
	require.True(t, p.closed)
	require.True(t, rt.shouldCloseConnection())
	require.False(t, rt.mc.tryBeginOperation())
}

func TestMigrationPreflightRefusalPreservesTarget(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	p := &migrationDisposalProtocol{testMysqlWriter: &testMysqlWriter{}}
	rt := NewRoutine(t.Context(), p, &config.FrontendParameters{})
	p.controller = rt.mc
	rt.setSession(ses)
	t.Cleanup(rt.cancelRoutineFunc)
	require.Error(t, rt.migrateConnectionTo(t.Context(), &query.MigrateConnToRequest{}))
	require.False(t, p.closed)
	require.True(t, rt.mc.tryBeginOperation())
	rt.mc.endOperation()
}
