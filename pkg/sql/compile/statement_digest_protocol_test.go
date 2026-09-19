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

package compile

import (
	"errors"
	"strings"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/version"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func statementHashProtocolScope(proc *process.Process) *Scope {
	op := projection.NewArgument()
	op.ProjectList = []*planpb.Expr{{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{
			Obj:     int64(planfunction.MO_STATEMENT_HASH) << 32,
			ObjName: "mo_statement_hash",
		}}},
	}}
	return &Scope{
		Magic:    Remote,
		Proc:     proc,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}
}

func TestIsFullBuildCommitID(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   string
		want bool
	}{
		{name: "full lowercase SHA-1", id: strings.Repeat("a", 40), want: true},
		{name: "abbreviated SHA", id: strings.Repeat("a", 7)},
		{name: "wrong length", id: strings.Repeat("a", 41)},
		{name: "non-hex character", id: strings.Repeat("g", 40)},
		{name: "uppercase spelling", id: strings.Repeat("A", 40)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, version.IsFullBuildCommitID(tc.id))
		})
	}
}

func TestStatementHashDestinationProtocolValidation(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	oldBuildCommitID := version.BuildCommitID
	version.BuildCommitID = strings.Repeat("a", 40)
	client.buildCommitID = version.BuildCommitID
	t.Cleanup(func() { version.BuildCommitID = oldBuildCommitID })
	scope := statementHashProtocolScope(c.proc)
	defer scope.RootOp.Release()

	for _, version := range []int64{
		defines.MORPCVersion68, // Previously used by a different mainline capability.
		defines.MORPCVersion85, // Integer-parameter support alone is below this feature's v86 gate.
		defines.MORPCVersion86, // Function support alone predates the v87 hash contract.
	} {
		client.version = version
		_, err := encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "remote destination does not support MO_STATEMENT_HASH")
	}

	client.version = defines.MORPCVersion87
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	client.buildCommitID = "different-build"
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "same build commit")
	client.buildCommitID = version.BuildCommitID

	// The destination is probed for every send, so a worker downgrade after
	// compile-time placement is fenced before a sender is created.
	for _, version := range []int64{
		defines.MORPCVersion68,
		defines.MORPCVersion85,
		defines.MORPCVersion86,
	} {
		client.version = version
		_, err = encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "remote destination does not support MO_STATEMENT_HASH")
	}
	client.version = defines.MORPCVersion87
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)

	// A missing local build identity cannot establish formatter equivalence and
	// must fail before probing or dispatching to the worker.
	localBuildCommitID := version.BuildCommitID
	for _, invalidID := range []string{"", version.BuildCommitID[:7], strings.Repeat("g", 40)} {
		version.BuildCommitID = invalidID
		probeCalls := client.calls
		_, err = encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "full 40-character local build commit ID")
		require.Equal(t, probeCalls, client.calls,
			"an inexact source identity must fail before worker probing or dispatch")
	}
	version.BuildCommitID = localBuildCommitID

	client.buildCommitID = ""
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "same build commit")
	require.Equal(t, client.calls, client.releases)
}

func TestStatementHashRemoteForwardingPreservesOriginBuildAcrossHops(t *testing.T) {
	const originBuild = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const replacementBuild = "cccccccccccccccccccccccccccccccccccccccc"

	c, client := expressionProtocolTestCompile(t)
	oldBuildCommitID := version.BuildCommitID
	version.BuildCommitID = originBuild
	t.Cleanup(func() { version.BuildCommitID = oldBuildCommitID })
	client.version = defines.MORPCVersion87
	client.buildCommitID = originBuild

	remoteSession, err := process.ConvertToProcessSessionInfo(pipeline.SessionInfo{
		StatementHashExpectedBuildCommitId: originBuild,
	})
	require.NoError(t, err)
	c.proc.Base.SessionInfo = remoteSession

	scope := statementHashProtocolScope(c.proc)
	defer scope.RootOp.Release()

	// The first probe is against A, and a later worker replacement must be
	// probed again rather than inheriting a cached success.
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.Equal(t, 1, client.calls)
	client.buildCommitID = replacementBuild
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "same build commit")
	require.Equal(t, 2, client.calls)
}

func TestStatementHashRemoteForwardingRejectsIntermediateBuildBeforeProbe(t *testing.T) {
	const originBuild = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const intermediateBuild = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	c, client := expressionProtocolTestCompile(t)
	oldBuildCommitID := version.BuildCommitID
	version.BuildCommitID = originBuild
	t.Cleanup(func() { version.BuildCommitID = oldBuildCommitID })
	client.version = defines.MORPCVersion87
	client.buildCommitID = originBuild

	remoteSession, err := process.ConvertToProcessSessionInfo(pipeline.SessionInfo{
		StatementHashExpectedBuildCommitId: originBuild,
	})
	require.NoError(t, err)
	c.proc.Base.SessionInfo = remoteSession

	scope := statementHashProtocolScope(c.proc)
	defer scope.RootOp.Release()

	// A B binary receiving work from A must reject before any destination
	// capability probe or dispatch; it cannot reinterpret the request as B.
	version.BuildCommitID = intermediateBuild
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "worker build does not match")
	require.Zero(t, client.calls)
}

func TestStatementHashDestinationRejectsUnknownOrFailedProbe(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	oldBuildCommitID := version.BuildCommitID
	version.BuildCommitID = strings.Repeat("a", 40)
	client.buildCommitID = version.BuildCommitID
	t.Cleanup(func() { version.BuildCommitID = oldBuildCommitID })
	scope := statementHashProtocolScope(c.proc)
	defer scope.RootOp.Release()

	client.version = defines.MORPCVersion87
	scope.NodeInfo.Id = "missing-worker"
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination does not support MO_STATEMENT_HASH")

	scope.NodeInfo.Id = "old-worker"
	client.customResponse = true
	client.response = nil
	client.sendErr = errors.New("capability probe failed")
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination does not support MO_STATEMENT_HASH")

	// A missing pipeline destination is also fail-closed when validation is
	// called directly (fillPipeline normally materializes an empty NodeInfo).
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion87)
	err = validateStatementHashDestination(c.proc, &pipeline.Pipeline{})
	require.ErrorContains(t, err, "requires a versioned remote destination")

	// A coordinator below v86 is rejected by the coordinator gate before any
	// worker probe; a worker version cannot make the sender safe by itself.
	client.customResponse = false
	client.sendErr = nil
	for _, version := range []int64{
		defines.MORPCVersion72,
		defines.MORPCVersion73,
		defines.MORPCVersion74,
		defines.MORPCVersion75,
		defines.MORPCVersion76,
		defines.MORPCVersion77,
		defines.MORPCVersion78,
		defines.MORPCVersion79,
		defines.MORPCVersion80,
		defines.MORPCVersion81,
		defines.MORPCVersion82,
		defines.MORPCVersion83,
		defines.MORPCVersion84,
		defines.MORPCVersion85,
	} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		probeCalls := client.calls
		_, err = encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "MO_STATEMENT_HASH remote execution requires MORPC protocol version 86")
		require.Equal(t, probeCalls, client.calls, "coordinator gate must reject before probing the worker")
	}
}
