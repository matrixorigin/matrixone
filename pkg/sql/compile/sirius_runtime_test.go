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
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/sidecarflight"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

type siriusRuntimeTestProtector struct{ failUnregister bool }

func (*siriusRuntimeTestProtector) Begin(context.Context) (
	func(context.Context, []byte, []string, time.Time) error,
	func(context.Context, []byte) error,
	func(),
	error,
) {
	return func(context.Context, []byte, []string, time.Time) error { return nil },
		func(context.Context, []byte) error { return nil }, func() {}, nil
}

func (p *siriusRuntimeTestProtector) Unregister(context.Context, []byte) error {
	if p.failUnregister {
		return errors.New("test unregister failure")
	}
	return nil
}

type siriusRuntimeTestProvider struct{ schema []byte }

func (p siriusRuntimeTestProvider) PrepareSnapshotRead(context.Context, substrait.Read, []byte) (substrait.SnapshotFacts, error) {
	return substrait.SnapshotFacts{Manifest: []byte("manifest"), CanonicalSchema: p.schema}, nil
}

func TestSiriusRuntimeValidationAndLookup(t *testing.T) {
	require.Error(t, (*SiriusRuntime)(nil).Validate())
	require.NoError(t, (*SiriusRuntime)(nil).Close(context.Background()))

	nondurable := substrait.NewLeaseManager(1, &siriusRuntimeTestProtector{})
	invalid := &SiriusRuntime{
		Flight: &sidecarflight.Runtime{}, Leases: nondurable, Resolver: &substrait.ResolverServer{},
		AuthorizedClientSPKIHash: make([]byte, 32), DataDir: t.TempDir(), LeaseTTL: time.Minute, CleanupTimeout: time.Second,
	}
	require.Error(t, invalid.Validate())
	leases := substrait.NewPersistentLeaseManager(1, &siriusRuntimeTestProtector{}, siriusJournalStub{})
	require.NoError(t, leases.Replay(context.Background()))
	valid := &SiriusRuntime{
		Flight:   &sidecarflight.Runtime{},
		Leases:   leases,
		Resolver: &substrait.ResolverServer{}, AuthorizedClientSPKIHash: make([]byte, 32),
		DataDir: t.TempDir(), LeaseTTL: time.Minute, CleanupTimeout: time.Second,
	}
	require.NoError(t, valid.Validate())

	service := "sirius-runtime-lookup-test"
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
	moruntime.SetupServiceBasedRuntime(service, rt)
	_, ok := lookupSiriusRuntime("sirius-runtime-missing")
	require.False(t, ok)
	rt.SetGlobalVariables(SiriusRuntimeKey, "wrong type")
	_, ok = lookupSiriusRuntime(service)
	require.False(t, ok)
	rt.SetGlobalVariables(SiriusRuntimeKey, &SiriusRuntime{})
	_, ok = lookupSiriusRuntime(service)
	require.False(t, ok)
	rt.SetGlobalVariables(SiriusRuntimeKey, valid)
	actual, ok := lookupSiriusRuntime(service)
	require.True(t, ok)
	require.Same(t, valid, actual)
	require.True(t, rt.CompareAndDeleteGlobalVariables(SiriusRuntimeKey, valid))
}

func TestRecoverAdmittedReadReleasesOrRetainsRetryableOwner(t *testing.T) {
	require.Error(t, (*SiriusRuntime)(nil).recoverAdmittedRead(context.Background(), 0, nil, nil))
	query := &planpb.Query{
		StmtType: planpb.Query_SELECT, Steps: []int32{0}, Headings: []string{"a"},
		Nodes: []*planpb.Node{{
			NodeId: 0, NodeType: planpb.Node_TABLE_SCAN,
			ObjRef: &planpb.ObjectRef{Db: 7, Obj: 42, ObjName: "t"},
			TableDef: &planpb.TableDef{TblId: 42, Version: 3, Name: "t", TableType: "r", Cols: []*planpb.ColDef{{
				Name: "a", ColId: 11, Seqnum: 5, Typ: planpb.Type{Id: int32(types.T_int64)},
			}}},
		}},
	}
	candidate, err := substrait.Export(query)
	require.NoError(t, err)
	protector := &siriusRuntimeTestProtector{}
	leases := substrait.NewLeaseManager(1, protector)
	admitted, err := substrait.AdmitReads(context.Background(), substrait.AdmissionRequest{
		Candidate: candidate, Provider: siriusRuntimeTestProvider{schema: candidate.Reads()[0].Schema}, Leases: leases,
		AccountID: 1, QueryID: bytes.Repeat([]byte{'q'}, 16), SnapshotTS: make([]byte, 12),
		AuthorizedClientSPKIHash: make([]byte, 32), TTL: time.Minute, ReadOnly: true,
	})
	require.NoError(t, err)
	require.Len(t, leases.PendingExecutions(), 1)
	runtime := &SiriusRuntime{Flight: &sidecarflight.Runtime{}, Leases: leases, CleanupTimeout: time.Second}
	plan := &SiriusReadPlan{ReadRefs: admitted.ReadRefs}
	require.NoError(t, runtime.recoverAdmittedRead(nil, 1, bytes.Repeat([]byte{'q'}, 16), plan))
	require.Empty(t, leases.PendingExecutions())

	admitted, err = substrait.AdmitReads(context.Background(), substrait.AdmissionRequest{
		Candidate: candidate, Provider: siriusRuntimeTestProvider{schema: candidate.Reads()[0].Schema}, Leases: leases,
		AccountID: 1, QueryID: bytes.Repeat([]byte{'r'}, 16), SnapshotTS: make([]byte, 12),
		AuthorizedClientSPKIHash: make([]byte, 32), TTL: time.Minute, ReadOnly: true,
	})
	require.NoError(t, err)
	protector.failUnregister = true
	err = runtime.recoverAdmittedRead(context.Background(), 0, nil, &SiriusReadPlan{ReadRefs: admitted.ReadRefs})
	require.ErrorContains(t, err, "test unregister failure")
	require.ErrorContains(t, err, "invalid replayed execution")
	require.Len(t, leases.PendingExecutions(), 1)
	protector.failUnregister = false
	require.NoError(t, releaseReadRefs(context.Background(), leases, admitted.ReadRefs))
}

func TestSiriusCompileFastRejections(t *testing.T) {
	require.True(t, siriusStatementEligible(&tree.Select{}))
	require.False(t, siriusStatementEligible(&tree.Select{IsPerform: true}))
	require.False(t, siriusStatementEligible(&tree.Select{Ep: &tree.ExportParam{}}))
	require.False(t, siriusStatementEligible(nil))

	requested := WithSiriusOffload(context.Background())
	for _, c := range []*Compile{
		nil,
		{},
		{isPrepare: true, stmt: &tree.Select{}},
		{isInternal: true, stmt: &tree.Select{}},
		{stmt: &tree.Select{IsPerform: true}},
	} {
		offloaded, err := c.tryCompileSiriusRead(requested, nil)
		require.NoError(t, err)
		require.False(t, offloaded)
	}
	offloaded, err := (&Compile{}).tryCompileSiriusRead(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, offloaded)

	require.NoError(t, (*siriusReadOwner)(nil).finish(context.Background(), false))
	err = (&Compile{}).runSiriusRead(context.Background())
	require.ErrorContains(t, err, "missing Sirius execution owner")
}

func TestSQLSelectLimitIsMaterializedBeforeSiriusExport(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.ApplySQLSelectLimit = true
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == plan2.SQLSelectLimitVariable {
			return uint64(3), nil
		}
		return nil, nil
	})
	t.Cleanup(proc.Free)

	query := &planpb.Query{
		StmtType: planpb.Query_SELECT, Steps: []int32{0}, Headings: []string{"a"},
		ApplySqlSelectLimit: true,
		Nodes: []*planpb.Node{{
			NodeId: 0, NodeType: planpb.Node_TABLE_SCAN,
			ObjRef: &planpb.ObjectRef{Db: 7, Obj: 42, ObjName: "t"},
			TableDef: &planpb.TableDef{
				TblId: 42, Version: 3, Name: "t", TableType: "r",
				Cols: []*planpb.ColDef{{
					Name: "a", ColId: 11, Seqnum: 5,
					Typ: planpb.Type{Id: int32(types.T_int64)},
				}},
			},
		}},
	}
	queryPlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}
	c := &Compile{proc: proc}
	require.NoError(t, c.materializeSQLSelectLimit(queryPlan))
	require.False(t, query.ApplySqlSelectLimit)
	require.Equal(t, uint64(3), query.Nodes[0].Limit.GetLit().GetU64Val())

	candidate, err := substrait.Export(query)
	require.NoError(t, err)
	wire, err := candidate.Build(map[int32][]byte{0: {1}})
	require.NoError(t, err)
	offloadedPlan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, offloadedPlan))
	fetch := offloadedPlan.Relations[0].GetRoot().Input.GetFetch()
	require.NotNil(t, fetch)
	require.Equal(t, int64(3), fetch.GetCount())
}
