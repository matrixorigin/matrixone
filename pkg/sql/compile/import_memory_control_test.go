// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func memoryControlCN(id string) *memoryCNManifest {
	return &memoryCNManifest{id: id, addr: id, processID: id, manifest: MemoryManifest{TargetUnit: 64, Stages: []MemoryStageDescriptor{{Role: MemoryStageCSV, Count: 1, Schema: []types.Type{types.T_int64.ToType()}}}}}
}

func TestMemoryControlManifestEncoding(t *testing.T) {
	request := MemoryControlRequest{Action: MemoryControlOpen, ExpectedProcessID: runtime.ProcessInstanceID(), Manifest: &MemoryManifest{TargetUnit: 64, IncomingUnit: 128, Parallel: 3, Stages: memoryControlCN("A").manifest.Stages}}
	data, err := EncodeMemoryControlRequest(request)
	require.NoError(t, err)
	decoded, matched, err := DecodeMemoryControlRequest(data)
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, request, decoded)
	_, matched, err = DecodeMemoryControlRequest([]byte("legacy"))
	require.NoError(t, err)
	require.False(t, matched)
	_, matched, err = DecodeMemoryControlRequest([]byte(MemoryControlMagic + "{"))
	require.True(t, matched)
	require.Error(t, err)
	response := MemoryControlResponse{Token: "token", UnitBytes: 32, RetryParallel: 2, Error: "pressure"}
	data, err = EncodeMemoryControlResponse(response)
	require.NoError(t, err)
	actual, err := DecodeMemoryControlResponse(data)
	require.NoError(t, err)
	require.Equal(t, response, actual)
}

func TestMemoryControlOppositeOriginsUseSameCNOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	a, b := memoryControlCN("A"), memoryControlCN("B")
	slots := map[string]chan struct{}{"A": make(chan struct{}, 1), "B": make(chan struct{}, 1)}
	firstA := make(chan struct{}, 1)
	open := func(ctx context.Context, entry *memoryCNManifest) (*statementMemoryControl, error) {
		select {
		case slots[entry.id] <- struct{}{}:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
		if entry.id == "A" {
			select {
			case firstA <- struct{}{}:
			default:
			}
		}
		return &statementMemoryControl{unit: 64, memory: &process.ImportMemory{UnitBytes: 64}, closeLocal: func(error) { <-slots[entry.id] }}, nil
	}
	type result struct {
		controls []*statementMemoryControl
		err      error
	}
	resultA, resultB := make(chan result, 1), make(chan result, 1)
	go func() {
		controls, _, err := acquireMemoryControls(ctx, []*memoryCNManifest{a, b}, open)
		resultA <- result{controls, err}
	}()
	select {
	case <-firstA:
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	go func() {
		controls, _, err := acquireMemoryControls(ctx, []*memoryCNManifest{b, a}, open)
		resultB <- result{controls, err}
	}()
	var first result
	select {
	case first = <-resultA:
	case <-ctx.Done():
		t.Fatal("first origin could not acquire full CN set")
	}
	require.NoError(t, first.err)
	closeMemoryControls(first.controls, nil)
	var second result
	select {
	case second = <-resultB:
	case <-ctx.Done():
		t.Fatal("reverse origin deadlocked after first owner released")
	}
	require.NoError(t, second.err)
	closeMemoryControls(second.controls, nil)
	require.Empty(t, slots["A"])
	require.Empty(t, slots["B"])
}

func TestMemoryControlAliasesUsePhysicalProcessOrderAcrossQueries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	// Q1 uses A(P), C(Q); Q2 uses B(P), C(Q). CN ordering A<C<B
	// would acquire P->Q in Q1 and Q->P in Q2, even though neither query
	// contains the same physical process twice.
	a, b, c := memoryControlCN("A"), memoryControlCN("Z"), memoryControlCN("M")
	a.processID, b.processID, c.processID = "P", "P", "Q"
	slots := map[string]chan struct{}{"P": make(chan struct{}, 1), "Q": make(chan struct{}, 1)}
	acquiredP, releaseFirst := make(chan struct{}), make(chan struct{})
	secondAttempt := make(chan string, 1)
	open := func(ctx context.Context, entry *memoryCNManifest) (*statementMemoryControl, error) {
		select {
		case slots[entry.processID] <- struct{}{}:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
		return &statementMemoryControl{unit: 64, memory: &process.ImportMemory{UnitBytes: 64}, closeLocal: func(error) { <-slots[entry.processID] }}, nil
	}
	type result struct {
		controls []*statementMemoryControl
		err      error
	}
	first, second := make(chan result, 1), make(chan result, 1)
	go func() {
		controls, _, err := acquireMemoryControls(ctx, []*memoryCNManifest{a, c}, func(ctx context.Context, entry *memoryCNManifest) (*statementMemoryControl, error) {
			h, err := open(ctx, entry)
			if entry == a && err == nil {
				close(acquiredP)
				select {
				case <-releaseFirst:
				case <-ctx.Done():
				}
			}
			return h, err
		})
		first <- result{controls, err}
	}()
	select {
	case <-acquiredP:
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	go func() {
		isFirst := true
		controls, _, err := acquireMemoryControls(ctx, []*memoryCNManifest{c, b}, func(ctx context.Context, entry *memoryCNManifest) (*statementMemoryControl, error) {
			if isFirst {
				secondAttempt <- entry.processID
				isFirst = false
			}
			return open(ctx, entry)
		})
		second <- result{controls, err}
	}()
	select {
	case physical := <-secondAttempt:
		// Assert before allowing Q1 to continue: this fails deterministically
		// under the old CN-only order instead of relying on a timeout race.
		require.Equal(t, "P", physical)
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	close(releaseFirst)
	for _, results := range []chan result{first, second} {
		select {
		case actual := <-results:
			require.NoError(t, actual.err)
			closeMemoryControls(actual.controls, nil)
		case <-ctx.Done():
			t.Fatal("physical resource ordering did not permit progress")
		}
	}
	require.Empty(t, slots["P"])
	require.Empty(t, slots["Q"])
}

func TestMemoryControlPartialFailureReleasesBeforeRetry(t *testing.T) {
	var order []string
	expected := errors.New("retry lower DOP")
	controls, _, err := acquireMemoryControls(context.Background(), []*memoryCNManifest{memoryControlCN("C"), memoryControlCN("B"), memoryControlCN("A")}, func(_ context.Context, entry *memoryCNManifest) (*statementMemoryControl, error) {
		order = append(order, "open"+entry.id)
		if entry.id == "C" {
			return nil, expected
		}
		return &statementMemoryControl{unit: 64, memory: &process.ImportMemory{}, closeLocal: func(error) { order = append(order, "close"+entry.id) }}, nil
	})
	require.ErrorIs(t, err, expected)
	require.Nil(t, controls)
	require.Equal(t, []string{"openA", "openB", "openC", "closeB", "closeA"}, order)
}

type memoryControlTestStream struct {
	replies chan morpc.Message
	send    func(context.Context, morpc.Message) error
	mu      sync.Mutex
	closed  []bool
}

func (s *memoryControlTestStream) ID() uint64 { return 1 }
func (s *memoryControlTestStream) Send(ctx context.Context, msg morpc.Message) error {
	return s.send(ctx, msg)
}
func (s *memoryControlTestStream) Receive() (chan morpc.Message, error) { return s.replies, nil }
func (s *memoryControlTestStream) Close(conn bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = append(s.closed, conn)
	return nil
}

func TestMemoryControlEndLostACKClosesConnection(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "ack", true: "lost"}[lost], func(t *testing.T) {
			stream := &memoryControlTestStream{replies: make(chan morpc.Message, 1)}
			stream.send = func(ctx context.Context, raw morpc.Message) error {
				require.NoError(t, ctx.Err(), "cleanup has its own context after query cancellation")
				req, matched, err := DecodeMemoryControlRequest(raw.(*pipeline.Message).Data)
				require.NoError(t, err)
				require.True(t, matched)
				require.Equal(t, MemoryControlEnd, req.Action)
				if lost {
					close(stream.replies)
				} else {
					data, err := EncodeMemoryControlResponse(MemoryControlResponse{})
					require.NoError(t, err)
					stream.replies <- &pipeline.Message{Data: data}
				}
				return nil
			}
			handle := &statementMemoryControl{token: "opaque", unit: 64, stream: stream, receive: stream.replies}
			handle.close(nil)
			require.Equal(t, []bool{lost}, stream.closed)
		})
	}
}

func TestMemoryControlManifestIncludesEveryRemoteScope(t *testing.T) {
	c := memoryCompileFixture(t, 8)
	remoteA := memorySourceScope("127.0.0.2:6001", 3, 1)
	remoteB := memorySourceScope("127.0.0.2:6001", 5, 4)
	for _, scope := range []*Scope{remoteA, remoteB} {
		scope.Magic = Remote
		scope.Proc = c.proc
	}
	c.scopes = append(c.scopes, remoteA, remoteB)
	manifests, locations, err := c.collectMemoryManifests(64 << 20)
	require.NoError(t, err)
	require.Len(t, manifests, 2)
	for _, entry := range manifests {
		require.Zero(t, entry.manifest.IncomingUnit, "global commit happens before any execution RPC can send a frame")
	}
	require.Equal(t, remoteA.NodeInfo.Addr, locations[remoteA])
	require.Equal(t, remoteB.NodeInfo.Addr, locations[remoteB])
	remote := manifests[remoteA.NodeInfo.Addr]
	_, spec, controlled, err := memoryManifestLayout(remote.manifest, 32<<20)
	require.NoError(t, err)
	require.True(t, controlled)
	require.Equal(t, uint32(8), spec.Inputs[0].Count, "the first RPC must reserve both present and future remote source scopes")
	c.scopes[0].MemoryToken = "closed-previous-generation"
	remoteA.MemoryToken = "closed-previous-generation"
	c.endImportMemory()
	require.Empty(t, c.scopes[0].MemoryToken)
	require.Empty(t, remoteA.MemoryToken)
}

type memoryCapabilityClient struct {
	send     func(string, *query.Request) (*query.Response, error)
	releases int
}

func (c *memoryCapabilityClient) ServiceID() string { return "test" }
func (c *memoryCapabilityClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}
func (c *memoryCapabilityClient) SendMessage(_ context.Context, addr string, req *query.Request) (*query.Response, error) {
	return c.send(addr, req)
}
func (c *memoryCapabilityClient) Release(*query.Response) { c.releases++ }
func (c *memoryCapabilityClient) Close() error            { return nil }

func TestMemoryControlCapabilityLegacyAndNetworkFailureDiffer(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	entries := []*memoryCNManifest{{queryAddr: "old"}, {queryAddr: "new"}}
	client := &memoryCapabilityClient{}
	c.proc.Base.QueryClient = client
	client.send = func(addr string, req *query.Request) (*query.Response, error) {
		require.Equal(t, query.CmdMethod_GetProtocolVersion, req.CmdMethod)
		require.NotNil(t, req.GetProtocolVersion)
		return &query.Response{GetProtocolVersion: &query.GetProtocolVersionResponse{MemoryAdmission: addr == "new", MemoryAdmissionProcessId: addr}}, nil
	}
	capable, err := c.memoryCapabilities(context.Background(), entries)
	require.NoError(t, err)
	require.False(t, capable)
	require.Equal(t, 2, client.releases)
	network := errors.New("connection lost")
	client.send = func(string, *query.Request) (*query.Response, error) { return nil, network }
	capable, err = c.memoryCapabilities(context.Background(), entries)
	require.ErrorIs(t, err, network)
	require.False(t, capable)
	require.Equal(t, 2, client.releases, "failed RPC does not own a response to release")
}

func TestMemoryControlProcessAliasesAndMissingIdentityKeepWholeStatementLegacy(t *testing.T) {
	for _, test := range []struct {
		name   string
		ids    []string
		legacy bool
	}{
		{name: "different-processes", ids: []string{"Q", "P"}},
		{name: "same-process-aliases", ids: []string{"P", "P"}, legacy: true},
		{name: "earlier-admission-peer", ids: []string{"", "P"}, legacy: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			entries := []*memoryCNManifest{{id: "A", queryAddr: "first"}, {id: "Z", queryAddr: "second"}}
			client := &memoryCapabilityClient{send: func(addr string, _ *query.Request) (*query.Response, error) {
				index := 0
				if addr == "second" {
					index = 1
				}
				return &query.Response{GetProtocolVersion: &query.GetProtocolVersionResponse{MemoryAdmission: true, MemoryAdmissionProcessId: test.ids[index]}}, nil
			}}
			c.proc.Base.QueryClient = client
			capable, err := c.memoryCapabilities(context.Background(), entries)
			require.NoError(t, err)
			require.Equal(t, !test.legacy, capable)
			require.Equal(t, 2, client.releases)
			if !test.legacy {
				require.Equal(t, "Z", entries[0].id, "capability discovery orders by physical process before CN UUID")
			}
		})
	}
}

func TestMemoryControlLocalProcessRestartRejectedBeforeAdmission(t *testing.T) {
	entry := memoryControlCN("local")
	entry.processID = runtime.ProcessInstanceID() + "-previous-incarnation"
	// A nil throttler cannot admit anything: identity rejection must precede
	// querying admission capacity, registering a token, or touching a queue.
	handle, err := openLocalMemory(context.Background(), nil, entry)
	require.ErrorContains(t, err, "process identity changed")
	require.Nil(t, handle)
}

func TestMemoryControlUsesEndpointIdentityDespiteStalePlannedUUID(t *testing.T) {
	old := runtime.ServiceRuntime("")
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "", zap.NewNop())
	runtime.SetupServiceBasedRuntime("", rt)
	t.Cleanup(func() { runtime.SetupServiceBasedRuntime("", old) })
	c := memoryCompileFixture(t, 1)
	address := "127.0.0.2:6001"
	stale := metadata.CNService{ServiceID: "A-stale-planned-id", PipelineServiceAddress: "127.0.0.3:6001", QueryAddress: "query-A"}
	actual := metadata.CNService{ServiceID: "B-actual-endpoint", PipelineServiceAddress: address, QueryAddress: "query-B"}
	for _, services := range [][]metadata.CNService{{stale, actual}, {actual, stale}} {
		func() {
			cluster := clusterservice.NewMOCluster("", nil, time.Hour, clusterservice.WithDisableRefresh(), clusterservice.WithServices(services, nil))
			defer cluster.Close()
			rt.SetGlobalVariables(runtime.ClusterService, cluster)
			entries, err := c.resolveMemoryCNs(map[string]*memoryCNManifest{address: {addr: address, id: stale.ServiceID}})
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Equal(t, actual.ServiceID, entries[0].id, "every origin must order the CN that owns the actual control connection")
			require.Equal(t, actual.QueryAddress, entries[0].queryAddr)
		}()
	}
}

type waitingControlPipelineClient struct {
	cnclient.PipelineClient
	entered       chan struct{}
	emergencyStop chan struct{}
}

func (c *waitingControlPipelineClient) NewControlStream(ctx context.Context, _ string) (morpc.Stream, error) {
	close(c.entered)
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-c.emergencyStop:
		return nil, errors.New("test emergency cleanup")
	}
}

func TestMemoryControlBackendAcquisitionObservesQueryCancellation(t *testing.T) {
	old := runtime.ServiceRuntime("")
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "", zap.NewNop())
	runtime.SetupServiceBasedRuntime("", rt)
	t.Cleanup(func() { runtime.SetupServiceBasedRuntime("", old) })
	client := &waitingControlPipelineClient{entered: make(chan struct{}), emergencyStop: make(chan struct{})}
	rt.SetGlobalVariables(runtime.PipelineClient, client)
	t.Cleanup(func() { close(client.emergencyStop) })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() {
		_, err := openRemoteMemory(ctx, "", "busy-control-pool", MemoryManifest{}, runtime.ProcessInstanceID())
		result <- err
	}()
	select {
	case <-client.entered:
	case <-time.After(time.Second):
		t.Fatal("control backend acquisition was not entered")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("query cancellation did not interrupt control backend acquisition")
	}
}

func TestMemoryControlNonCSVStatementsKeepExistingMemoryPaths(t *testing.T) {
	for _, op := range []vm.Operator{&hashjoin.HashJoin{LeftTypes: []types.Type{types.T_text.ToType()}, RightTypes: []types.Type{types.T_text.ToType()}}, &multi_update.MultiUpdate{}} {
		t.Run(fmt.Sprint(op.OpType()), func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			c.scopes[0].RootOp = op
			m := &compilerMemoryAdmission{available: 0}
			require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
			require.Nil(t, c.importMemory)
			require.Empty(t, c.importMemoryControls)
			require.Zero(t, m.tries, "ordinary scan rows have no CSV record bound or source byte slicing")
		})
	}
}

func TestMemoryControlUnsupportedImportWritersKeepWholeStatementLegacy(t *testing.T) {
	cases := map[string]vm.Operator{
		"partition-insert":        insert.NewPartitionInsert(&insert.Insert{ToWriteS3: true}, 1),
		"partition-multi-update":  multi_update.NewPartitionMultiUpdate(&multi_update.MultiUpdate{Action: multi_update.UpdateWriteS3}, 1),
		"memory-table-raw-output": &insert.Insert{ToWriteS3: true, InsertCtx: &insert.InsertCtx{Engine: &memoryengine.BindedEngine{}}},
		"derived-composite-key":   &preinsert.PreInsert{CompPkeyExpr: &plan.Expr{}},
		"derived-cluster-key":     &preinsert.PreInsert{ClusterByExpr: &plan.Expr{}},
	}
	for name, op := range cases {
		t.Run(name, func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			remote := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "other-cn", Addr: "127.0.0.2:6001", Mcpu: 1}, RootOp: op, MemoryToken: "old-attempt"}
			c.scopes[0].PreScopes = []*Scope{remote}
			m := &compilerMemoryAdmission{failure: errors.New("admission must not be attempted")}
			require.NoError(t, c.beginStatementImportMemory(context.Background(), m), "unsupported nested writers must select legacy before CN metadata/capability/Open")
			require.Zero(t, m.tries)
			require.Zero(t, m.waits)
			require.Nil(t, c.importMemory)
			require.Nil(t, c.proc.Base.ImportMemory)
			require.Empty(t, c.importMemoryControls)
			require.Empty(t, remote.MemoryToken, "retry must not retain an old controlled execution token")
		})
	}
}

func TestMemoryControlPlainInsertRemainsControlledOnlyWithBoundedSource(t *testing.T) {
	table := &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}}, {Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar), Width: 1024}}}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}}
	for _, source := range []bool{false, true} {
		t.Run(fmt.Sprint(source), func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			if !source {
				c.scopes = nil
			}
			pre := &preinsert.PreInsert{TableDef: table}
			writer := &insert.Insert{ToWriteS3: true, InsertCtx: &insert.InsertCtx{TableDef: table}}
			writer.AppendChild(pre)
			c.scopes = append(c.scopes, &Scope{NodeInfo: engine.Node{Addr: c.addr, Mcpu: 1}, RootOp: writer})
			sentinel := errors.New("bounded plain insert reached admission")
			m := &compilerMemoryAdmission{failure: sentinel}
			err := c.beginStatementImportMemory(context.Background(), m)
			if source {
				require.ErrorIs(t, err, sentinel)
				require.Equal(t, 1, m.tries, "ordinary source + raw single PK writer must remain eligible")
			} else {
				require.NoError(t, err)
				require.Zero(t, m.tries, "a table writer alone has no bounded CSV input contract")
			}
			require.Nil(t, c.importMemory)
		})
	}
}

func TestMemoryControlWriterEligibilityDoesNotExtendToUnrelatedOperators(t *testing.T) {
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	serial := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varbinary)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "serial"}, Args: []*plan.Expr{col, col}}}}
	for _, op := range []vm.Operator{
		&insert.Insert{InsertCtx: &insert.InsertCtx{Engine: &memoryengine.BindedEngine{}}},
		&projection.Projection{ProjectList: []*plan.Expr{serial}},
	} {
		c := memoryCompileFixture(t, 1)
		c.scopes = append(c.scopes, &Scope{RootOp: op})
		require.True(t, c.supportsBoundedImportKeys(), "this gate covers memoryengine S3 raw output and the two PreInsert derived fields; it does not redefine Project expression support")
	}
}

func TestMemoryControlUnknownVarlenKeysKeepWholeLoadLegacy(t *testing.T) {
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	unknown := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "unknown_varlen_result"}, Args: []*plan.Expr{col}}}}
	cases := map[string]vm.Operator{
		"build":                   &hashbuild.HashBuild{Conditions: []*plan.Expr{unknown}},
		"probe-left":              &hashjoin.HashJoin{EqConds: [][]*plan.Expr{{unknown}, {col}}},
		"probe-right":             &hashjoin.HashJoin{EqConds: [][]*plan.Expr{{col}, {unknown}}},
		"unknown-fixed-predicate": &hashjoin.HashJoin{EqConds: [][]*plan.Expr{{col}, {col}}, NonEqCond: &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "json_valid"}, Args: []*plan.Expr{col}}}}},
	}
	for name, op := range cases {
		t.Run(name, func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			c.scopes = append(c.scopes, &Scope{NodeInfo: engine.Node{Addr: c.addr, Mcpu: 1}, RootOp: op})
			m := &compilerMemoryAdmission{available: 0}
			require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
			require.Zero(t, m.tries, "no I/W ticket may coexist with a build whose key result has no allocation bound")
			require.Nil(t, c.importMemory)
		})
	}
	c := memoryCompileFixture(t, 1)
	serial := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varbinary)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "serial_full"}, Args: []*plan.Expr{col}}}}
	c.scopes = append(c.scopes, &Scope{RootOp: &hashbuild.HashBuild{Conditions: []*plan.Expr{serial}}})
	require.True(t, c.supportsBoundedImportKeys(), "known serialization keys retain the controlled LOAD path")
}

func TestMemoryControlManifestPreservesBoundedKeyExpressionTrees(t *testing.T) {
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	serial := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varbinary)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "serial_full"}, Args: []*plan.Expr{col, col}}}}
	join := &hashjoin.HashJoin{LeftTypes: []types.Type{types.T_varchar.ToType()}, RightTypes: []types.Type{types.T_varchar.ToType()}, EqConds: [][]*plan.Expr{{col}, {serial}}}
	stage, controlled, err := memoryStage(join, 2)
	require.NoError(t, err)
	require.True(t, controlled)
	manifest := MemoryManifest{TargetUnit: 1 << 20, Parallel: 2, Stages: []MemoryStageDescriptor{stage}}
	local, spec, controlled, err := memoryManifestLayout(manifest, 1<<20)
	require.NoError(t, err)
	require.True(t, controlled)
	data, err := EncodeMemoryControlRequest(MemoryControlRequest{Action: MemoryControlOpen, Manifest: &manifest})
	require.NoError(t, err)
	decoded, matched, err := DecodeMemoryControlRequest(data)
	require.NoError(t, err)
	require.True(t, matched)
	remote, remoteSpec, controlled, err := memoryManifestLayout(*decoded.Manifest, 1<<20)
	require.NoError(t, err)
	require.True(t, controlled)
	require.Equal(t, spec, remoteSpec)
	require.Equal(t, local.JoinInputBytes, remote.JoinInputBytes)
	require.GreaterOrEqual(t, remote.IOBytes, remote.JoinInputBytes, "key evaluation and frame compaction must have a complete pre-reserved spill workspace")
	expression := new(plan.Expr)
	require.NoError(t, expression.Unmarshal(decoded.Manifest.Stages[0].KeyExpressions[1][0]))
	require.Equal(t, "serial_full", expression.GetF().Func.ObjName)
	require.Len(t, expression.GetF().Args, 2, "repeated source columns must survive transport for memory amplification accounting")
}

func TestMemoryControlCapacityEnvelopeShrinksBeforeAdmission(t *testing.T) {
	key := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	for depth := 0; depth < 5; depth++ {
		key = &plan.Expr{Typ: plan.Type{Id: int32(types.T_varbinary)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "serial_full"}, Args: []*plan.Expr{key}}}}
	}
	join := &hashjoin.HashJoin{LeftTypes: []types.Type{types.T_varchar.ToType()}, RightTypes: []types.Type{types.T_varchar.ToType()}, EqConds: [][]*plan.Expr{{key}, {key}}}
	stage, ok, err := memoryStage(join, 1)
	require.NoError(t, err)
	require.True(t, ok)
	manifest := MemoryManifest{TargetUnit: 64 << 20, Parallel: 1, Stages: []MemoryStageDescriptor{stage}}
	_, _, _, err = memoryManifestLayout(manifest, 64<<20)
	var capacity *memoryEnvelopeCapacityError
	require.ErrorAs(t, err, &capacity, "large serialized keys can exceed vector capacity at the initial unit")
	_, _, _, err = memoryManifestLayout(manifest, 32<<20)
	require.NoError(t, err, "the same legal expression has a smaller calculable unit")
	reached := errors.New("reached atomic admission")
	provider := &compilerMemoryAdmission{failure: reached}
	_, err = admitMemoryManifest(context.Background(), provider, manifest)
	require.ErrorIs(t, err, reached)
	require.Equal(t, int64(32<<20), provider.lastUnit)
	require.Equal(t, 1, provider.tries, "no resources are requested until the envelope can be computed")
}

func TestMemoryControlGlobalCommitBoundsActualFramesOnEveryCN(t *testing.T) {
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	stage, ok, err := memoryStage(&hashjoin.HashJoin{LeftTypes: []types.Type{types.T_int64.ToType()}, RightTypes: []types.Type{types.T_int64.ToType()}, EqConds: [][]*plan.Expr{{col}, {col}}}, 1)
	require.NoError(t, err)
	require.True(t, ok)
	entries := []*memoryCNManifest{memoryControlCN("A"), memoryControlCN("B")}
	entries[0].manifest.TargetUnit = 1024
	entries[1].manifest.TargetUnit = 512
	for _, entry := range entries {
		entry.manifest.Stages = append(entry.manifest.Stages, stage)
	}
	handles, unit, err := acquireMemoryControls(context.Background(), entries, func(_ context.Context, entry *memoryCNManifest) (*statementMemoryControl, error) {
		meta, _, _, err := memoryManifestLayout(entry.manifest, entry.manifest.TargetUnit)
		return &statementMemoryControl{unit: entry.manifest.TargetUnit, memory: meta}, err
	})
	require.NoError(t, err)
	t.Cleanup(func() { closeMemoryControls(handles, nil) })
	require.Equal(t, int64(512), unit)
	for _, handle := range handles {
		require.Equal(t, unit, handle.memory.UnitBytes)
		require.LessOrEqual(t, unit, handle.unit)
	}
	mp := mpool.MustNewZero()
	src := batch.NewWithSize(1)
	src.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { src.Clean(mp) })
	values := make([]int64, 256)
	require.NoError(t, vector.AppendFixedList(src.Vecs[0], values, nil, mp))
	src.SetRowCount(len(values))
	rows := make([]int32, len(values))
	for i := range rows {
		rows[i] = int32(i)
	}
	file, err := os.CreateTemp(t.TempDir(), "committed-frames-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	frames, totalRows := 0, 0
	require.NoError(t, message.WriteJoinSpillRows(mp, src, rows, unit, func(frame *batch.Batch) error {
		frames++
		totalRows += frame.RowCount()
		payload := frame.Vecs[0].WindowSize(0, frame.RowCount())
		require.LessOrEqual(t, payload, unit)
		encoded, err := message.WriteJoinSpillFrame(file, frame)
		if err != nil {
			return err
		}
		for _, handle := range handles {
			require.LessOrEqual(t, payload, handle.unit, "a sender cannot produce the initial target after the global min commit")
			require.LessOrEqual(t, encoded, handle.memory.JoinInputBytes, "every CN reserved the complete receiver frame envelope before execution")
		}
		return nil
	}))
	require.Greater(t, frames, 1)
	require.Equal(t, len(values), totalRows)
}
