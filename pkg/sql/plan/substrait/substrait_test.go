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

package substrait

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

func TestExportBuildSupportedSubset(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_FILTER, Children: []int32{0}, FilterList: []*planpb.Expr{fn(">", boolType(), col(0), i64(7))}})
	q.Steps[0] = 1
	c, err := Export(q)
	require.NoError(t, err)
	require.Len(t, c.Reads(), 1)
	b, err := c.Build(map[int32][]byte{0: {1, 2, 3}})
	require.NoError(t, err)
	p := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(b, p))
	require.Equal(t, uint32(78), p.Version.MinorNumber)
	require.Equal(t, TaeReadTypeURL, p.Relations[0].GetRoot().Input.GetFilter().Input.GetRead().GetExtensionTable().Detail.TypeUrl)
}

func TestExportRejectsBeforeSnapshotAccess(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_JOIN, Children: []int32{0, 0}})
	q.Steps[0] = 1
	_, err := Export(q)
	require.ErrorContains(t, err, "unsupported operator")
}

func TestProjectEmitsOnlyMOProjectColumns(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{col(0)}})
	q.Steps[0] = 1
	c, err := Export(q)
	require.NoError(t, err)
	b, err := c.Build(map[int32][]byte{0: {1}})
	require.NoError(t, err)
	p := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(b, p))
	require.Equal(t, []int32{1}, p.Relations[0].GetRoot().Input.GetProject().Common.GetEmit().OutputMapping)
}

func TestExportRejectsOutOfRangeColumnsAndNonSuffixHiddenColumns(t *testing.T) {
	q := scanQuery()
	q.Nodes[0].ProjectList = []*planpb.Expr{col(1)}
	_, err := Export(q)
	require.ErrorContains(t, err, "outside input width")

	q = scanQuery()
	q.Nodes[0].TableDef.Cols = []*planpb.ColDef{
		{Name: "hidden", Hidden: true, Typ: i64Type()},
		{Name: "visible", Typ: i64Type()},
	}
	_, err = Export(q)
	require.ErrorContains(t, err, "non-suffix hidden")
}

func TestExportRejectsLiteralTypeMismatch(t *testing.T) {
	q := scanQuery()
	bad := &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "not-an-integer"}}}}
	q.Nodes[0].FilterList = []*planpb.Expr{fn("=", boolType(), col(0), bad)}
	_, err := Export(q)
	require.ErrorContains(t, err, "literal value does not match")
}

func TestAggregateDistinctAndBadArityAreRejected(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, AggList: []*planpb.Expr{fn("sum", i64Type(), col(0))}})
	distinct := uint64(q.Nodes[1].AggList[0].GetF().Func.Obj) | uint64(function.Distinct)
	q.Nodes[1].AggList[0].GetF().Func.Obj = int64(distinct)
	q.Steps[0] = 1
	_, err := Export(q)
	require.ErrorContains(t, err, "unsupported aggregate form")
	q = scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_FILTER, Children: []int32{0}, FilterList: []*planpb.Expr{fn("not", boolType(), fn(">", boolType(), col(0), i64(1)), fn(">", boolType(), col(0), i64(2)))}})
	q.Steps[0] = 1
	_, err = Export(q)
	require.ErrorContains(t, err, "requires 1 arguments")
}

func TestAggregateSortFetchLowering(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, GroupBy: []*planpb.Expr{col(0)}, AggList: []*planpb.Expr{fn("min", i64Type(), col(0))}}, &planpb.Node{NodeId: 2, NodeType: planpb.Node_SORT, Children: []int32{1}, OrderBy: []*planpb.OrderBySpec{{Expr: col(0), Flag: planpb.OrderBySpec_ASC}}, Limit: i64(5)})
	q.Steps[0] = 2
	c, err := Export(q)
	require.NoError(t, err)
	b, err := c.Build(map[int32][]byte{0: {1}})
	require.NoError(t, err)
	p := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(b, p))
	fetch := p.Relations[0].GetRoot().Input.GetFetch()
	require.Equal(t, int64(5), fetch.GetCount())
	require.NotNil(t, fetch.Input.GetSort().Input.GetAggregate())
}

func TestStarCountAggregateLowering(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, AggList: []*planpb.Expr{fn("starcount", i64Type(), i64(1))}})
	q.Steps[0] = 1
	c, err := Export(q)
	require.NoError(t, err)
	b, err := c.Build(map[int32][]byte{0: {1}})
	require.NoError(t, err)
	p := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(b, p))
	require.Len(t, p.Relations[0].GetRoot().Input.GetAggregate().Measures[0].Measure.Arguments, 1)
}

func TestTaeReadStrictWire(t *testing.T) {
	now := uint64(time.Now().UnixMilli())
	h := sha256.Sum256([]byte("x"))
	r := &TaeRead{ProtocolVersion: 1, ReadRef: bytes.Repeat([]byte{1}, 32), QueryID: []byte("q"), AccountID: 1, TableID: 2, SnapshotTS: make([]byte, 12), SchemaDigest: h[:], ManifestSHA256: h[:], CapabilityHash: CapabilityHash[:], ExpiresAtUnixMS: now + 1000}
	b, err := MarshalTaeRead(r)
	require.NoError(t, err)
	got, err := UnmarshalTaeRead(b, now)
	require.NoError(t, err)
	require.Equal(t, r.TableID, got.TableID)
	_, err = UnmarshalTaeRead(append(b, b[:2]...), now)
	require.Error(t, err)
}

type fakeProvider struct {
	calls int
	facts SnapshotFacts
}

func (f *fakeProvider) PrepareSnapshotRead(context.Context, Read, []byte) (SnapshotFacts, error) {
	f.calls++
	return f.facts, nil
}

type fakeProtector struct {
	registered, unregistered int
	fail, failUnregister     bool
}

func (f *fakeProtector) Register(context.Context, []byte, []string, time.Time) error {
	f.registered++
	if f.fail {
		return context.Canceled
	}
	return nil
}
func (f *fakeProtector) Unregister(context.Context, []byte) error {
	f.unregistered++
	if f.failUnregister {
		return context.Canceled
	}
	return nil
}

func TestAdmissionPublishesOnlyProtectedSnapshot(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	provider := &fakeProvider{facts: SnapshotFacts{Manifest: []byte("manifest"), CanonicalSchema: read.Schema, ObjectNames: []string{"o"}}}
	protector := new(fakeProtector)
	leases := NewLeaseManager(1, protector)
	now := time.Now()
	wires, err := Admit(context.Background(), AdmissionRequest{Candidate: c, Provider: provider, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), TTL: time.Minute, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{9}, 32)), Now: now})
	require.NoError(t, err)
	require.Equal(t, 1, protector.registered)
	tr, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)
	_, ok := leases.Resolve(tr.ReadRef)
	require.True(t, ok)
}

func TestAdmissionRejectsUnsafeSnapshotWithoutLease(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	provider := &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema, VisibleTombstones: true}}
	protector := new(fakeProtector)
	_, err = Admit(context.Background(), AdmissionRequest{Candidate: c, Provider: provider, Leases: NewLeaseManager(1, protector), AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), TTL: time.Minute, ReadOnly: true})
	require.ErrorContains(t, err, "unsupported")
	require.Zero(t, protector.registered)
}

func TestReleaseFailureRevokesLeaseAndRetainsRetryState(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	provider := &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}}
	protector := &fakeProtector{failUnregister: true}
	leases := NewLeaseManager(1, protector)
	now := time.Now()
	wires, err := Admit(context.Background(), AdmissionRequest{Candidate: c, Provider: provider, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), TTL: time.Minute, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{4}, 32)), Now: now})
	require.NoError(t, err)
	tr, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)
	require.Error(t, leases.Release(context.Background(), tr.ReadRef))
	_, ok := leases.Resolve(tr.ReadRef)
	require.False(t, ok)
	protector.failUnregister = false
	require.NoError(t, leases.Release(context.Background(), tr.ReadRef))
	_, ok = leases.Resolve(tr.ReadRef)
	require.False(t, ok)
}

func TestPersistentLeaseReplayAndReleasedCrashRecovery(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("lease-test", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	journal, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases")
	require.NoError(t, err)
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	provider := &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema, ObjectNames: []string{"o"}}}
	protector := new(fakeProtector)
	leases := NewPersistentLeaseManager(1, protector, journal)
	now := time.Now()
	_, ok := leases.Resolve(bytes.Repeat([]byte{7}, 32))
	require.False(t, ok)
	require.ErrorContains(t, leases.Acquire(ctx, []*Lease{{}}), "not been replayed")
	require.NoError(t, leases.Replay(ctx))
	wires, err := Admit(ctx, AdmissionRequest{Candidate: c, Provider: provider, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), TTL: time.Minute, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{7}, 32)), Now: now})
	require.NoError(t, err)
	tr, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)

	replayed := NewPersistentLeaseManager(1, protector, journal)
	replayed.now = func() time.Time { return now }
	require.NoError(t, replayed.Replay(ctx))
	_, ok = replayed.Resolve(tr.ReadRef)
	require.True(t, ok)

	protector.failUnregister = true
	require.Error(t, replayed.Release(ctx, tr.ReadRef))
	_, ok = replayed.Resolve(tr.ReadRef)
	require.False(t, ok)

	protector.failUnregister = false
	afterCrash := NewPersistentLeaseManager(1, protector, journal)
	afterCrash.now = func() time.Time { return now }
	require.NoError(t, afterCrash.Replay(ctx))
	_, ok = afterCrash.Resolve(tr.ReadRef)
	require.False(t, ok)
	loaded, err := journal.Load(ctx)
	require.NoError(t, err)
	require.Empty(t, loaded)
}

func TestPersistentLeaseReplayExpiresWithoutResurrection(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("expiry-test", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	journal, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases")
	require.NoError(t, err)
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	now := time.Now()
	leases := NewPersistentLeaseManager(1, new(fakeProtector), journal)
	leases.now = func() time.Time { return now }
	require.NoError(t, leases.Replay(ctx))
	wires, err := Admit(ctx, AdmissionRequest{Candidate: c, Provider: &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}}, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), TTL: time.Second, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{6}, 32)), Now: now})
	require.NoError(t, err)
	tr, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)

	replayed := NewPersistentLeaseManager(1, new(fakeProtector), journal)
	replayed.now = func() time.Time { return now.Add(2 * time.Second) }
	require.NoError(t, replayed.Replay(ctx))
	_, ok := replayed.Resolve(tr.ReadRef)
	require.False(t, ok)
	loaded, err := journal.Load(ctx)
	require.NoError(t, err)
	require.Empty(t, loaded)
}

func TestLeaseResolveReleaseRace(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	provider := &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}}
	leases := NewLeaseManager(1, new(fakeProtector))
	now := time.Now()
	wires, err := Admit(context.Background(), AdmissionRequest{Candidate: c, Provider: provider, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), TTL: time.Minute, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{5}, 32)), Now: now})
	require.NoError(t, err)
	tr, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				leases.Resolve(tr.ReadRef)
			}
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); require.NoError(t, leases.Release(context.Background(), tr.ReadRef)) }()
	wg.Wait()
}

func TestResolveRequiresVerifiedMTLSAndExactSchema(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	provider := &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}}
	leases := NewLeaseManager(1, new(fakeProtector))
	now := time.Now()
	wires, err := Admit(context.Background(), AdmissionRequest{Candidate: c, Provider: provider, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), TTL: time.Minute, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{8}, 32)), Now: now})
	require.NoError(t, err)
	body := appendBytes(nil, 1, wires[0])
	body = appendBytes(body, 2, read.Schema)
	handler := ResolveHandler(leases, func() time.Time { return now })
	req := httptest.NewRequest(http.MethodPost, ResolvePath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)
	req = httptest.NewRequest(http.MethodPost, ResolvePath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{}}}}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func scanQuery() *planpb.Query {
	return &planpb.Query{StmtType: planpb.Query_SELECT, Steps: []int32{0}, Nodes: []*planpb.Node{{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 42, ObjName: "t"}, TableDef: &planpb.TableDef{TblId: 42, Name: "t", TableType: "r", Cols: []*planpb.ColDef{{Name: "a", Typ: i64Type()}}}}}}
}
func i64Type() planpb.Type  { return planpb.Type{Id: int32(types.T_int64)} }
func boolType() planpb.Type { return planpb.Type{Id: int32(types.T_bool)} }
func col(pos int32) *planpb.Expr {
	return &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}}}
}
func i64(v int64) *planpb.Expr {
	return &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: v}}}}
}
func fn(name string, typ planpb.Type, args ...*planpb.Expr) *planpb.Expr {
	ids := map[string]int32{"=": function.EQUAL, ">": function.GREAT_THAN, "not": function.NOT, "sum": function.SUM, "min": function.MIN, "starcount": function.STARCOUNT}
	id, ok := ids[name]
	if !ok {
		panic("missing test function id: " + name)
	}
	return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: name, Obj: function.EncodeOverloadID(id, 0)}, Args: args}}}
}
