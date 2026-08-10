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
	"encoding/base64"
	"encoding/hex"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	planbuilder "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/encoding/protowire"
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
	require.Equal(t, []string{"a"}, p.Relations[0].GetRoot().Names)
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
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, GroupBy: []*planpb.Expr{col(0)}, AggList: []*planpb.Expr{fn("min", i64Type(), col(0))}}, &planpb.Node{NodeId: 2, NodeType: planpb.Node_SORT, Children: []int32{1}, OrderBy: []*planpb.OrderBySpec{{Expr: col(0), Flag: planpb.OrderBySpec_ASC}}, Limit: u64(5)})
	q.Steps[0] = 2
	q.Headings = []string{"a", "min(a)"}
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

func TestSortPreservesMatrixOneNullOrdering(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag planpb.OrderBySpec_OrderByFlag
		want spb.SortField_SortDirection
	}{
		{name: "implicit ascending", flag: planpb.OrderBySpec_INTERNAL, want: spb.SortField_SORT_DIRECTION_ASC_NULLS_FIRST},
		{name: "explicit ascending", flag: planpb.OrderBySpec_ASC, want: spb.SortField_SORT_DIRECTION_ASC_NULLS_FIRST},
		{name: "default descending", flag: planpb.OrderBySpec_INTERNAL | planpb.OrderBySpec_DESC, want: spb.SortField_SORT_DIRECTION_DESC_NULLS_LAST},
		{name: "explicit ascending last", flag: planpb.OrderBySpec_ASC | planpb.OrderBySpec_NULLS_LAST, want: spb.SortField_SORT_DIRECTION_ASC_NULLS_LAST},
		{name: "explicit descending first", flag: planpb.OrderBySpec_DESC | planpb.OrderBySpec_NULLS_FIRST, want: spb.SortField_SORT_DIRECTION_DESC_NULLS_FIRST},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := scanQuery()
			q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_SORT, Children: []int32{0}, OrderBy: []*planpb.OrderBySpec{{Expr: col(0), Flag: tc.flag}}})
			q.Steps[0] = 1
			candidate, err := Export(q)
			require.NoError(t, err)
			wire, err := candidate.Build(map[int32][]byte{0: {1}})
			require.NoError(t, err)
			plan := new(spb.Plan)
			require.NoError(t, proto.Unmarshal(wire, plan))
			require.Equal(t, tc.want, plan.Relations[0].GetRoot().Input.GetSort().Sorts[0].GetDirection())
		})
	}
}

func TestBoundImplicitSortExportsAsAscending(t *testing.T) {
	query := boundSQLQuery(t, "select a from select_test.bind_select order by a")
	sortNode := boundNode(t, query, planpb.Node_SORT)
	require.Len(t, sortNode.OrderBy, 1)
	require.Equal(t, planpb.OrderBySpec_INTERNAL, sortNode.OrderBy[0].Flag)

	plan := buildSubstraitPlan(t, query)
	sort := findSubstraitSort(plan.Relations[0].GetRoot().Input)
	require.NotNil(t, sort)
	require.Len(t, sort.Sorts, 1)
	require.Equal(t, spb.SortField_SORT_DIRECTION_ASC_NULLS_FIRST, sort.Sorts[0].GetDirection())
}

func TestBoundNullOnEmptyAggregateUsesNullableOutput(t *testing.T) {
	for _, tc := range []struct {
		name            string
		sql             string
		wantNullability spb.Type_Nullability
	}{
		{name: "global empty input", sql: "select min(a) from select_test.bind_select where false", wantNullability: spb.Type_NULLABILITY_NULLABLE},
		{name: "global max empty input", sql: "select max(a) from select_test.bind_select where false", wantNullability: spb.Type_NULLABILITY_NULLABLE},
		{name: "grouped", sql: "select a, min(a) from select_test.bind_select group by a", wantNullability: spb.Type_NULLABILITY_REQUIRED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := boundSQLQuery(t, tc.sql)
			aggregateNode := boundNode(t, query, planpb.Node_AGG)
			require.Len(t, aggregateNode.AggList, 1)
			require.True(t, aggregateNode.AggList[0].Typ.NotNullable)
			require.True(t, aggregateNode.AggList[0].GetF().Args[0].Typ.NotNullable)

			plan := buildSubstraitPlan(t, query)
			aggregate := findSubstraitAggregate(plan.Relations[0].GetRoot().Input)
			require.NotNil(t, aggregate)
			require.Len(t, aggregate.Measures, 1)
			require.Equal(t, tc.wantNullability, aggregate.Measures[0].Measure.OutputType.GetI64().GetNullability())
		})
	}
}

func TestBoundNullOnEmptyAggregateProjectionIsNullable(t *testing.T) {
	query := boundSQLQuery(t, "select min(a) + 1 from select_test.bind_select where false")
	aggregateNode := boundNode(t, query, planpb.Node_AGG)
	require.Len(t, aggregateNode.AggList, 1)
	require.True(t, aggregateNode.AggList[0].Typ.NotNullable, "the aggregate node carries the stale binder annotation corrected by export")
	projectNode := boundNode(t, query, planpb.Node_PROJECT)
	require.Len(t, projectNode.ProjectList, 1)
	addExpr := projectNode.ProjectList[0]
	require.False(t, addExpr.Typ.NotNullable)
	require.Len(t, addExpr.GetF().Args, 2)
	require.False(t, addExpr.GetF().Args[0].Typ.NotNullable, "the real bound aggregate ColRef is already nullable")

	plan := buildSubstraitPlan(t, query)
	aggregate := findSubstraitAggregate(plan.Relations[0].GetRoot().Input)
	require.NotNil(t, aggregate)
	require.Equal(t, spb.Type_NULLABILITY_NULLABLE, aggregate.Measures[0].Measure.OutputType.GetI64().GetNullability())
	project := plan.Relations[0].GetRoot().Input.GetProject()
	require.NotNil(t, project)
	require.Len(t, project.Expressions, 1)
	add := project.Expressions[0].GetScalarFunction()
	require.NotNil(t, add)
	require.Equal(t, spb.Type_NULLABILITY_NULLABLE, add.OutputType.GetI64().GetNullability())
}

func TestBoundInt64SumIsNotAdvertised(t *testing.T) {
	query := boundSQLQuery(t, "select sum(a) from select_test.bind_select")
	aggregateNode := boundNode(t, query, planpb.Node_AGG)
	require.Len(t, aggregateNode.AggList, 1)
	require.Equal(t, int32(types.T_decimal128), aggregateNode.AggList[0].Typ.Id)

	_, err := Export(query)
	require.ErrorContains(t, err, "no declared Sirius semantic equivalence")
	require.NotContains(t, CapabilityDocument, "sum(i64)->i64")
}

func TestFetchAcceptsBoundUint64AndPreservesAbsentCount(t *testing.T) {
	q := scanQuery()
	q.Nodes[0].Offset = u64(2)
	candidate, err := Export(q)
	require.NoError(t, err)
	wire, err := candidate.Build(map[int32][]byte{0: {1}})
	require.NoError(t, err)
	plan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, plan))
	fetch := plan.Relations[0].GetRoot().Input.GetFetch()
	require.Equal(t, int64(2), fetch.GetOffset())
	require.Nil(t, fetch.CountMode)
}

func TestPlannerModAliasUsesExactSemanticRegistry(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{fn("mod", i64Type(), col(0), i64(2))}})
	q.Steps[0] = 1
	candidate, err := Export(q)
	require.NoError(t, err)
	wire, err := candidate.Build(map[int32][]byte{0: {1}})
	require.NoError(t, err)
	plan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, plan))
	require.Equal(t, "modulus", plan.Extensions[0].GetExtensionFunction().Name)

	floatInputs := []types.Type{types.T_float64.ToType(), types.T_float64.ToType()}
	floatMod, err := function.GetFunctionByName(context.Background(), "mod", floatInputs)
	require.NoError(t, err)
	floatResult := floatMod.GetReturnType()
	q = scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{{
		Typ:  planpb.Type{Id: int32(floatResult.Oid), Width: floatResult.Width, Scale: floatResult.Scale},
		Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "mod", Obj: floatMod.GetEncodedOverloadID()}, Args: []*planpb.Expr{f64(7.5), f64(2)}}},
	}}})
	q.Steps[0] = 1
	_, err = Export(q)
	require.ErrorContains(t, err, "no declared Sirius semantic equivalence")
}

func TestSemanticRegistryRejectsForgedNullability(t *testing.T) {
	registry, err := loadSemanticRegistry()
	require.NoError(t, err)
	require.NotEmpty(t, registry)
	resolved, err := function.GetFunctionByName(
		context.Background(),
		"mod",
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
	)
	require.NoError(t, err)
	ref := &planpb.ObjectRef{ObjName: "mod", Obj: resolved.GetEncodedOverloadID()}
	args := []*planpb.Expr{{Typ: i64Type()}, {Typ: i64Type()}}

	supported, err := hasSemanticCapability(semanticScalar, "modulus", ref, args, ptrType(i64Type()))
	require.NoError(t, err)
	require.True(t, supported)
	forgedResult := i64Type()
	forgedResult.NotNullable = true
	supported, err = hasSemanticCapability(semanticScalar, "modulus", ref, args, &forgedResult)
	require.NoError(t, err)
	require.False(t, supported)

	for i := range args {
		args[i].Typ.NotNullable = true
	}
	supported, err = hasSemanticCapability(semanticScalar, "modulus", ref, args, &forgedResult)
	require.NoError(t, err)
	require.True(t, supported)
	supported, err = hasSemanticCapability(semanticScalar, "modulus", ref, args, ptrType(i64Type()))
	require.NoError(t, err)
	require.False(t, supported)
}

func TestExportRequiresCompleteBoundOutputHeadings(t *testing.T) {
	q := scanQuery()
	q.Headings = nil
	_, err := Export(q)
	require.ErrorContains(t, err, "headings")
}

func TestStarCountAggregateLowering(t *testing.T) {
	q := scanQuery()
	countType := i64Type()
	countType.NotNullable = true
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, AggList: []*planpb.Expr{fn("starcount", countType, i64(1))}})
	q.Steps[0] = 1
	c, err := Export(q)
	require.NoError(t, err)
	b, err := c.Build(map[int32][]byte{0: {1}})
	require.NoError(t, err)
	p := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(b, p))
	require.Len(t, p.Relations[0].GetRoot().Input.GetAggregate().Measures[0].Measure.Arguments, 1)

	q = scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, AggList: []*planpb.Expr{fn("starcount", countType, col(0))}})
	q.Steps[0] = 1
	_, err = Export(q)
	require.ErrorContains(t, err, "non-NULL literal")
}

func TestTaeReadStrictWire(t *testing.T) {
	now := uint64(time.Now().UnixMilli())
	h := sha256.Sum256([]byte("x"))
	r := &TaeRead{ProtocolVersion: 1, ReadRef: bytes.Repeat([]byte{1}, 32), QueryID: []byte("q"), AccountID: 1, DatabaseID: 3, TableID: 2, SnapshotTS: make([]byte, 12), SchemaDigest: h[:], ManifestSHA256: h[:], CapabilityHash: CapabilityHash[:], ExpiresAtUnixMS: now + 1000}
	b, err := MarshalTaeRead(r)
	require.NoError(t, err)
	got, err := UnmarshalTaeRead(b, now)
	require.NoError(t, err)
	require.Equal(t, r.TableID, got.TableID)
	_, err = UnmarshalTaeRead(append(b, b[:2]...), now)
	require.Error(t, err)
}

func TestProtocolRejectsMalformedWireShapes(t *testing.T) {
	now := uint64(time.Now().UnixMilli())
	digest := sha256.Sum256([]byte("protocol-test"))
	valid := func() *TaeRead {
		return &TaeRead{
			ProtocolVersion: TaeReadProtocolVersion,
			ReadRef:         bytes.Repeat([]byte{1}, 32),
			QueryID:         []byte("query"),
			AccountID:       1,
			DatabaseID:      3,
			TableID:         2,
			SnapshotTS:      make([]byte, 12),
			SchemaDigest:    digest[:],
			ManifestSHA256:  digest[:],
			CapabilityHash:  CapabilityHash[:],
			ExpiresAtUnixMS: now + 1000,
		}
	}

	require.Error(t, (*TaeRead)(nil).Validate(now))
	read := valid()
	read.QueryID = nil
	require.ErrorContains(t, read.Validate(now), "identity or digest")
	read = valid()
	read.ExpiresAtUnixMS = now
	require.ErrorContains(t, read.Validate(now), "expired")
	read = valid()
	read.ExpiresAtUnixMS = maxTaeReadExpiryUnixMS + 1
	require.ErrorContains(t, read.Validate(now), "expired")
	read = valid()
	read.CapabilityHash = bytes.Repeat([]byte{9}, sha256.Size)
	require.ErrorContains(t, read.Validate(now), "capability mismatch")

	unknown := protowire.AppendTag(nil, 13, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, []byte{1})
	wrongType := protowire.AppendTag(nil, 1, protowire.BytesType)
	wrongType = protowire.AppendBytes(wrongType, []byte{1})
	truncatedVarint := append(protowire.AppendTag(nil, 1, protowire.VarintType), 0x80)
	overflowVersion := protowire.AppendTag(nil, 1, protowire.VarintType)
	overflowVersion = protowire.AppendVarint(overflowVersion, 1<<32)
	truncatedBytes := append(protowire.AppendTag(nil, 3, protowire.BytesType), 1)
	for _, wire := range [][]byte{
		nil,
		{0x80},
		unknown,
		wrongType,
		truncatedVarint,
		overflowVersion,
		truncatedBytes,
		appendUint(nil, 1, 1),
	} {
		_, err := UnmarshalTaeRead(wire, now)
		require.Error(t, err)
	}

	request := appendBytes(nil, 1, []byte("read"))
	request = appendBytes(request, 2, []byte("schema"))
	decoded, err := UnmarshalResolveRequest(request)
	require.NoError(t, err)
	require.Equal(t, []byte("schema"), decoded.RequestedSchema)
	_, err = UnmarshalResolveRequest(appendBytes(nil, 1, []byte("read")))
	require.ErrorContains(t, err, "missing protobuf field")
	oversizedRequest := appendBytes(nil, 1, make([]byte, maxTaeReadSize+1))
	oversizedRequest = appendBytes(oversizedRequest, 2, []byte("schema"))
	_, err = UnmarshalResolveRequest(oversizedRequest)
	require.ErrorContains(t, err, "invalid resolve request")
	_, err = MarshalResolveResponse(ResolveTaeReadResponse{})
	require.ErrorContains(t, err, "invalid resolve response")
	_, err = MarshalResolveResponse(ResolveTaeReadResponse{TaeRead: make([]byte, maxTaeReadSize+1), Manifest: []byte("manifest"), CanonicalSchema: []byte("schema")})
	require.ErrorContains(t, err, "invalid resolve response")
	response, err := MarshalResolveResponse(ResolveTaeReadResponse{TaeRead: []byte("read"), Manifest: []byte("manifest"), CanonicalSchema: []byte("schema")})
	require.NoError(t, err)
	var streamed bytes.Buffer
	require.NoError(t, writeResolveResponse(&streamed, ResolveTaeReadResponse{TaeRead: []byte("read"), Manifest: []byte("manifest"), CanonicalSchema: []byte("schema")}))
	require.Equal(t, response, streamed.Bytes())
	fields, err := consumeStrictBytes(response, 3, len(response))
	require.NoError(t, err)
	require.Equal(t, []byte("manifest"), fields[1])

	require.Empty(t, appendUint(nil, 1, 0))
	require.Empty(t, appendBytes(nil, 1, nil))
	require.False(t, equalBytes([]byte{1}, []byte{1, 2}))
	require.False(t, equalBytes([]byte{1}, []byte{2}))
}

type fakeProvider struct {
	calls     int
	facts     SnapshotFacts
	err       error
	onPrepare func()
}

var testClientSPKI = []byte("test-sidecar-subject-public-key-info")

func testClientSPKIHash() []byte {
	digest := sha256.Sum256(testClientSPKI)
	return digest[:]
}

func testVerifiedTLS(spki []byte) *tls.ConnectionState {
	certificate := &x509.Certificate{RawSubjectPublicKeyInfo: append([]byte(nil), spki...)}
	return &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}, VerifiedChains: [][]*x509.Certificate{{certificate}}}
}

type fakeResolveAuditor struct {
	events      []ResolveAuditEvent
	err         error
	sawDeadline bool
}

func (f *fakeResolveAuditor) RecordResolve(ctx context.Context, event ResolveAuditEvent) error {
	_, f.sawDeadline = ctx.Deadline()
	f.events = append(f.events, event)
	return f.err
}

func acceptResolveAudit() ResolveAuditRecorder {
	return ResolveAuditFunc(func(context.Context, ResolveAuditEvent) error { return nil })
}

func (f *fakeProvider) PrepareSnapshotRead(context.Context, Read, []byte) (SnapshotFacts, error) {
	f.calls++
	if f.onPrepare != nil {
		f.onPrepare()
	}
	return f.facts, f.err
}

type fakeProtector struct {
	begun, closed, registered, rolledBack, unregistered int
	failRegisterAt                                      int
	active, fail, failUnregister                        bool
	sawCleanupDeadline, sawRollbackDeadline             bool
	unregisterContextErr, rollbackContextErr            error
}

func (f *fakeProtector) Begin(context.Context) (func(context.Context, []byte, []string, time.Time) error, func(context.Context, []byte) error, func(), error) {
	f.begun++
	f.active = true
	return f.Register, f.Rollback, func() {
		f.closed++
		f.active = false
	}, nil
}

func (f *fakeProtector) Register(context.Context, []byte, []string, time.Time) error {
	f.registered++
	if f.fail || f.failRegisterAt == f.registered {
		return context.Canceled
	}
	return nil
}

func (f *fakeProtector) Rollback(ctx context.Context, _ []byte) error {
	f.rolledBack++
	_, f.sawRollbackDeadline = ctx.Deadline()
	f.rollbackContextErr = ctx.Err()
	return nil
}

type fakeLeaseJournal struct {
	admission                        sync.Mutex
	events                           []string
	leases                           []*Lease
	loaded                           int
	storeErr, markErr, deleteErr     error
	sawCleanupDeadline               bool
	markContextErr, deleteContextErr error
}

type gatedProtector struct {
	registerCount                  int
	registerStarted, registerAllow chan struct{}
	unregisterStarted              chan struct{}
	unregisterAllow                chan struct{}
}

type gatedJournalHeaderFS struct {
	fileservice.FileService
	path           string
	started, allow chan struct{}
	once           sync.Once
}

type blockingResolveBody struct {
	reader  *bytes.Reader
	entered chan struct{}
	allow   chan struct{}
	once    sync.Once
}

func (b *blockingResolveBody) Read(p []byte) (int, error) {
	b.once.Do(func() {
		close(b.entered)
		<-b.allow
	})
	return b.reader.Read(p)
}

func (*blockingResolveBody) Close() error { return nil }

type observedResolveBody struct{ read bool }

func (b *observedResolveBody) Read([]byte) (int, error) {
	b.read = true
	return 0, errors.New("unexpected request body read")
}

func (*observedResolveBody) Close() error { return nil }

// staleAuthorityCacheFS models one CN's private read cache. Delete through a
// different instance reaches the shared backing store but cannot invalidate
// this cache, matching the remote-cache failure mode of authority-by-read.
type staleAuthorityCacheFS struct {
	fileservice.FileService
	mu    sync.Mutex
	cache map[string][]byte
}

func newStaleAuthorityCacheFS(fs fileservice.FileService) *staleAuthorityCacheFS {
	return &staleAuthorityCacheFS{FileService: fs, cache: make(map[string][]byte)}
}

func (f *staleAuthorityCacheFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if !strings.Contains(vector.FilePath, "/authority/") || len(vector.Entries) != 1 {
		return f.FileService.Read(ctx, vector)
	}
	f.mu.Lock()
	cached := append([]byte(nil), f.cache[vector.FilePath]...)
	f.mu.Unlock()
	if cached != nil {
		vector.Entries[0].Data = cached
		return nil
	}
	if err := f.FileService.Read(ctx, vector); err != nil {
		return err
	}
	f.mu.Lock()
	f.cache[vector.FilePath] = append([]byte(nil), vector.Entries[0].Data...)
	f.mu.Unlock()
	return nil
}

func (f *gatedJournalHeaderFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if vector.FilePath == f.path && len(vector.Entries) == 1 && vector.Entries[0].Size == int64(journalHeaderSize) {
		f.once.Do(func() { close(f.started) })
		select {
		case <-f.allow:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return f.FileService.Read(ctx, vector)
}

type testJournalAdmission struct {
	lock          contextMutex
	mu            sync.Mutex
	key           string
	open          int
	max           int
	gate          bool
	gateCalls     int
	firstEntered  chan struct{}
	secondWaiting chan struct{}
	allowFirst    chan struct{}
}

func newTestJournalAdmission() *testJournalAdmission {
	return &testJournalAdmission{lock: newContextMutex()}
}

func (a *testJournalAdmission) RunExclusive(ctx context.Context, key string, fn func(context.Context) error) error {
	a.mu.Lock()
	gateCall := 0
	if a.gate {
		a.gateCalls++
		gateCall = a.gateCalls
		if gateCall == 2 {
			close(a.secondWaiting)
		}
	}
	a.mu.Unlock()
	if err := a.lock.lock(ctx); err != nil {
		return err
	}
	defer a.lock.unlock()
	if gateCall == 1 {
		close(a.firstEntered)
		select {
		case <-a.allowFirst:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	a.mu.Lock()
	if a.key == "" {
		a.key = key
	}
	if a.key != key {
		a.mu.Unlock()
		return moerr.NewInternalErrorNoCtx("substrait: test admission key mismatch")
	}
	a.open++
	if a.open > a.max {
		a.max = a.open
	}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.open--
		a.mu.Unlock()
	}()
	return fn(ctx)
}

func (a *testJournalAdmission) gateNextPair() (<-chan struct{}, <-chan struct{}, chan<- struct{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.gate = true
	a.gateCalls = 0
	a.firstEntered = make(chan struct{})
	a.secondWaiting = make(chan struct{})
	a.allowFirst = make(chan struct{})
	return a.firstEntered, a.secondWaiting, a.allowFirst
}

func (a *testJournalAdmission) maxConcurrent() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.max
}

func (p *gatedProtector) Begin(context.Context) (func(context.Context, []byte, []string, time.Time) error, func(context.Context, []byte) error, func(), error) {
	return func(context.Context, []byte, []string, time.Time) error {
		p.registerCount++
		if p.registerCount == 2 && p.registerStarted != nil {
			close(p.registerStarted)
			<-p.registerAllow
		}
		return nil
	}, func(context.Context, []byte) error { return nil }, func() {}, nil
}

func (p *gatedProtector) Unregister(context.Context, []byte) error {
	if p.unregisterStarted != nil {
		close(p.unregisterStarted)
		<-p.unregisterAllow
	}
	return nil
}

func (f *fakeLeaseJournal) store(lease *Lease) error {
	f.events = append(f.events, "store")
	f.leases = append(f.leases, cloneLease(lease))
	return f.storeErr
}

func (f *fakeLeaseJournal) StoreIfCapacity(_ context.Context, leases []*Lease, maximum int) (int, error) {
	f.admission.Lock()
	defer f.admission.Unlock()
	if len(leases) == 0 || maximum <= 0 || len(leases) > maximum {
		return 0, moerr.NewInternalErrorNoCtx("substrait: invalid lease journal admission")
	}
	live := 0
	now := uint64(time.Now().UnixMilli())
	for _, lease := range f.leases {
		if !lease.Released && lease.Read.ExpiresAtUnixMS > now {
			live++
		}
	}
	if live > maximum-len(leases) {
		return 0, moerr.NewInternalErrorNoCtx("substrait: read lease capacity reached")
	}
	for i, lease := range leases {
		if err := f.store(lease); err != nil {
			return i + 1, err
		}
	}
	return len(leases), nil
}

func (f *fakeLeaseJournal) Active(_ context.Context, lease *Lease) (bool, error) {
	for _, stored := range f.leases {
		if stored.Released || !equalBytes(stored.Read.ReadRef, lease.Read.ReadRef) {
			continue
		}
		return equalBytes(stored.Wire, lease.Wire) &&
			equalBytes(stored.AuthorizedClientSPKIHash, lease.AuthorizedClientSPKIHash), nil
	}
	return false, nil
}

func (f *fakeLeaseJournal) MarkReleased(ctx context.Context, readRef []byte) error {
	f.events = append(f.events, "mark-released")
	_, f.sawCleanupDeadline = ctx.Deadline()
	f.markContextErr = ctx.Err()
	for _, lease := range f.leases {
		if equalBytes(lease.Read.ReadRef, readRef) {
			lease.Released = true
		}
	}
	return f.markErr
}

func (f *fakeLeaseJournal) Delete(ctx context.Context, readRef []byte) error {
	f.events = append(f.events, "delete")
	f.deleteContextErr = ctx.Err()
	if f.deleteErr != nil {
		return f.deleteErr
	}
	for i, lease := range f.leases {
		if equalBytes(lease.Read.ReadRef, readRef) {
			f.leases = append(f.leases[:i], f.leases[i+1:]...)
			break
		}
	}
	return nil
}

func (f *fakeLeaseJournal) Load(_ context.Context, visit func(*Lease) error) error {
	leases := append([]*Lease(nil), f.leases...)
	for _, lease := range leases {
		f.loaded++
		if err := visit(cloneLease(lease)); err != nil {
			return err
		}
	}
	return nil
}

func collectJournalLeases(ctx context.Context, journal LeaseJournal) ([]*Lease, error) {
	result := make([]*Lease, 0)
	err := journal.Load(ctx, func(lease *Lease) error {
		result = append(result, cloneLease(lease))
		return nil
	})
	return result, err
}

func resolveLease(manager *LeaseManager, readRef []byte) (*Lease, bool) {
	lease, ok, _ := manager.resolve(context.Background(), readRef)
	return lease, ok
}

func testDurableLease(t *testing.T, seed byte, expiresAt uint64) *Lease {
	manifest := []byte{'m', seed}
	schema := []byte{'s', seed}
	manifestHash := sha256.Sum256(manifest)
	schemaHash := sha256.Sum256(schema)
	read := &TaeRead{
		ProtocolVersion: TaeReadProtocolVersion,
		ReadRef:         bytes.Repeat([]byte{seed}, 32),
		QueryID:         []byte{'q', seed},
		AccountID:       1,
		DatabaseID:      2,
		TableID:         uint64(seed),
		SnapshotTS:      make([]byte, types.TxnTsSize),
		SchemaDigest:    schemaHash[:],
		ManifestSHA256:  manifestHash[:],
		CapabilityHash:  CapabilityHash[:],
		ExpiresAtUnixMS: expiresAt,
	}
	wire, err := MarshalTaeRead(read)
	require.NoError(t, err)
	return &Lease{Read: read, Wire: wire, Manifest: manifest, CanonicalSchema: schema, AuthorizedClientSPKIHash: testClientSPKIHash()}
}

func storeJournalLease(t *testing.T, journal *FileServiceLeaseJournal, lease *Lease) {
	t.Helper()
	stored, err := journal.StoreIfCapacity(context.Background(), []*Lease{lease}, 1)
	require.NoError(t, err)
	require.Equal(t, 1, stored)
}

func TestReplayCleansTerminalRecordsBeforeLiveCapacity(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name     string
		released bool
		expires  time.Time
	}{
		{name: "released", released: true, expires: now.Add(time.Minute)},
		{name: "expired", expires: now.Add(-time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			journal := new(fakeLeaseJournal)
			for seed := byte(1); seed <= 3; seed++ {
				lease := testDurableLease(t, seed, uint64(tc.expires.UnixMilli()))
				lease.Released = tc.released
				journal.leases = append(journal.leases, lease)
			}
			live := testDurableLease(t, 4, uint64(now.Add(time.Minute).UnixMilli()))
			journal.leases = append(journal.leases, live)
			protector := new(fakeProtector)
			manager := NewPersistentLeaseManager(1, protector, journal)
			manager.now = func() time.Time { return now }

			require.NoError(t, manager.Replay(context.Background()))
			require.True(t, manager.Ready())
			_, ok := resolveLease(manager, live.Read.ReadRef)
			require.True(t, ok)
			require.Len(t, journal.leases, 1)
			require.Equal(t, 5, journal.loaded)
			require.Equal(t, 1, protector.registered)
			require.Equal(t, 3, protector.unregistered)
		})
	}
}

func TestReplayStopsAfterBoundedLiveCapacity(t *testing.T) {
	now := time.Now()
	journal := new(fakeLeaseJournal)
	for seed := byte(1); seed <= 5; seed++ {
		journal.leases = append(journal.leases, testDurableLease(t, seed, uint64(now.Add(time.Minute).UnixMilli())))
	}
	protector := new(fakeProtector)
	manager := NewPersistentLeaseManager(1, protector, journal)
	manager.now = func() time.Time { return now }

	require.ErrorContains(t, manager.Replay(context.Background()), "exceed capacity")
	require.Equal(t, 2, journal.loaded)
	require.Zero(t, protector.registered)
	require.False(t, manager.Ready())
}

func TestReplayDoesNotPreallocateConfiguredCapacity(t *testing.T) {
	manager := NewPersistentLeaseManager(int(^uint(0)>>1), new(fakeProtector), new(fakeLeaseJournal))
	require.NoError(t, manager.Replay(context.Background()))
	require.True(t, manager.Ready())
}

func TestReplayRollsBackPartialProtectionWithCleanupContext(t *testing.T) {
	now := time.Now()
	journal := new(fakeLeaseJournal)
	for seed := byte(1); seed <= 2; seed++ {
		journal.leases = append(journal.leases, testDurableLease(t, seed, uint64(now.Add(time.Minute).UnixMilli())))
	}
	protector := &fakeProtector{failRegisterAt: 2}
	manager := NewPersistentLeaseManager(2, protector, journal)
	manager.now = func() time.Time { return now }
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorContains(t, manager.Replay(canceled), "replay read lease protection")
	require.Equal(t, 2, protector.registered)
	require.Equal(t, 1, protector.rolledBack)
	require.True(t, protector.sawRollbackDeadline)
	require.NoError(t, protector.rollbackContextErr)
	require.False(t, manager.Ready())
	require.Empty(t, manager.leases)
	require.Len(t, journal.leases, 2)
}

func (f *fakeProtector) Unregister(ctx context.Context, _ []byte) error {
	f.unregistered++
	_, f.sawCleanupDeadline = ctx.Deadline()
	f.unregisterContextErr = ctx.Err()
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
	provider.onPrepare = func() { require.True(t, protector.active) }
	leases := NewLeaseManager(1, protector)
	now := time.Now()
	wires, err := Admit(context.Background(), AdmissionRequest{Candidate: c, Provider: provider, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Minute, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{9}, 32)), Now: now})
	require.NoError(t, err)
	require.Equal(t, 1, protector.begun)
	require.Equal(t, 1, protector.closed)
	require.False(t, protector.active)
	require.Equal(t, 1, protector.registered)
	tr, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)
	_, ok := resolveLease(leases, tr.ReadRef)
	require.True(t, ok)
}

func TestAdmissionRejectsUnsafeSnapshotWithoutLease(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	provider := &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema, VisibleTombstones: true}}
	protector := new(fakeProtector)
	_, err = Admit(context.Background(), AdmissionRequest{Candidate: c, Provider: provider, Leases: NewLeaseManager(1, protector), AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Minute, ReadOnly: true})
	require.ErrorContains(t, err, "unsupported")
	require.Zero(t, protector.registered)
}

func TestAdmissionRollbackDurablyRevokesAndReportsCleanupFailure(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	request := func(manager *LeaseManager) AdmissionRequest {
		return AdmissionRequest{
			Candidate: c, Provider: &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}},
			Leases: manager, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Minute,
			ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{2}, 32)), Now: time.Now(),
		}
	}
	deleteErr := errors.New("delete cleanup failed")

	t.Run("protection failure", func(t *testing.T) {
		journal := &fakeLeaseJournal{deleteErr: deleteErr}
		protector := &fakeProtector{fail: true}
		manager := NewLeaseManager(1, protector)
		manager.journal = journal
		_, err := Admit(context.Background(), request(manager))
		require.ErrorContains(t, err, "protect read lease")
		require.ErrorContains(t, err, deleteErr.Error())
		require.Equal(t, []string{"store", "mark-released", "delete"}, journal.events)
		require.True(t, journal.sawCleanupDeadline)
		require.Len(t, journal.leases, 1)
		require.True(t, journal.leases[0].Released)
	})

	t.Run("ambiguous store failure", func(t *testing.T) {
		storeErr := errors.New("store failed after write")
		journal := &fakeLeaseJournal{storeErr: storeErr, deleteErr: deleteErr}
		manager := NewLeaseManager(1, new(fakeProtector))
		manager.journal = journal
		_, err := Admit(context.Background(), request(manager))
		require.ErrorContains(t, err, storeErr.Error())
		require.ErrorContains(t, err, deleteErr.Error())
		require.Equal(t, []string{"store", "mark-released", "delete"}, journal.events)
		require.Len(t, journal.leases, 1)
		require.True(t, journal.leases[0].Released)
	})

	t.Run("retains protection without durable revocation", func(t *testing.T) {
		markErr := errors.New("mark failed")
		journal := &fakeLeaseJournal{markErr: markErr, deleteErr: deleteErr}
		protector := new(fakeProtector)
		manager := NewLeaseManager(1, protector)
		manager.journal = journal
		lease := &Lease{Read: &TaeRead{ReadRef: bytes.Repeat([]byte{3}, 32)}}
		err := manager.rollbackAcquisition(context.Background(), protector.Rollback, []*Lease{lease}, []*Lease{lease})
		require.ErrorContains(t, err, markErr.Error())
		require.ErrorContains(t, err, deleteErr.Error())
		require.Zero(t, protector.unregistered)
	})
}

func TestReleaseFailureRevokesLeaseAndRetainsRetryState(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	provider := &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}}
	protector := &fakeProtector{failUnregister: true}
	leases := NewLeaseManager(1, protector)
	now := time.Now()
	wires, err := Admit(context.Background(), AdmissionRequest{Candidate: c, Provider: provider, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Minute, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{4}, 32)), Now: now})
	require.NoError(t, err)
	tr, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)
	require.Error(t, leases.Release(context.Background(), tr.ReadRef))
	_, ok := resolveLease(leases, tr.ReadRef)
	require.False(t, ok)
	protector.failUnregister = false
	require.NoError(t, leases.Release(context.Background(), tr.ReadRef))
	_, ok = resolveLease(leases, tr.ReadRef)
	require.False(t, ok)
}

func TestReleaseMarkFailureKeepsAmbiguousRevocationRetryable(t *testing.T) {
	now := time.Now()
	lease := testDurableLease(t, 11, uint64(now.Add(time.Minute).UnixMilli()))
	journal := new(fakeLeaseJournal)
	protector := new(fakeProtector)
	manager := NewPersistentLeaseManager(1, protector, journal)
	manager.ready = true
	require.NoError(t, manager.Acquire(context.Background(), []*Lease{lease}))

	journal.markErr = errors.New("ambiguous authority delete")
	require.ErrorContains(t, manager.Release(context.Background(), lease.Read.ReadRef), "ambiguous authority delete")
	_, ok := resolveLease(manager, lease.Read.ReadRef)
	require.False(t, ok)
	require.Equal(t, releaseRevoking, manager.releases[string(lease.Read.ReadRef)])
	require.Zero(t, protector.unregistered)

	journal.markErr = nil
	require.NoError(t, manager.Release(context.Background(), lease.Read.ReadRef))
	require.Equal(t, 1, protector.unregistered)
	require.Empty(t, manager.leases)
}

func TestResolveDoesNotWaitForLeaseMutationIO(t *testing.T) {
	now := time.Now()
	first := testDurableLease(t, 1, uint64(now.Add(time.Minute).UnixMilli()))
	second := testDurableLease(t, 2, uint64(now.Add(time.Minute).UnixMilli()))
	protector := &gatedProtector{
		registerStarted: make(chan struct{}), registerAllow: make(chan struct{}),
		unregisterStarted: make(chan struct{}), unregisterAllow: make(chan struct{}),
	}
	defer closeChannelIfOpen(protector.registerAllow)
	defer closeChannelIfOpen(protector.unregisterAllow)
	manager := NewLeaseManager(2, protector)
	require.NoError(t, manager.Acquire(context.Background(), []*Lease{first}))

	acquireDone := make(chan error, 1)
	go func() { acquireDone <- manager.Acquire(context.Background(), []*Lease{second}) }()
	select {
	case <-protector.registerStarted:
	case <-time.After(time.Second):
		t.Fatal("second registration did not start")
	}
	assertResolvePromptly(t, manager, first.Read.ReadRef)
	close(protector.registerAllow)
	require.NoError(t, <-acquireDone)

	releaseDone := make(chan error, 1)
	go func() { releaseDone <- manager.Release(context.Background(), first.Read.ReadRef) }()
	select {
	case <-protector.unregisterStarted:
	case <-time.After(time.Second):
		t.Fatal("unregistration did not start")
	}
	assertResolvePromptly(t, manager, second.Read.ReadRef)
	close(protector.unregisterAllow)
	require.NoError(t, <-releaseDone)
}

func closeChannelIfOpen(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func assertResolvePromptly(t *testing.T, manager *LeaseManager, readRef []byte) {
	t.Helper()
	resolved := make(chan bool, 1)
	go func() {
		_, ok := resolveLease(manager, readRef)
		resolved <- ok
	}()
	select {
	case ok := <-resolved:
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("resolve waited for unrelated lease mutation I/O")
	}
}

func TestReleaseUsesIndependentBoundedCleanupContext(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	journal := new(fakeLeaseJournal)
	protector := new(fakeProtector)
	manager := NewLeaseManager(1, protector)
	manager.journal = journal
	now := time.Now()
	wires, err := Admit(context.Background(), AdmissionRequest{
		Candidate: c, Provider: &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}},
		Leases: manager, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Minute,
		ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{4}, 32)), Now: now,
	})
	require.NoError(t, err)
	tr, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, manager.Release(canceled, tr.ReadRef))
	require.True(t, journal.sawCleanupDeadline)
	require.NoError(t, journal.markContextErr)
	require.NoError(t, journal.deleteContextErr)
	require.Equal(t, []string{"store", "mark-released", "delete"}, journal.events)
	require.Equal(t, 1, protector.unregistered)
}

func TestPersistentLeaseReplayAndReleasedCrashRecovery(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("lease-test", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	journal, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases", newTestJournalAdmission())
	require.NoError(t, err)
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	provider := &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema, ObjectNames: []string{"o"}}}
	protector := new(fakeProtector)
	leases := NewPersistentLeaseManager(1, protector, journal)
	now := time.Now()
	_, ok := resolveLease(leases, bytes.Repeat([]byte{7}, 32))
	require.False(t, ok)
	require.ErrorContains(t, leases.Acquire(ctx, []*Lease{{}}), "not been replayed")
	require.NoError(t, leases.Replay(ctx))
	wires, err := Admit(ctx, AdmissionRequest{Candidate: c, Provider: provider, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Minute, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{7}, 32)), Now: now})
	require.NoError(t, err)
	tr, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)

	replayed := NewPersistentLeaseManager(1, protector, journal)
	replayed.now = func() time.Time { return now }
	require.NoError(t, replayed.Replay(ctx))
	replayedLease, ok := resolveLease(replayed, tr.ReadRef)
	require.True(t, ok)
	require.Equal(t, testClientSPKIHash(), replayedLease.AuthorizedClientSPKIHash)

	protector.failUnregister = true
	require.Error(t, replayed.Release(ctx, tr.ReadRef))
	_, ok = resolveLease(replayed, tr.ReadRef)
	require.False(t, ok)
	_, ok = resolveLease(leases, tr.ReadRef)
	require.False(t, ok, "a manager with stale replay state must observe durable revocation")
	require.NotEmpty(t, leases.leases, "failed GC cleanup must retain a hidden retry owner")
	require.Equal(t, releaseMarked, leases.releases[string(tr.ReadRef)])

	protector.failUnregister = false
	require.NoError(t, leases.Release(ctx, tr.ReadRef))
	require.Empty(t, leases.leases)
	afterCrash := NewPersistentLeaseManager(1, protector, journal)
	afterCrash.now = func() time.Time { return now }
	require.NoError(t, afterCrash.Replay(ctx))
	_, ok = resolveLease(afterCrash, tr.ReadRef)
	require.False(t, ok)
	loaded, err := collectJournalLeases(ctx, journal)
	require.NoError(t, err)
	require.Empty(t, loaded)
}

func TestPersistentLeaseReplayExpiresWithoutResurrection(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("expiry-test", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	journal, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases", newTestJournalAdmission())
	require.NoError(t, err)
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	now := time.Now()
	leases := NewPersistentLeaseManager(1, new(fakeProtector), journal)
	leases.now = func() time.Time { return now }
	require.NoError(t, leases.Replay(ctx))
	wires, err := Admit(ctx, AdmissionRequest{Candidate: c, Provider: &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}}, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Second, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{6}, 32)), Now: now})
	require.NoError(t, err)
	tr, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)

	replayed := NewPersistentLeaseManager(1, new(fakeProtector), journal)
	replayed.now = func() time.Time { return now.Add(2 * time.Second) }
	require.NoError(t, replayed.Replay(ctx))
	_, ok := resolveLease(replayed, tr.ReadRef)
	require.False(t, ok)
	loaded, err := collectJournalLeases(ctx, journal)
	require.NoError(t, err)
	require.Empty(t, loaded)
}

func TestJournalCapacityIsAtomicAcrossManagersAndReplay(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("journal-capacity-race", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	admission := newTestJournalAdmission()
	journalA, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases", admission)
	require.NoError(t, err)
	journalB, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases", admission)
	require.NoError(t, err)
	managerA := NewPersistentLeaseManager(1, new(fakeProtector), journalA)
	managerB := NewPersistentLeaseManager(1, new(fakeProtector), journalB)
	require.NoError(t, managerA.Replay(ctx))
	require.NoError(t, managerB.Replay(ctx))

	expires := uint64(time.Now().Add(time.Minute).UnixMilli())
	firstEntered, secondWaiting, allowFirst := admission.gateNextPair()
	resultA := make(chan error, 1)
	resultB := make(chan error, 1)
	go func() {
		resultA <- managerA.Acquire(ctx, []*Lease{testDurableLease(t, 1, expires)})
	}()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first journal admission did not enter the shared critical section")
	}
	go func() {
		resultB <- managerB.Acquire(ctx, []*Lease{testDurableLease(t, 2, expires)})
	}()
	select {
	case <-secondWaiting:
	case <-time.After(time.Second):
		t.Fatal("second journal admission did not wait on the shared coordinator")
	}
	close(allowFirst)
	require.NoError(t, <-resultA)
	err = <-resultB
	require.ErrorContains(t, err, "capacity reached")
	if got := admission.maxConcurrent(); got != 1 {
		t.Fatalf("journal critical section concurrency = %d, want 1", got)
	}

	journalC, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases", admission)
	require.NoError(t, err)
	afterWriters := NewPersistentLeaseManager(1, new(fakeProtector), journalC)
	require.NoError(t, afterWriters.Replay(ctx))
	require.Len(t, afterWriters.leases, 1)
}

func TestAdmissionReconcilesRemoteRevocationBeforeCapacity(t *testing.T) {
	now := time.Now()
	journal := new(fakeLeaseJournal)
	oldLease := testDurableLease(t, 1, uint64(now.Add(time.Minute).UnixMilli()))
	journal.leases = append(journal.leases, cloneLease(oldLease))

	protectorA := new(fakeProtector)
	managerA := NewPersistentLeaseManager(1, protectorA, journal)
	managerA.now = func() time.Time { return now }
	require.NoError(t, managerA.Replay(context.Background()))

	protectorB := new(fakeProtector)
	managerB := NewPersistentLeaseManager(1, protectorB, journal)
	managerB.now = func() time.Time { return now }
	require.NoError(t, managerB.Replay(context.Background()))

	// Manager A owns durable revocation. Manager B still has the immutable
	// lease locally and would reject at capacity without reconciliation.
	require.NoError(t, managerA.Release(context.Background(), oldLease.Read.ReadRef))
	newLease := testDurableLease(t, 2, uint64(now.Add(time.Minute).UnixMilli()))
	require.NoError(t, managerB.Acquire(context.Background(), []*Lease{newLease}))

	_, oldOK := resolveLease(managerB, oldLease.Read.ReadRef)
	_, newOK := resolveLease(managerB, newLease.Read.ReadRef)
	require.False(t, oldOK)
	require.True(t, newOK)
	require.Equal(t, 1, protectorB.unregistered)
	require.Equal(t, 2, protectorB.registered)
}

func TestLeaseResolveReleaseRace(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	provider := &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}}
	leases := NewLeaseManager(1, new(fakeProtector))
	now := time.Now()
	wires, err := Admit(context.Background(), AdmissionRequest{Candidate: c, Provider: provider, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Minute, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{5}, 32)), Now: now})
	require.NoError(t, err)
	tr, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				resolveLease(leases, tr.ReadRef)
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
	wires, err := Admit(context.Background(), AdmissionRequest{Candidate: c, Provider: provider, Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Minute, ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{8}, 32)), Now: now})
	require.NoError(t, err)
	body := appendBytes(nil, 1, wires[0])
	body = appendBytes(body, 2, read.Schema)
	auditor := new(fakeResolveAuditor)
	handler := ResolveHandler(leases, func() time.Time { return now }, auditor)
	req := httptest.NewRequest(http.MethodPost, ResolvePath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)
	req = httptest.NewRequest(http.MethodPost, ResolvePath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.TLS = testVerifiedTLS([]byte("different-sidecar"))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
	req = httptest.NewRequest(http.MethodPost, ResolvePath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.TLS = testVerifiedTLS(testClientSPKI)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Len(t, auditor.events, 1)
	require.True(t, auditor.sawDeadline)
	require.Equal(t, uint64(1), auditor.events[0].AccountID)
	require.Equal(t, uint64(7), auditor.events[0].DatabaseID)
	require.Equal(t, uint64(42), auditor.events[0].TableID)
	require.Equal(t, []byte("q"), auditor.events[0].QueryID)
	require.Len(t, auditor.events[0].ReadRefSHA256, sha256.Size)

	failingAudit := &fakeResolveAuditor{err: errors.New("audit unavailable")}
	req = httptest.NewRequest(http.MethodPost, ResolvePath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.TLS = testVerifiedTLS(testClientSPKI)
	w = httptest.NewRecorder()
	ResolveHandler(leases, func() time.Time { return now }, failingAudit).ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	var nilAudit ResolveAuditFunc
	require.ErrorContains(t, nilAudit.RecordResolve(context.Background(), ResolveAuditEvent{}), "nil resolution audit")
}

func TestAdmissionRejectsInvalidInputsBeforePublishing(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	now := time.Now()
	valid := func() AdmissionRequest {
		return AdmissionRequest{
			Candidate:                c,
			Provider:                 &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}},
			Leases:                   NewLeaseManager(1, new(fakeProtector)),
			AccountID:                1,
			QueryID:                  []byte("q"),
			SnapshotTS:               make([]byte, 12),
			AuthorizedClientSPKIHash: testClientSPKIHash(),
			TTL:                      time.Minute,
			ReadOnly:                 true,
			Random:                   bytes.NewReader(bytes.Repeat([]byte{1}, 32)),
			Now:                      now,
		}
	}

	for _, tc := range []struct {
		name   string
		mutate func(*AdmissionRequest)
		want   string
	}{
		{name: "missing candidate", mutate: func(r *AdmissionRequest) { r.Candidate = nil }, want: "incomplete admission"},
		{name: "missing provider", mutate: func(r *AdmissionRequest) { r.Provider = nil }, want: "incomplete admission"},
		{name: "missing leases", mutate: func(r *AdmissionRequest) { r.Leases = nil }, want: "incomplete admission"},
		{name: "not read only", mutate: func(r *AdmissionRequest) { r.ReadOnly = false }, want: "read-only snapshot"},
		{name: "prior writes", mutate: func(r *AdmissionRequest) { r.PriorWrites = true }, want: "read-only snapshot"},
		{name: "missing account", mutate: func(r *AdmissionRequest) { r.AccountID = 0 }, want: "identity"},
		{name: "missing query", mutate: func(r *AdmissionRequest) { r.QueryID = nil }, want: "identity"},
		{name: "bad timestamp", mutate: func(r *AdmissionRequest) { r.SnapshotTS = []byte{1} }, want: "identity"},
		{name: "missing authorized client", mutate: func(r *AdmissionRequest) { r.AuthorizedClientSPKIHash = nil }, want: "identity"},
		{name: "zero ttl", mutate: func(r *AdmissionRequest) { r.TTL = 0 }, want: "TTL"},
		{name: "oversized ttl", mutate: func(r *AdmissionRequest) { r.TTL = MaxLeaseTTL + time.Second }, want: "TTL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := valid()
			tc.mutate(&r)
			_, err := Admit(context.Background(), r)
			require.ErrorContains(t, err, tc.want)
		})
	}

	r := valid()
	r.Provider = &fakeProvider{err: context.Canceled}
	_, err = Admit(context.Background(), r)
	require.ErrorContains(t, err, "prepare table")
	require.False(t, IsNotEligible(err))

	for _, facts := range []SnapshotFacts{
		{Manifest: []byte("m"), CanonicalSchema: read.Schema, CommittedInMemory: true},
		{Manifest: []byte("m"), CanonicalSchema: read.Schema, Uncommitted: true},
		{Manifest: []byte("m"), CanonicalSchema: read.Schema, VisibleTombstones: true},
		{Manifest: []byte("m"), CanonicalSchema: read.Schema, NonTAE: true},
	} {
		r = valid()
		r.Provider = &fakeProvider{facts: facts}
		_, err = Admit(context.Background(), r)
		require.ErrorContains(t, err, "unsupported")
		require.True(t, IsNotEligible(err))
	}

	for _, facts := range []SnapshotFacts{
		{CanonicalSchema: read.Schema},
		{Manifest: []byte("m"), CanonicalSchema: []byte("wrong")},
	} {
		r = valid()
		r.Provider = &fakeProvider{facts: facts}
		_, err = Admit(context.Background(), r)
		require.ErrorContains(t, err, "schema or manifest mismatch")
	}

	r = valid()
	r.Random = bytes.NewReader(nil)
	_, err = Admit(context.Background(), r)
	require.ErrorContains(t, err, "create read reference")

	protector := &fakeProtector{fail: true}
	r = valid()
	r.Leases = NewLeaseManager(1, protector)
	_, err = Admit(context.Background(), r)
	require.ErrorContains(t, err, "protect read lease")
	require.Equal(t, 1, protector.registered)
}

func TestResolveHandlerRejectsInvalidRequests(t *testing.T) {
	c, err := Export(scanQuery())
	require.NoError(t, err)
	read := c.Reads()[0]
	now := time.Now()
	leases := NewLeaseManager(1, new(fakeProtector))
	wires, err := Admit(context.Background(), AdmissionRequest{
		Candidate: c, Provider: &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}},
		Leases: leases, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Minute,
		ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{3}, 32)), Now: now,
	})
	require.NoError(t, err)
	validBody := appendBytes(nil, 1, wires[0])
	validBody = appendBytes(validBody, 2, read.Schema)
	verifiedTLS := testVerifiedTLS(testClientSPKI)

	tests := []struct {
		name        string
		method      string
		path        string
		contentType string
		body        []byte
		tls         *tls.ConnectionState
		manager     *LeaseManager
		want        int
	}{
		{name: "wrong path", method: http.MethodPost, path: "/wrong", want: http.StatusNotFound},
		{name: "wrong method", method: http.MethodGet, path: ResolvePath, want: http.StatusMethodNotAllowed},
		{name: "wrong content type", method: http.MethodPost, path: ResolvePath, want: http.StatusUnsupportedMediaType},
		{name: "unverified client", method: http.MethodPost, path: ResolvePath, contentType: "application/x-protobuf", body: validBody, want: http.StatusUnauthorized},
		{name: "empty verified chain", method: http.MethodPost, path: ResolvePath, contentType: "application/x-protobuf", body: validBody, tls: &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{}}}, manager: leases, want: http.StatusUnauthorized},
		{name: "malformed request", method: http.MethodPost, path: ResolvePath, contentType: "application/x-protobuf", body: []byte{0xff}, tls: verifiedTLS, manager: leases, want: http.StatusBadRequest},
		{name: "resolver unavailable", method: http.MethodPost, path: ResolvePath, contentType: "application/x-protobuf", body: validBody, tls: verifiedTLS, want: http.StatusServiceUnavailable},
		{name: "schema mismatch", method: http.MethodPost, path: ResolvePath, contentType: "application/x-protobuf", body: appendBytes(appendBytes(nil, 1, wires[0]), 2, []byte("wrong")), tls: verifiedTLS, manager: leases, want: http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.contentType)
			req.TLS = tc.tls
			w := httptest.NewRecorder()
			ResolveHandler(tc.manager, func() time.Time { return now }, acceptResolveAudit()).ServeHTTP(w, req)
			require.Equal(t, tc.want, w.Code)
		})
	}
}

func TestResolverServerLifecycle(t *testing.T) {
	leases := NewLeaseManager(1, new(fakeProtector))
	validTLS := &tls.Config{
		MinVersion:   tls.VersionTLS10,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    x509.NewCertPool(),
		Certificates: []tls.Certificate{{Certificate: [][]byte{{1}}}},
	}
	auditor := acceptResolveAudit()
	_, err := NewResolverServer("", validTLS, leases, auditor)
	require.Error(t, err)
	_, err = NewResolverServer("127.0.0.1:0", nil, leases, auditor)
	require.Error(t, err)
	_, err = NewResolverServer("127.0.0.1:0", &tls.Config{}, leases, auditor)
	require.Error(t, err)
	_, err = NewResolverServer("127.0.0.1:0", validTLS, leases, nil)
	require.ErrorContains(t, err, "audit recorder")
	neverStarted, err := NewResolverServer("127.0.0.1:0", validTLS, leases, auditor)
	require.NoError(t, err)
	require.NoError(t, neverStarted.Close(context.Background()))
	require.ErrorContains(t, neverStarted.Start(), "closed")

	server, err := NewResolverServer("127.0.0.1:0", validTLS, leases, auditor)
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS12), server.server.TLSConfig.MinVersion)
	require.Equal(t, uint16(tls.VersionTLS10), validTLS.MinVersion)
	require.NoError(t, server.Start())
	require.ErrorContains(t, server.Start(), "already started")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, server.Close(ctx))
	require.NoError(t, server.Close(ctx))
	require.ErrorContains(t, server.Start(), "closed")
}

func TestFileServiceLeaseJournalValidationAndAuthority(t *testing.T) {
	_, err := NewFileServiceLeaseJournal(nil, "leases", newTestJournalAdmission())
	require.Error(t, err)
	fs, err := fileservice.NewMemoryFS("journal-validation", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	_, err = NewFileServiceLeaseJournal(fs, "leases", nil)
	require.Error(t, err)
	_, err = NewFileServiceLeaseJournal(fs, "../leases", newTestJournalAdmission())
	require.Error(t, err)
	journal, err := NewFileServiceLeaseJournal(fs, "/sirius/read-leases/", newTestJournalAdmission())
	require.NoError(t, err)
	_, err = journal.StoreIfCapacity(context.Background(), []*Lease{nil}, 1)
	require.Error(t, err)
	require.Error(t, journal.MarkReleased(context.Background(), []byte("short")))
	require.Error(t, journal.Delete(context.Background(), []byte("short")))

	ref := bytes.Repeat([]byte{4}, 32)
	require.NoError(t, journal.MarkReleased(context.Background(), ref))
	require.NoError(t, journal.MarkReleased(context.Background(), ref))
	require.NoError(t, journal.Delete(context.Background(), ref))
	require.NoError(t, journal.Delete(context.Background(), ref))

	orphanRef := bytes.Repeat([]byte{5}, 32)
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{FilePath: journal.authorityPath(orphanRef), Entries: []fileservice.IOEntry{{Offset: 0, Size: sha256.Size, Data: make([]byte, sha256.Size)}}}))
	require.NoError(t, journal.Load(context.Background(), func(*Lease) error { return nil }))
	_, err = fs.StatFile(context.Background(), journal.authorityPath(orphanRef))
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))

	lease := testDurableLease(t, 6, uint64(time.Now().Add(time.Minute).UnixMilli()))
	storeJournalLease(t, journal, lease)
	active, err := journal.Active(context.Background(), lease)
	require.NoError(t, err)
	require.True(t, active)
	stale := cloneLease(lease)
	stale.AuthorizedClientSPKIHash[0] ^= 1
	active, err = journal.Active(context.Background(), stale)
	require.NoError(t, err)
	require.False(t, active)
	require.NoError(t, fs.Delete(context.Background(), journal.authorityPath(lease.Read.ReadRef)))
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: journal.authorityPath(lease.Read.ReadRef),
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: sha256.Size, Data: make([]byte, sha256.Size)}},
	}))
	active, err = journal.Active(context.Background(), lease)
	require.NoError(t, err)
	require.False(t, active, "authority existence must not replace its lease/SPKI digest binding")
	require.NoError(t, journal.MarkReleased(context.Background(), lease.Read.ReadRef))
	active, err = journal.Active(context.Background(), lease)
	require.NoError(t, err)
	require.False(t, active)
	require.NoError(t, journal.Delete(context.Background(), lease.Read.ReadRef))
}

func TestFileServiceLeaseJournalAuthorityLinearizesRelease(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("journal-authority-race", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	journal, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases", newTestJournalAdmission())
	require.NoError(t, err)
	lease := testDurableLease(t, 9, uint64(time.Now().Add(time.Minute).UnixMilli()))
	storeJournalLease(t, journal, lease)
	gated := &gatedJournalHeaderFS{
		FileService: fs,
		path:        journal.activePath(lease.Read.ReadRef),
		started:     make(chan struct{}),
		allow:       make(chan struct{}),
	}
	defer closeChannelIfOpen(gated.allow)
	journal.fs = gated
	result := make(chan bool, 1)
	errs := make(chan error, 1)
	go func() {
		active, err := journal.Active(ctx, lease)
		result <- active
		errs <- err
	}()
	select {
	case <-gated.started:
	case <-time.After(time.Second):
		t.Fatal("authority validation did not reach the immutable record read")
	}
	require.NoError(t, journal.MarkReleased(ctx, lease.Read.ReadRef))
	close(gated.allow)
	require.NoError(t, <-errs)
	require.False(t, <-result)
}

func TestFileServiceLeaseJournalAuthorityIgnoresAnotherClientStaleCache(t *testing.T) {
	ctx := context.Background()
	shared, err := fileservice.NewMemoryFS("journal-authority-cache", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	clientA := newStaleAuthorityCacheFS(shared)
	clientB := newStaleAuthorityCacheFS(shared)
	admission := newTestJournalAdmission()
	journalA, err := NewFileServiceLeaseJournal(clientA, "sirius/read-leases", admission)
	require.NoError(t, err)
	journalB, err := NewFileServiceLeaseJournal(clientB, "sirius/read-leases", admission)
	require.NoError(t, err)
	lease := testDurableLease(t, 10, uint64(time.Now().Add(time.Minute).UnixMilli()))
	storeJournalLease(t, journalA, lease)

	// Warm client A's private authority cache, then revoke through client B.
	authority := fileservice.IOVector{
		FilePath: journalA.authorityPath(lease.Read.ReadRef),
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: sha256.Size}},
	}
	require.NoError(t, clientA.Read(ctx, &authority))
	authority.Release()
	require.NoError(t, journalB.MarkReleased(ctx, lease.Read.ReadRef))

	// The adversarial cache still returns the deleted authority bytes.
	stale := fileservice.IOVector{
		FilePath: journalA.authorityPath(lease.Read.ReadRef),
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: sha256.Size}},
	}
	require.NoError(t, clientA.Read(ctx, &stale))
	require.Len(t, stale.Entries[0].Data, sha256.Size)
	stale.Release()

	// Active uses the shared backing-store stat as its final linearization
	// point and therefore observes client B's revocation.
	active, err := journalA.Active(ctx, lease)
	require.NoError(t, err)
	require.False(t, active)
}

func TestFileServiceLeaseJournalCleansOrphanAuthoritiesInBoundedBatches(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("journal-orphan-batches", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	journal, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases", newTestJournalAdmission())
	require.NoError(t, err)
	refs := make([][]byte, 0, journalCleanupBatchSize+1)
	for i := 0; i <= journalCleanupBatchSize; i++ {
		ref := make([]byte, 32)
		ref[0], ref[1] = byte(i), byte(i>>8)
		refs = append(refs, ref)
		require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: journal.authorityPath(ref), Entries: []fileservice.IOEntry{{Offset: 0, Size: sha256.Size, Data: make([]byte, sha256.Size)}}}))
	}
	require.NoError(t, journal.Load(ctx, func(*Lease) error { return nil }))
	for _, ref := range refs {
		_, err = fs.StatFile(ctx, journal.authorityPath(ref))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	}
}

func TestResolveByteBudgetIsBoundedAndCancellationAware(t *testing.T) {
	budget := newResolveByteBudget(10)
	release, err := budget.acquire(context.Background(), 10)
	require.NoError(t, err)
	_, err = budget.acquire(context.Background(), 11)
	require.ErrorContains(t, err, "exceeds byte budget")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = budget.acquire(canceled, 1)
	require.ErrorIs(t, err, context.Canceled)
	release()
	release()
	release, err = budget.acquire(context.Background(), 1)
	require.NoError(t, err)
	release()
}

func TestResolveBudgetPrecedesRequestBodyAllocation(t *testing.T) {
	candidate, err := Export(scanQuery())
	require.NoError(t, err)
	read := candidate.Reads()[0]
	manager := NewLeaseManager(1, new(fakeProtector))
	manager.resolveBytes = newResolveByteBudget(maxResolveHandlerBytes)
	now := time.Now()
	wires, err := Admit(context.Background(), AdmissionRequest{
		Candidate: candidate, Provider: &fakeProvider{facts: SnapshotFacts{Manifest: []byte("m"), CanonicalSchema: read.Schema}},
		Leases: manager, AccountID: 1, QueryID: []byte("q"), SnapshotTS: make([]byte, 12), AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Minute,
		ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{8}, 32)), Now: now,
	})
	require.NoError(t, err)
	body := appendBytes(nil, 1, wires[0])
	body = appendBytes(body, 2, read.Schema)
	handler := ResolveHandler(manager, func() time.Time { return now }, acceptResolveAudit())

	firstBody := &blockingResolveBody{reader: bytes.NewReader(body), entered: make(chan struct{}), allow: make(chan struct{})}
	defer closeChannelIfOpen(firstBody.allow)
	firstRequest := httptest.NewRequest(http.MethodPost, ResolvePath, nil)
	firstRequest.Header.Set("Content-Type", "application/x-protobuf")
	firstRequest.TLS = testVerifiedTLS(testClientSPKI)
	firstRequest.Body = firstBody
	firstResponse := httptest.NewRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		handler.ServeHTTP(firstResponse, firstRequest)
	}()
	select {
	case <-firstBody.entered:
	case <-time.After(time.Second):
		t.Fatal("first resolver did not begin reading its admitted body")
	}

	secondBody := new(observedResolveBody)
	secondRequest := httptest.NewRequest(http.MethodPost, ResolvePath, nil)
	secondRequest.Header.Set("Content-Type", "application/x-protobuf")
	secondRequest.TLS = testVerifiedTLS(testClientSPKI)
	secondRequest.Body = secondBody
	canceled, cancel := context.WithCancel(secondRequest.Context())
	cancel()
	secondRequest = secondRequest.WithContext(canceled)
	secondResponse := httptest.NewRecorder()
	handler.ServeHTTP(secondResponse, secondRequest)
	require.Equal(t, http.StatusServiceUnavailable, secondResponse.Code)
	require.False(t, secondBody.read, "a request without byte admission must not materialize its body")

	closeChannelIfOpen(firstBody.allow)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first resolver did not finish")
	}
	require.Equal(t, http.StatusOK, firstResponse.Code)
}

func TestLeaseMutationLockHonorsContext(t *testing.T) {
	manager := NewLeaseManager(1, new(fakeProtector))
	require.NoError(t, manager.mutation.lock(context.Background()))
	defer manager.mutation.unlock()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, manager.mutation.lock(canceled), context.Canceled)

	fs, err := fileservice.NewMemoryFS("journal-context-lock", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	admission := newTestJournalAdmission()
	journal, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases", admission)
	require.NoError(t, err)
	require.NoError(t, admission.lock.lock(context.Background()))
	defer admission.lock.unlock()
	_, err = journal.StoreIfCapacity(canceled, []*Lease{testDurableLease(t, 1, uint64(time.Now().Add(time.Minute).UnixMilli()))}, 1)
	require.ErrorContains(t, err, "context canceled")
}

func TestJournalBoundIncludesJSONBase64Expansion(t *testing.T) {
	minimum := base64.StdEncoding.EncodedLen(maxManifestSize) +
		base64.StdEncoding.EncodedLen(maxCanonicalSchemaSize) + maxManifestSize
	require.Greater(t, maxJournalRecordSize, minimum)
}

func TestFileServiceLeaseJournalRejectsCorruption(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{name: "multiple values", data: []byte("{}{}"), want: "multiple JSON values"},
		{name: "checksum mismatch", data: []byte(`{"record":{},"sha256":"AA=="}`), want: "checksum mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs, err := fileservice.NewMemoryFS("journal-corrupt-"+tc.name, fileservice.CacheConfig{}, nil)
			require.NoError(t, err)
			journal, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases", newTestJournalAdmission())
			require.NoError(t, err)
			name := journal.activePath(bytes.Repeat([]byte{5}, 32))
			data := make([]byte, 0, journalHeaderSize+len(tc.data))
			data = append(data, journalHeaderMagic...)
			data = append(data, make([]byte, sha256.Size)...)
			data = append(data, tc.data...)
			require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: name, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), Data: data}}}))
			err = journal.Load(ctx, func(*Lease) error { return nil })
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestFileServiceLeaseJournalRejectsNonCanonicalName(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("journal-name", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	journal, err := NewFileServiceLeaseJournal(fs, "sirius/read-leases", newTestJournalAdmission())
	require.NoError(t, err)
	lease := testDurableLease(t, 0xab, uint64(time.Now().Add(time.Minute).UnixMilli()))
	storeJournalLease(t, journal, lease)
	data, err := journal.read(ctx, journal.activePath(lease.Read.ReadRef))
	require.NoError(t, err)
	require.NoError(t, journal.Delete(ctx, lease.Read.ReadRef))
	upper := strings.ToUpper(hex.EncodeToString(lease.Read.ReadRef)) + ".json"
	name := path.Join(journal.prefix, "active", upper)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: name, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), Data: data}}}))

	err = journal.Load(ctx, func(*Lease) error { return nil })
	require.ErrorContains(t, err, "invalid lease journal name")
}

func TestSubstraitTypeAndLiteralMappings(t *testing.T) {
	typesAndLiterals := []struct {
		typ planpb.Type
		lit *planpb.Literal
	}{
		{typ: planpb.Type{Id: int32(types.T_bool)}, lit: &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}}},
		{typ: planpb.Type{Id: int32(types.T_int8)}, lit: &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: 1}}},
		{typ: planpb.Type{Id: int32(types.T_int16)}, lit: &planpb.Literal{Value: &planpb.Literal_I16Val{I16Val: 2}}},
		{typ: planpb.Type{Id: int32(types.T_int32)}, lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 3}}},
		{typ: planpb.Type{Id: int32(types.T_int64)}, lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 4}}},
		{typ: planpb.Type{Id: int32(types.T_float32)}, lit: &planpb.Literal{Value: &planpb.Literal_Fval{Fval: 1.5}}},
		{typ: planpb.Type{Id: int32(types.T_float64)}, lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: 2.5}}},
		{typ: planpb.Type{Id: int32(types.T_char), Width: 1}, lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "c"}}},
		{typ: planpb.Type{Id: int32(types.T_varchar), Width: 12}, lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "varchar"}}},
	}
	for _, tc := range typesAndLiterals {
		_, err := substraitType(&tc.typ)
		require.NoError(t, err)
		_, err = literal(tc.lit, &tc.typ)
		require.NoError(t, err)
	}

	nullType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	_, err := literal(&planpb.Literal{Isnull: true}, &nullType)
	require.NoError(t, err)
	_, err = literal(nil, &nullType)
	require.ErrorContains(t, err, "nil literal")
	_, err = literal(&planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 1}}, &nullType)
	require.ErrorContains(t, err, "unsupported literal")
	_, err = literal(&planpb.Literal{Value: &planpb.Literal_Sval{Sval: "wrong"}}, &nullType)
	require.ErrorContains(t, err, "does not match")
	_, err = substraitType(nil)
	require.ErrorContains(t, err, "missing type")
	_, err = substraitType(&planpb.Type{Id: int32(types.T_varchar), Width: -1})
	require.ErrorContains(t, err, "negative varchar")
	_, err = substraitType(&planpb.Type{Id: int32(types.T_decimal64)})
	require.ErrorContains(t, err, "unsupported type")

	charType := planpb.Type{Id: int32(types.T_char), Width: 5}
	charSubstrait, err := substraitType(&charType)
	require.NoError(t, err)
	require.Equal(t, int32(5), charSubstrait.GetFixedChar().GetLength())
	charLiteral, err := literal(&planpb.Literal{Value: &planpb.Literal_Sval{Sval: "fixed"}}, &charType)
	require.NoError(t, err)
	require.Equal(t, "fixed", charLiteral.GetLiteral().GetFixedChar())
	_, err = literal(&planpb.Literal{Value: &planpb.Literal_Sval{Sval: "short"}}, &planpb.Type{Id: int32(types.T_char), Width: 8})
	require.True(t, IsNotEligible(err))

	_, err = substraitType(&planpb.Type{Id: int32(types.T_char)})
	require.True(t, IsNotEligible(err))
}

func TestExportRejectsTemporalScansAndLiteralsUntilRepresentationIsDefined(t *testing.T) {
	// Integer remains the nearest admitted control.
	_, err := Export(scanQuery())
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		typ  planpb.Type
		lit  *planpb.Literal
	}{
		{name: "date", typ: planpb.Type{Id: int32(types.T_date)}, lit: &planpb.Literal{Value: &planpb.Literal_Dateval{Dateval: int32(types.DateFromCalendar(1970, 1, 1))}}},
		{name: "timestamp", typ: planpb.Type{Id: int32(types.T_timestamp), Scale: 6}, lit: &planpb.Literal{Value: &planpb.Literal_Timestampval{Timestampval: int64(types.FromClockUTC(1970, 1, 1, 0, 0, 0, 0))}}},
	} {
		t.Run(tc.name+" scan", func(t *testing.T) {
			q := scanQuery()
			q.Nodes[0].TableDef.Cols[0].Typ = tc.typ
			_, err := Export(q)
			require.True(t, IsNotEligible(err))
			reason, ok := NotEligibleReason(err)
			require.True(t, ok)
			require.Equal(t, EligibilityType, reason)
		})
		t.Run(tc.name+" literal", func(t *testing.T) {
			q := scanQuery()
			q.Nodes = append(q.Nodes, &planpb.Node{
				NodeId:      1,
				NodeType:    planpb.Node_PROJECT,
				Children:    []int32{0},
				ProjectList: []*planpb.Expr{{Typ: tc.typ, Expr: &planpb.Expr_Lit{Lit: tc.lit}}},
			})
			q.Steps[0] = 1
			_, err := Export(q)
			require.True(t, IsNotEligible(err))
			reason, ok := NotEligibleReason(err)
			require.True(t, ok)
			require.Equal(t, EligibilityType, reason)
		})
	}
}

func TestEligibilityDeclinesAreDistinctFromMalformedAndOperationalErrors(t *testing.T) {
	unsupported := scanQuery()
	unsupported.Nodes = append(unsupported.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_JOIN})
	unsupported.Steps[0] = 1
	_, err := Export(unsupported)
	require.True(t, IsNotEligible(err))
	reason, ok := NotEligibleReason(err)
	require.True(t, ok)
	require.Equal(t, EligibilityOperator, reason)

	malformed := scanQuery()
	malformed.Nodes[0].ObjRef = nil
	_, err = Export(malformed)
	require.Error(t, err)
	require.False(t, IsNotEligible(err))

	candidate, err := Export(scanQuery())
	require.NoError(t, err)
	_, err = candidate.Build(nil)
	require.Error(t, err)
	require.False(t, IsNotEligible(err), "post-export build failures are operational")
}

func TestCanonicalSchemaAndScalarSignatureContracts(t *testing.T) {
	table := &planpb.TableDef{Name: "t", Cols: []*planpb.ColDef{
		{Name: "a", Typ: i64Type()},
		{Name: "hidden", Hidden: true, Typ: i64Type()},
	}}
	schema, err := CanonicalSchema(table)
	require.NoError(t, err)
	require.NotEmpty(t, schema)
	_, err = CanonicalSchema(&planpb.TableDef{Name: "empty"})
	require.ErrorContains(t, err, "no exportable columns")
	_, err = CanonicalSchema(&planpb.TableDef{Name: "nil", Cols: []*planpb.ColDef{nil}})
	require.ErrorContains(t, err, "nil column")

	boolExpr := &planpb.Expr{Typ: boolType()}
	intExpr := &planpb.Expr{Typ: i64Type()}
	require.NoError(t, validateScalarSignature("and", ptrType(boolType()), []*planpb.Expr{boolExpr, boolExpr}))
	require.NoError(t, validateScalarSignature("equal", ptrType(boolType()), []*planpb.Expr{intExpr, intExpr}))
	require.NoError(t, validateScalarSignature("is_null", ptrType(boolType()), []*planpb.Expr{intExpr}))
	require.NoError(t, validateScalarSignature("add", ptrType(i64Type()), []*planpb.Expr{intExpr, intExpr}))
	require.ErrorContains(t, validateScalarSignature("and", ptrType(i64Type()), []*planpb.Expr{boolExpr, boolExpr}), "non-boolean result")
	require.ErrorContains(t, validateScalarSignature("or", ptrType(boolType()), []*planpb.Expr{intExpr, intExpr}), "non-boolean argument")
	require.ErrorContains(t, validateScalarSignature("equal", ptrType(boolType()), []*planpb.Expr{intExpr, boolExpr}), "unsupported equal")
	require.ErrorContains(t, validateScalarSignature("is_not_null", ptrType(i64Type()), []*planpb.Expr{intExpr}), "unsupported is_not_null")
	require.ErrorContains(t, validateScalarSignature("divide", ptrType(boolType()), []*planpb.Expr{boolExpr, boolExpr}), "unsupported divide")
}

func TestExportAcceptsBinderNullPredicateAliases(t *testing.T) {
	for _, name := range []string{"isnull", "isnotnull"} {
		t.Run(name, func(t *testing.T) {
			q := scanQuery()
			resultType := boolType()
			resultType.NotNullable = true
			q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_FILTER, Children: []int32{0}, FilterList: []*planpb.Expr{fn(name, resultType, col(0))}})
			q.Steps[0] = 1
			_, err := Export(q)
			require.NoError(t, err)
		})
	}
}

func TestNonnegativeIntegerLiteralForms(t *testing.T) {
	for _, expr := range []*planpb.Expr{
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: 1}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I16Val{I16Val: 2}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 3}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 4}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: math.MaxInt64}}}},
	} {
		value, err := nonnegativeIntLiteral(expr, -1)
		require.NoError(t, err)
		require.Positive(t, value)
	}
	value, err := nonnegativeIntLiteral(nil, 7)
	require.NoError(t, err)
	require.Equal(t, int64(7), value)
	_, err = nonnegativeIntLiteral(&planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}, 0)
	require.ErrorContains(t, err, "constant integer")
	_, err = nonnegativeIntLiteral(&planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "1"}}}}, 0)
	require.ErrorContains(t, err, "integer")
	_, err = nonnegativeIntLiteral(&planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: -1}}}}, 0)
	require.ErrorContains(t, err, "non-negative")
	_, err = nonnegativeIntLiteral(&planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: math.MaxInt64 + 1}}}}, 0)
	require.ErrorContains(t, err, "signed integer range")
}

func TestExporterValidationGuards(t *testing.T) {
	_, err := (*Candidate)(nil).Build(nil)
	require.ErrorContains(t, err, "nil candidate")
	candidate, err := Export(scanQuery())
	require.NoError(t, err)
	_, err = candidate.Build(nil)
	require.ErrorContains(t, err, "no admitted TaeRead")

	widthExporter := &exporter{query: &planpb.Query{Nodes: []*planpb.Node{
		{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, ProjectList: []*planpb.Expr{col(0)}},
		{NodeId: 1, NodeType: planpb.Node_TABLE_SCAN},
		{NodeId: 2, NodeType: planpb.Node_FILTER},
		{NodeId: 3, NodeType: planpb.Node_JOIN},
	}}}
	width, err := widthExporter.nodeWidth(0)
	require.NoError(t, err)
	require.Equal(t, 1, width)
	_, err = widthExporter.nodeWidth(-1)
	require.ErrorContains(t, err, "invalid node id")
	_, err = widthExporter.nodeWidth(1)
	require.ErrorContains(t, err, "scan has no table")
	_, err = widthExporter.nodeWidth(2)
	require.ErrorContains(t, err, "requires one child")
	_, err = widthExporter.nodeWidth(3)
	require.ErrorContains(t, err, "unsupported width")

	exprExporter := &exporter{functions: make(map[string]uint32)}
	_, err = exprExporter.conjunction(nil)
	require.ErrorContains(t, err, "empty filter")
	_, err = exprExporter.conjunction([]*planpb.Expr{intExpr(1)})
	require.ErrorContains(t, err, "not boolean")
	predicate := fn(">", boolType(), col(0), i64(1))
	_, err = exprExporter.conjunction([]*planpb.Expr{predicate, predicate})
	require.NoError(t, err)
	require.ErrorContains(t, validateExprFields([]*planpb.Expr{nil}, 1), "nil expression")
	require.ErrorContains(t, validateExprFields([]*planpb.Expr{{Expr: &planpb.Expr_F{}}}, 1), "malformed function")
	require.ErrorContains(t, validateExprFields([]*planpb.Expr{{}}, 1), "unsupported expression")

	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, GroupingFlag: []bool{true}})
	q.Steps[0] = 1
	_, err = Export(q)
	require.ErrorContains(t, err, "grouping sets")

	for _, order := range []*planpb.OrderBySpec{
		nil,
		{Expr: col(0), Collation: "utf8"},
		{Expr: col(0), Flag: planpb.OrderBySpec_UNIQUE},
		{Expr: col(0), Flag: planpb.OrderBySpec_ASC | planpb.OrderBySpec_DESC},
		{Expr: col(0), Flag: planpb.OrderBySpec_NULLS_FIRST | planpb.OrderBySpec_NULLS_LAST},
		{Expr: col(0), Flag: planpb.OrderBySpec_OrderByFlag(1 << 15)},
	} {
		q = scanQuery()
		q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_SORT, Children: []int32{0}, OrderBy: []*planpb.OrderBySpec{order}})
		q.Steps[0] = 1
		_, err = Export(q)
		require.Error(t, err)
	}
}

func ptrType(value planpb.Type) *planpb.Type { return &value }

func intExpr(value int64) *planpb.Expr { return i64(value) }

func boundSQLQuery(t *testing.T, sql string) *planpb.Query {
	t.Helper()
	statement, err := mysql.ParseOne(context.Background(), sql, 1)
	require.NoError(t, err)
	built, err := planbuilder.BuildPlan(planbuilder.NewMockCompilerContext(false), statement, false)
	require.NoError(t, err)
	query := built.GetQuery()
	require.NotNil(t, query)
	for _, node := range query.Nodes {
		if node == nil || node.NodeType != planpb.Node_TABLE_SCAN {
			continue
		}
		require.NotNil(t, node.ObjRef)
		require.NotNil(t, node.TableDef)
		// The mock compiler intentionally leaves catalog IDs unset. Supply only
		// those physical identities so Export sees an otherwise real bound plan.
		node.ObjRef.Db = 7
		if node.TableDef.TblId == 0 {
			node.TableDef.TblId = uint64(node.NodeId) + 42
		}
		node.ObjRef.Obj = int64(node.TableDef.TblId)
	}
	return query
}

func boundNode(t *testing.T, query *planpb.Query, nodeType planpb.Node_NodeType) *planpb.Node {
	t.Helper()
	var found *planpb.Node
	for _, node := range query.Nodes {
		if node != nil && node.NodeType == nodeType {
			require.Nil(t, found, "multiple %s nodes", nodeType.String())
			found = node
		}
	}
	require.NotNil(t, found, "missing %s node", nodeType.String())
	return found
}

func buildSubstraitPlan(t *testing.T, query *planpb.Query) *spb.Plan {
	t.Helper()
	candidate, err := Export(query)
	require.NoError(t, err)
	reads := make(map[int32][]byte, len(candidate.Reads()))
	for _, read := range candidate.Reads() {
		reads[read.NodeID] = []byte{1}
	}
	wire, err := candidate.Build(reads)
	require.NoError(t, err)
	plan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, plan))
	return plan
}

func findSubstraitAggregate(rel *spb.Rel) *spb.AggregateRel {
	for rel != nil {
		if aggregate := rel.GetAggregate(); aggregate != nil {
			return aggregate
		}
		switch {
		case rel.GetProject() != nil:
			rel = rel.GetProject().Input
		case rel.GetFilter() != nil:
			rel = rel.GetFilter().Input
		case rel.GetSort() != nil:
			rel = rel.GetSort().Input
		case rel.GetFetch() != nil:
			rel = rel.GetFetch().Input
		default:
			return nil
		}
	}
	return nil
}

func findSubstraitSort(rel *spb.Rel) *spb.SortRel {
	for rel != nil {
		if sort := rel.GetSort(); sort != nil {
			return sort
		}
		switch {
		case rel.GetProject() != nil:
			rel = rel.GetProject().Input
		case rel.GetFilter() != nil:
			rel = rel.GetFilter().Input
		case rel.GetAggregate() != nil:
			rel = rel.GetAggregate().Input
		case rel.GetFetch() != nil:
			rel = rel.GetFetch().Input
		default:
			return nil
		}
	}
	return nil
}

func scanQuery() *planpb.Query {
	return &planpb.Query{StmtType: planpb.Query_SELECT, Steps: []int32{0}, Headings: []string{"a"}, Nodes: []*planpb.Node{{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Db: 7, Obj: 42, ObjName: "t"}, TableDef: &planpb.TableDef{TblId: 42, Version: 3, Name: "t", TableType: "r", Cols: []*planpb.ColDef{{Name: "a", ColId: 11, Seqnum: 5, Typ: i64Type()}}}}}}
}
func i64Type() planpb.Type  { return planpb.Type{Id: int32(types.T_int64)} }
func boolType() planpb.Type { return planpb.Type{Id: int32(types.T_bool)} }
func f64Type() planpb.Type  { return planpb.Type{Id: int32(types.T_float64)} }
func col(pos int32) *planpb.Expr {
	return &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}}}
}
func i64(v int64) *planpb.Expr {
	return &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: v}}}}
}
func u64(v uint64) *planpb.Expr {
	return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: v}}}}
}
func f64(v float64) *planpb.Expr {
	return &planpb.Expr{Typ: f64Type(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: v}}}}
}
func fn(name string, typ planpb.Type, args ...*planpb.Expr) *planpb.Expr {
	ids := map[string]int32{"=": function.EQUAL, ">": function.GREAT_THAN, "not": function.NOT, "isnull": function.ISNULL, "isnotnull": function.ISNOTNULL, "mod": function.MOD, "sum": function.SUM, "min": function.MIN, "starcount": function.STARCOUNT}
	id, ok := ids[name]
	if !ok {
		panic("missing test function id: " + name)
	}
	return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: name, Obj: function.EncodeOverloadID(id, 0)}, Args: args}}}
}
