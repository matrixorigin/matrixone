// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mongoscan

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type testConnectionResolver struct{ connection mongodb.Connection }

func (r testConnectionResolver) ResolveMongoDBConnection(context.Context, uint32, uint64, uint64) (mongodb.Connection, error) {
	return r.connection, nil
}

type testSecretResolver struct{}

func (testSecretResolver) ResolveMongoDBCredentials(context.Context, uint32, string, string) (mongodb.Credentials, error) {
	return mongodb.Credentials{Username: "reader", Password: "not-serialized"}, nil
}

type testCursor struct {
	mu       sync.Mutex
	docs     [][]byte
	index    int
	current  []byte
	err      error
	closed   int
	blocked  bool
	closeErr error
}

func (c *testCursor) Next(ctx context.Context) bool {
	if c.blocked {
		<-ctx.Done()
		c.err = context.Cause(ctx)
		return false
	}
	if c.index >= len(c.docs) {
		return false
	}
	c.current = c.docs[c.index]
	c.index++
	return true
}
func (c *testCursor) CurrentRaw() []byte { return c.current }
func (c *testCursor) Err() error         { return c.err }
func (c *testCursor) Close(ctx context.Context) error {
	c.mu.Lock()
	c.closed++
	c.closeErr = ctx.Err()
	c.mu.Unlock()
	return nil
}

type testCollection struct {
	cursor mongodb.Cursor
	err    error
}

func (c testCollection) Find(context.Context, mongodb.FindSpec) (mongodb.Cursor, error) {
	return c.cursor, c.err
}

func (c testCollection) Aggregate(context.Context, mongodb.AggregateSpec) (mongodb.Cursor, error) {
	return c.cursor, c.err
}

type recordingCollection struct {
	mu                sync.Mutex
	cursor            mongodb.Cursor
	findErr           error
	aggregateErr      error
	findSpecs         []mongodb.FindSpec
	aggregateSpecs    []mongodb.AggregateSpec
	findContexts      []context.Context
	aggregateContexts []context.Context
}

// minimalCarrierCollection models MongoDB applying the connector projection to
// a source document. It returns the large source unchanged for an exclusion or
// missing projection, making the row-count converter enforce MaxValueBytes.
type minimalCarrierCollection struct {
	source         []byte
	findSpecs      []mongodb.FindSpec
	aggregateSpecs []mongodb.AggregateSpec
}

func (c *minimalCarrierCollection) Find(_ context.Context, spec mongodb.FindSpec) (mongodb.Cursor, error) {
	c.findSpecs = append(c.findSpecs, spec)
	projection, _ := spec.Projection.(bson.D)
	return c.cursorFor(projection), nil
}

func (c *minimalCarrierCollection) Aggregate(_ context.Context, spec mongodb.AggregateSpec) (mongodb.Cursor, error) {
	c.aggregateSpecs = append(c.aggregateSpecs, spec)
	pipeline, _ := spec.Pipeline.([]bson.D)
	if len(pipeline) == 0 {
		return c.cursorFor(nil), nil
	}
	last := pipeline[len(pipeline)-1]
	if len(last) != 1 || last[0].Key != "$project" {
		return c.cursorFor(nil), nil
	}
	projection, _ := last[0].Value.(bson.D)
	return c.cursorFor(projection), nil
}

func (c *minimalCarrierCollection) cursorFor(projection bson.D) mongodb.Cursor {
	if len(projection) == 1 && projection[0].Key == "_id" && projection[0].Value == 1 {
		carrier, err := bson.Marshal(bson.D{{Key: "_id", Value: "carrier"}})
		if err == nil {
			return &testCursor{docs: [][]byte{carrier}}
		}
	}
	return &testCursor{docs: [][]byte{c.source}}
}

func (c *recordingCollection) Find(ctx context.Context, spec mongodb.FindSpec) (mongodb.Cursor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.findSpecs = append(c.findSpecs, spec)
	c.findContexts = append(c.findContexts, ctx)
	return c.cursor, c.findErr
}

func (c *recordingCollection) Aggregate(ctx context.Context, spec mongodb.AggregateSpec) (mongodb.Cursor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.aggregateSpecs = append(c.aggregateSpecs, spec)
	c.aggregateContexts = append(c.aggregateContexts, ctx)
	return c.cursor, c.aggregateErr
}

type testClient struct {
	collection mongodb.Collection
	disconnect int
}

func (c *testClient) Collection(string, string) mongodb.Collection { return c.collection }
func (*testClient) Ping(context.Context) error                     { return nil }
func (c *testClient) Disconnect(context.Context) error             { c.disconnect++; return nil }

type testFactory struct{ client mongodb.Client }

func (f testFactory) Connect(context.Context, mongodb.Connection, mongodb.Credentials, mongodb.RuntimeConfig) (mongodb.Client, error) {
	if f.client == nil {
		return nil, errors.New("find setup failed")
	}
	return f.client, nil
}

type testMappingResolver struct{ mapping mongodb.TableMapping }

func (r testMappingResolver) ResolveMongoDBMapping(context.Context, uint32, uint64, uint64) (mongodb.TableMapping, error) {
	return r.mapping, nil
}

func testScanDependencies(cursor mongodb.Cursor) (*mongodb.RuntimeDependencies, *testClient) {
	connection := mongodb.Connection{AccountID: 7, ConnectionID: 9, Version: 3, CredentialSecretRef: "secret://env/MONGO"}
	client := &testClient{collection: testCollection{cursor: cursor}}
	cfg := mongodb.DefaultRuntimeConfig()
	cfg.Enable = true
	cfg.BatchRows = 1
	return &mongodb.RuntimeDependencies{
		Config: cfg, Connections: testConnectionResolver{connection: connection}, Secrets: testSecretResolver{},
		Mappings: testMappingResolver{mapping: mongodb.TableMapping{
			TableID: 7, MappingID: 8, ConnectionID: 9, Database: "db", Collection: "readings", Version: 2,
			MaxParallelism: 1,
			Columns:        []mongodb.ColumnMapping{{Name: "value", Path: "value", TypeID: int32(types.T_int64), Conversion: mongodb.ConversionStrict}},
		}},
		Pool: mongodb.NewClientPool(testFactory{client: client}), Limiter: mongodb.NewSourceLimiter(1),
	}, client
}

func testScanPlan() *plan.MongoScan {
	return &plan.MongoScan{
		TableId: 7, MappingId: 8, MappingVersion: 2, ConnectionId: 9, ConnectionVersion: 3, Database: "db", Collection: "readings", MaxParallelism: 1,
		Columns: []*plan.MongoColumnMapping{{Name: "value", Path: "value", MoType: plan.Type{Id: int32(types.T_int64)}, ConversionMode: mongodb.ConversionStrict}},
	}
}

func applyTestUserQueryPlan(t *testing.T, spec *plan.MongoScan, source string, includeQueryColumn bool) {
	t.Helper()
	query, err := mongodb.ParseUserQuery(t.Context(), source)
	require.NoError(t, err)
	spec.IncludeQueryColumn = includeQueryColumn
	require.NoError(t, mongodb.ApplyUserQueryToPlan(t.Context(), query, spec))
}

func TestMongoScanExplicitFilterUsesFindAndPopulatesQueryColumn(t *testing.T) {
	source := `{"filter":{"device_id":"pump-1"}}`
	doc, err := bson.Marshal(bson.D{{Key: "value", Value: int64(11)}})
	require.NoError(t, err)
	cursor := &testCursor{docs: [][]byte{doc}}
	collection := &recordingCollection{cursor: cursor}
	deps, client := testScanDependencies(cursor)
	client.collection = collection
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	spec := testScanPlan()
	applyTestUserQueryPlan(t, spec, source, true)
	spec.IncludeQueryColumn = true
	automatic, err := mongodb.PredicateToPlan(t.Context(), &mongodb.Predicate{
		Op: mongodb.PredicateGreaterEqual, Path: "value", Value: int64(10),
	})
	require.NoError(t, err)
	spec.PushedPredicate = automatic
	scan := NewArgument().WithScan(spec)
	scan.Dependencies = deps
	require.NoError(t, scan.Prepare(proc))

	result, err := scan.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, int64(11), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
	require.Equal(t, source, result.Batch.Vecs[1].GetStringAt(0))
	require.Equal(t, []string{"value", "__mo_query"}, result.Batch.Attrs)
	require.Len(t, collection.findSpecs, 1)
	require.Empty(t, collection.aggregateSpecs)
	filter, ok := collection.findSpecs[0].Filter.(bson.D)
	require.True(t, ok)
	require.Equal(t, "$and", filter[0].Key)
	require.Equal(t, int32(1), collection.findSpecs[0].BatchSize)
	deadline, ok := collection.findContexts[0].Deadline()
	require.True(t, ok)
	require.LessOrEqual(t, time.Until(deadline), mongodb.MaxUserQueryExecution)
	require.Greater(t, time.Until(deadline), mongodb.MaxUserQueryExecution-time.Second)

	scan.Free(proc, false, nil)
	require.Equal(t, 1, cursor.closed)
	require.NoError(t, deps.Pool.Close(t.Context()))
	require.Equal(t, 1, client.disconnect)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMongoScanExplicitQueryRejectsRolledBackProtocolBeforeMongoCall(t *testing.T) {
	cursor := &testCursor{}
	collection := &recordingCollection{cursor: cursor}
	deps, client := testScanDependencies(cursor)
	client.collection = collection
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion37)
		}
	})

	// This plan represents a statement compiled while the cluster supported the
	// payload. A rollback before Prepare must fail before any client/driver call.
	spec := testScanPlan()
	applyTestUserQueryPlan(t, spec, `{"filter":{"value":1}}`, false)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion37)
	scan := NewArgument().WithScan(spec)
	scan.Dependencies = deps
	require.ErrorContains(t, scan.Prepare(proc), "MORPC protocol version 38")
	require.Empty(t, collection.findSpecs)
	require.Empty(t, collection.aggregateSpecs)
	require.NoError(t, deps.Pool.Close(t.Context()))
	proc.Free()
}

func TestMongoScanExplicitPipelineUsesAggregateAndMappedOutput(t *testing.T) {
	source := `{"pipeline":[{"$match":{"value":{"$gte":10}}},{"$group":{"_id":null,"value":{"$sum":"$value"}}},{"$project":{"_id":0,"value":1}}]}`
	doc, err := bson.Marshal(bson.D{{Key: "value", Value: int64(33)}})
	require.NoError(t, err)
	cursor := &testCursor{docs: [][]byte{doc}}
	collection := &recordingCollection{cursor: cursor}
	deps, client := testScanDependencies(cursor)
	client.collection = collection
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	spec := testScanPlan()
	applyTestUserQueryPlan(t, spec, source, false)
	// A pipeline operates first. Any SQL predicate remains an MO residual and
	// must not be injected into this opaque pipeline.
	spec.PushedPredicate = &plan.MongoPredicate{
		Op: plan.MongoPredicateOp_MONGO_PREDICATE_EQUAL, Path: "value",
	}
	scan := NewArgument().WithScan(spec)
	scan.Dependencies = deps
	require.NoError(t, scan.Prepare(proc))

	result, err := scan.Call(proc)
	require.NoError(t, err)
	require.Equal(t, int64(33), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
	require.Empty(t, collection.findSpecs)
	require.Len(t, collection.aggregateSpecs, 1)
	pipeline, ok := collection.aggregateSpecs[0].Pipeline.([]bson.D)
	require.True(t, ok)
	require.Len(t, pipeline, 4, "connector projection is appended after the validated user pipeline")
	require.Equal(t, "$project", pipeline[len(pipeline)-1][0].Key)
	require.Equal(t, int32(1), collection.aggregateSpecs[0].BatchSize)

	scan.Free(proc, false, nil)
	require.Equal(t, 1, cursor.closed)
	require.NoError(t, deps.Pool.Close(t.Context()))
	require.Equal(t, 1, client.disconnect)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMongoScanExplicitQuerySupportsZeroColumnRowCarrier(t *testing.T) {
	doc, err := bson.Marshal(bson.D{{Key: "ignored", Value: int64(1)}})
	require.NoError(t, err)
	cursor := &testCursor{docs: [][]byte{doc}}
	collection := &recordingCollection{cursor: cursor}
	deps, client := testScanDependencies(cursor)
	client.collection = collection
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	spec := testScanPlan()
	spec.Columns = nil
	applyTestUserQueryPlan(t, spec, `{"filter":{"device_id":"pump-1"}}`, false)
	scan := NewArgument().WithScan(spec)
	scan.Dependencies = deps
	require.NoError(t, scan.Prepare(proc))

	result, err := scan.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Empty(t, result.Batch.Vecs)
	require.Len(t, collection.findSpecs, 1)

	scan.Free(proc, false, nil)
	require.Equal(t, 1, cursor.closed)
	require.NoError(t, deps.Pool.Close(t.Context()))
	require.Equal(t, 1, client.disconnect)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMongoScanZeroColumnQueryProjectsMinimalCarrier(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
	}{
		{name: "find", query: `{"filter":{"device_id":"pump-1"}}`},
		{name: "aggregate", query: `{"pipeline":[{"$match":{"device_id":"pump-1"}}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, err := bson.Marshal(bson.D{
				{Key: "_id", Value: "carrier"},
				{Key: "irrelevant", Value: strings.Repeat("x", 1024)},
			})
			require.NoError(t, err)
			collection := &minimalCarrierCollection{source: source}
			deps, client := testScanDependencies(&testCursor{})
			deps.Config.MaxValueBytes = 128
			client.collection = collection
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
			spec := testScanPlan()
			spec.Columns = nil
			applyTestUserQueryPlan(t, spec, tc.query, false)
			scan := NewArgument().WithScan(spec)
			scan.Dependencies = deps
			require.NoError(t, scan.Prepare(proc))

			result, err := scan.Call(proc)
			require.NoError(t, err)
			require.NotNil(t, result.Batch)
			require.Equal(t, 1, result.Batch.RowCount())
			require.Empty(t, result.Batch.Vecs)
			if tc.name == "find" {
				require.Len(t, collection.findSpecs, 1)
				require.Equal(t, bson.D{{Key: "_id", Value: 1}}, collection.findSpecs[0].Projection)
				require.Empty(t, collection.aggregateSpecs)
			} else {
				require.Empty(t, collection.findSpecs)
				require.Len(t, collection.aggregateSpecs, 1)
				pipeline := collection.aggregateSpecs[0].Pipeline.([]bson.D)
				require.Equal(t, bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}}, pipeline[len(pipeline)-1])
			}

			scan.Free(proc, false, nil)
			require.NoError(t, deps.Pool.Close(t.Context()))
			require.Equal(t, 1, client.disconnect)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestMongoScanEmptyUserQueryResultAvoidsRemoteOperation(t *testing.T) {
	collection := &recordingCollection{cursor: &testCursor{}}
	deps, client := testScanDependencies(collection.cursor)
	client.collection = collection
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	spec := testScanPlan()
	spec.EmptyResult = true
	scan := NewArgument().WithScan(spec)
	scan.Dependencies = deps
	require.NoError(t, scan.Prepare(proc))
	result, err := scan.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Empty(t, collection.findSpecs)
	require.Empty(t, collection.aggregateSpecs)
	scan.Free(proc, false, nil)
	require.NoError(t, deps.Pool.Close(t.Context()))
	require.Zero(t, client.disconnect, "an empty candidate set must not acquire a client")
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMongoScanRejectsUnsafeSerializedPipelineBeforeRemoteOperation(t *testing.T) {
	collection := &recordingCollection{cursor: &testCursor{}}
	deps, client := testScanDependencies(collection.cursor)
	client.collection = collection
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	spec := testScanPlan()
	applyTestUserQueryPlan(t, spec, `{"pipeline":[{"$match":{}}]}`, false)
	unsafeStage, err := bson.Marshal(bson.D{{Key: "$out", Value: "archive"}})
	require.NoError(t, err)
	spec.UserPipelineStageBson[0] = unsafeStage
	scan := NewArgument().WithScan(spec)
	scan.Dependencies = deps
	require.ErrorContains(t, scan.Prepare(proc), "is not allowed")
	require.Empty(t, collection.findSpecs)
	require.Empty(t, collection.aggregateSpecs)
	scan.Free(proc, true, nil)
	require.NoError(t, deps.Pool.Close(t.Context()))
	require.Zero(t, client.disconnect, "plan validation must fail before acquiring a client")
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMongoScanAggregateFailureReleasesSourceAndClientLeases(t *testing.T) {
	collection := &recordingCollection{aggregateErr: errors.New("injected aggregate error")}
	deps, client := testScanDependencies(nil)
	client.collection = collection
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	spec := testScanPlan()
	applyTestUserQueryPlan(t, spec, `{"pipeline":[{"$match":{}}]}`, false)
	scan := NewArgument().WithScan(spec)
	scan.Dependencies = deps
	require.ErrorContains(t, scan.Prepare(proc), "MongoDB aggregate failed")
	require.Empty(t, collection.findSpecs)
	require.Len(t, collection.aggregateSpecs, 1)
	release, err := deps.Limiter.Acquire(proc.Ctx, 7, 9)
	require.NoError(t, err, "aggregate failure must release the source lease")
	release()
	scan.Free(proc, true, nil)
	require.NoError(t, deps.Pool.Close(t.Context()))
	require.Equal(t, 1, client.disconnect)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMongoScanMultiBatchLifecycleAndReuse(t *testing.T) {
	doc1, err := bson.Marshal(bson.D{{Key: "value", Value: int64(11)}})
	require.NoError(t, err)
	doc2, err := bson.Marshal(bson.D{{Key: "value", Value: int64(22)}})
	require.NoError(t, err)
	cursor := &testCursor{docs: [][]byte{doc1, doc2}}
	deps, client := testScanDependencies(cursor)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	scan := NewArgument().WithScan(testScanPlan())
	scan.Dependencies = deps
	require.NoError(t, scan.Prepare(proc))
	var values []int64
	for {
		result, callErr := scan.Call(proc)
		require.NoError(t, callErr)
		if result.Batch != nil {
			values = append(values, vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	require.Equal(t, []int64{11, 22}, values)
	scan.Reset(proc, false, nil)
	scan.Free(proc, false, nil)
	scan.Free(proc, false, nil)
	require.Equal(t, 1, cursor.closed)
	require.NoError(t, deps.Pool.Close(context.Background()))
	require.Equal(t, 1, client.disconnect)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMongoScanRejectsMappingSnapshotDriftBeforeConnecting(t *testing.T) {
	deps, client := testScanDependencies(&testCursor{})
	resolver := deps.Mappings.(testMappingResolver)
	resolver.mapping.Collection = "redirected"
	deps.Mappings = resolver
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	scan := NewArgument().WithScan(testScanPlan())
	scan.Dependencies = deps
	require.ErrorContains(t, scan.Prepare(proc), "mapping changed")
	scan.Free(proc, true, nil)
	require.NoError(t, deps.Pool.Close(t.Context()))
	require.Zero(t, client.disconnect, "snapshot drift must fail before opening a MongoDB client")
	proc.Free()
}

func TestMongoScanRejectsUnimplementedSplitBeforeConnecting(t *testing.T) {
	deps, client := testScanDependencies(&testCursor{})
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	spec := testScanPlan()
	spec.Split = &plan.MongoSplit{KeyPath: "ts"}
	scan := NewArgument().WithScan(spec)
	scan.Dependencies = deps
	require.ErrorContains(t, scan.Prepare(proc), "split")
	scan.Free(proc, true, nil)
	require.NoError(t, deps.Pool.Close(t.Context()))
	require.Zero(t, client.disconnect, "unsupported split must fail before opening a MongoDB client")
	proc.Free()
}

func TestMongoScanCancelBlockedGetMore(t *testing.T) {
	cursor := &testCursor{blocked: true}
	deps, _ := testScanDependencies(cursor)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	ctx, cancel := context.WithCancel(defines.AttachAccountId(proc.Ctx, 7))
	proc.Ctx = ctx
	spec := testScanPlan()
	applyTestUserQueryPlan(t, spec, `{"pipeline":[{"$match":{}}]}`, false)
	scan := NewArgument().WithScan(spec)
	scan.Dependencies = deps
	require.NoError(t, scan.Prepare(proc))
	cancel()
	_, err := scan.Call(proc)
	require.Error(t, err)
	scan.Free(proc, true, err)
	require.Equal(t, 1, cursor.closed)
	require.NoError(t, cursor.closeErr, "cleanup must not inherit the canceled statement context")
	require.NoError(t, deps.Pool.Close(context.Background()))
	proc.Free()
}

func TestMongoScanExplicitQueryDeadlineRejectsBufferedDocument(t *testing.T) {
	doc, err := bson.Marshal(bson.D{{Key: "value", Value: int64(11)}})
	require.NoError(t, err)
	cursor := &testCursor{docs: [][]byte{doc}}
	deps, _ := testScanDependencies(cursor)
	deps.Config.SocketTimeout = time.Nanosecond
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion38)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion38)
		}
	})

	spec := testScanPlan()
	applyTestUserQueryPlan(t, spec, `{"filter":{"value":11}}`, false)
	scan := NewArgument().WithScan(spec)
	scan.Dependencies = deps
	require.NoError(t, scan.Prepare(proc))
	<-scan.ctr.queryCtx.Done()

	result, err := scan.Call(proc)
	require.ErrorContains(t, err, "MongoDB explicit query deadline exceeded")
	require.Nil(t, result.Batch)
	require.Zero(t, cursor.index, "a buffered document must not be emitted after the deadline")
	require.Equal(t, 1, cursor.closed)
	require.NoError(t, deps.Pool.Close(context.Background()))
	proc.Free()
}

func TestMongoScanEmptyAndFindFailureReleaseResources(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		cursor := &testCursor{}
		deps, client := testScanDependencies(cursor)
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		scan := NewArgument().WithScan(testScanPlan())
		scan.Dependencies = deps
		require.NoError(t, scan.Prepare(proc))
		result, err := scan.Call(proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, result.Status)
		require.Nil(t, result.Batch)
		scan.Reset(proc, false, nil)
		scan.Free(proc, false, nil)
		require.Equal(t, 1, cursor.closed)
		require.NoError(t, deps.Pool.Close(context.Background()))
		require.Equal(t, 1, client.disconnect)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	t.Run("find-error", func(t *testing.T) {
		deps, client := testScanDependencies(nil)
		client.collection = testCollection{err: errors.New("injected find error")}
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		scan := NewArgument().WithScan(testScanPlan())
		scan.Dependencies = deps
		require.Error(t, scan.Prepare(proc))
		// Prepare acquired the source semaphore before Find. A second lease can
		// succeed only if the error unwind released it.
		release, err := deps.Limiter.Acquire(proc.Ctx, 7, 9)
		require.NoError(t, err)
		release()
		scan.Free(proc, true, err)
		require.NoError(t, deps.Pool.Close(context.Background()))
		require.Equal(t, 1, client.disconnect)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
}

func TestMongoScanCursorErrorAndGenerationReuse(t *testing.T) {
	querySource := `{"pipeline":[{"$match":{}}]}`
	doc1, err := bson.Marshal(bson.D{{Key: "value", Value: int64(11)}})
	require.NoError(t, err)
	firstCursor := &testCursor{docs: [][]byte{doc1}, err: errors.New("injected getMore error")}
	deps, client := testScanDependencies(firstCursor)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	spec := testScanPlan()
	applyTestUserQueryPlan(t, spec, querySource, true)
	scan := NewArgument().WithScan(spec)
	scan.Dependencies = deps
	require.NoError(t, scan.Prepare(proc))
	result, err := scan.Call(proc)
	require.NoError(t, err)
	require.Equal(t, int64(11), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
	require.Equal(t, querySource, result.Batch.Vecs[1].GetStringAt(0))
	_, err = scan.Call(proc)
	require.Error(t, err)
	require.Equal(t, 1, firstCursor.closed)

	scan.Reset(proc, true, err)
	doc2, marshalErr := bson.Marshal(bson.D{{Key: "value", Value: int64(22)}})
	require.NoError(t, marshalErr)
	secondCursor := &testCursor{docs: [][]byte{doc2}}
	client.collection = testCollection{cursor: secondCursor}
	require.NoError(t, scan.Prepare(proc))
	result, err = scan.Call(proc)
	require.NoError(t, err)
	require.Equal(t, int64(22), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
	require.Equal(t, querySource, result.Batch.Vecs[1].GetStringAt(0))
	result, err = scan.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Equal(t, 1, secondCursor.closed)

	scan.Free(proc, false, nil)
	scan.Free(proc, false, nil)
	require.NoError(t, deps.Pool.Close(context.Background()))
	require.Equal(t, 1, client.disconnect, "the current client generation is reused across Prepare calls")
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMongoScanPartialPrepareFailureReleasesLease(t *testing.T) {
	cursor := &testCursor{}
	deps, client := testScanDependencies(cursor)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	invalid := testScanPlan()
	invalid.Columns[0].MoType.Id = int32(types.T_array_float32)
	resolver := deps.Mappings.(testMappingResolver)
	resolver.mapping.Columns[0].TypeID = int32(types.T_array_float32)
	deps.Mappings = resolver
	scan := NewArgument().WithScan(invalid)
	scan.Dependencies = deps
	require.Error(t, scan.Prepare(proc))
	release, err := deps.Limiter.Acquire(proc.Ctx, 7, 9)
	require.NoError(t, err)
	release()
	scan.Free(proc, true, err)
	require.Zero(t, cursor.closed, "Find was never reached")
	require.NoError(t, deps.Pool.Close(context.Background()))
	require.Equal(t, 1, client.disconnect)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMongoScanBatchAndStatementLimits(t *testing.T) {
	doc1, err := bson.Marshal(bson.D{{Key: "value", Value: int64(11)}})
	require.NoError(t, err)
	doc2, err := bson.Marshal(bson.D{{Key: "value", Value: int64(22)}})
	require.NoError(t, err)

	t.Run("bounded pending document", func(t *testing.T) {
		cursor := &testCursor{docs: [][]byte{doc1, doc2}}
		deps, _ := testScanDependencies(cursor)
		deps.Config.BatchRows = 10
		deps.Config.MaxBatchBytes = int64(len(doc1) + 1)
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		scan := NewArgument().WithScan(testScanPlan())
		scan.Dependencies = deps
		require.NoError(t, scan.Prepare(proc))
		first, err := scan.Call(proc)
		require.NoError(t, err)
		require.Equal(t, int64(11), vector.GetFixedAtNoTypeCheck[int64](first.Batch.Vecs[0], 0))
		require.LessOrEqual(t, int64(len(scan.ctr.pendingRaw)), deps.Config.MaxBatchBytes)
		second, err := scan.Call(proc)
		require.NoError(t, err)
		require.Equal(t, int64(22), vector.GetFixedAtNoTypeCheck[int64](second.Batch.Vecs[0], 0))
		stop, err := scan.Call(proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, stop.Status)
		scan.Free(proc, false, nil)
		require.NoError(t, deps.Pool.Close(t.Context()))
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	t.Run("single document exceeds batch", func(t *testing.T) {
		cursor := &testCursor{docs: [][]byte{doc1}}
		deps, _ := testScanDependencies(cursor)
		deps.Config.MaxBatchBytes = int64(len(doc1) - 1)
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		scan := NewArgument().WithScan(testScanPlan())
		scan.Dependencies = deps
		require.NoError(t, scan.Prepare(proc))
		_, err := scan.Call(proc)
		require.ErrorContains(t, err, "batch byte limit")
		require.Equal(t, 1, cursor.closed)
		scan.Free(proc, true, err)
		require.NoError(t, deps.Pool.Close(t.Context()))
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	t.Run("decoded duplicated projection exceeds batch", func(t *testing.T) {
		smallDoc, err := bson.Marshal(bson.D{{Key: "payload", Value: bson.Binary{Data: []byte("fits")}}})
		require.NoError(t, err)
		payload := make([]byte, 256<<10)
		doc, err := bson.Marshal(bson.D{{Key: "payload", Value: bson.Binary{Data: payload}}})
		require.NoError(t, err)
		cursor := &testCursor{docs: [][]byte{smallDoc, doc}}
		deps, _ := testScanDependencies(cursor)
		deps.Config.BatchRows = 10
		deps.Config.MaxBatchBytes = 1 << 20
		mapping := deps.Mappings.(testMappingResolver)
		mapping.mapping.Columns = nil
		spec := testScanPlan()
		spec.Columns = nil
		for i := range 8 {
			name := fmt.Sprintf("payload_%d", i)
			mapping.mapping.Columns = append(mapping.mapping.Columns, mongodb.ColumnMapping{
				Name: name, Path: "payload", TypeID: int32(types.T_blob), Conversion: mongodb.ConversionStrict,
			})
			spec.Columns = append(spec.Columns, &plan.MongoColumnMapping{
				Name: name, Path: "payload", MoType: plan.Type{Id: int32(types.T_blob)}, ConversionMode: mongodb.ConversionStrict,
			})
		}
		deps.Mappings = mapping
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		scan := NewArgument().WithScan(spec)
		scan.Dependencies = deps
		require.NoError(t, scan.Prepare(proc))
		first, err := scan.Call(proc)
		require.NoError(t, err)
		require.Equal(t, 1, first.Batch.RowCount())
		require.NotEmpty(t, scan.ctr.pendingRaw)
		_, err = scan.Call(proc)
		require.True(t, mongodb.IsDecodedBatchBudgetExceeded(err))
		require.Equal(t, 1, cursor.closed)
		scan.Free(proc, true, err)
		require.NoError(t, deps.Pool.Close(t.Context()))
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	t.Run("statement row limit", func(t *testing.T) {
		cursor := &testCursor{docs: [][]byte{doc1, doc2}}
		deps, _ := testScanDependencies(cursor)
		deps.Config.BatchRows = 10
		deps.Config.MaxScanRows = 1
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		scan := NewArgument().WithScan(testScanPlan())
		scan.Dependencies = deps
		require.NoError(t, scan.Prepare(proc))
		_, err := scan.Call(proc)
		require.ErrorContains(t, err, "statement scan limit")
		require.Equal(t, 1, cursor.closed)
		scan.Free(proc, true, err)
		require.NoError(t, deps.Pool.Close(t.Context()))
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
}
