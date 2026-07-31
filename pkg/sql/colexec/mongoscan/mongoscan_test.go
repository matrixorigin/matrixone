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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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
	scan := NewArgument().WithScan(testScanPlan())
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
	doc1, err := bson.Marshal(bson.D{{Key: "value", Value: int64(11)}})
	require.NoError(t, err)
	firstCursor := &testCursor{docs: [][]byte{doc1}, err: errors.New("injected getMore error")}
	deps, client := testScanDependencies(firstCursor)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	scan := NewArgument().WithScan(testScanPlan())
	scan.Dependencies = deps
	require.NoError(t, scan.Prepare(proc))
	result, err := scan.Call(proc)
	require.NoError(t, err)
	require.Equal(t, int64(11), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
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
