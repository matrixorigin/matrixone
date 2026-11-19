// Copyright 2021 - 2026 Matrix Origin
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

package ctl

import (
	"context"
	"strings"
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// mockFlushTxnOperator only needs to make proc.GetTxnOperator() non-nil;
// flush passes it through to StorageEngine.Database, which is mocked per test.
// The embedded nil interface makes any unexpected use fail loudly instead of
// silently satisfying the test.
type mockFlushTxnOperator struct {
	client.TxnOperator
}

type mockFlushRelation struct {
	engine.Relation
	tableID uint64
}

func (r *mockFlushRelation) GetTableID(_ context.Context) uint64 {
	return r.tableID
}

type mockFlushDatabase struct {
	engine.Database
	relation    engine.Relation
	relationErr error
}

func (d *mockFlushDatabase) Relation(_ context.Context, _ string, _ any) (engine.Relation, error) {
	if d.relationErr != nil {
		return nil, d.relationErr
	}
	return d.relation, nil
}

type mockFlushEngine struct {
	engine.Engine
	database    engine.Database
	databaseErr error
	gotCtx      context.Context
	gotDB       string
}

func (e *mockFlushEngine) Database(ctx context.Context, name string, _ client.TxnOperator) (engine.Database, error) {
	e.gotCtx = ctx
	e.gotDB = name
	if e.databaseErr != nil {
		return nil, e.databaseErr
	}
	return e.database, nil
}

func newFlushTestProc(eng engine.Engine, withTxnOp bool) *process.Process {
	ctx := context.Background()
	proc := process.NewTopProcess(ctx, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if withTxnOp {
		proc.Base.TxnOperator = &mockFlushTxnOperator{}
	}
	proc.Base.SessionInfo.StorageEngine = eng
	return proc
}

// flushCaptureSender records the CN op requests built by the handler and
// answers every request with a TN string response, the way the real
// MoCtlTNCmdSender does for OpFlush.
func flushCaptureSender(captured *[]txn.CNOpRequest) requestSender {
	return func(_ context.Context, _ *process.Process, cr []txn.CNOpRequest) ([]txn.CNOpResponse, error) {
		*captured = append(*captured, cr...)
		responses := make([]txn.CNOpResponse, 0, len(cr))
		for range cr {
			responses = append(responses, txn.CNOpResponse{
				Payload: protoc.MustMarshal(&api.TNStringResponse{}),
			})
		}
		return responses, nil
	}
}

func flushRun(proc *process.Process, target string) ([]txn.CNOpRequest, error, *Result) {
	var captured []txn.CNOpRequest
	sender := flushCaptureSender(&captured)
	result, err := handleFlush()(proc, tn, target, sender)
	return captured, err, &result
}

// Malformed or empty flush targets must be rejected as invalid input (20301)
// with a bounded usage hint instead of internal errors or sentinel leaks.
func TestHandleFlushRejectsInvalidTarget(t *testing.T) {
	initTestRuntime(1)
	eng := &mockFlushEngine{
		database: &mockFlushDatabase{
			relation: &mockFlushRelation{tableID: 42},
		},
	}
	proc := newFlushTestProc(eng, true)

	for _, target := range []string{
		"",        // empty
		"a",       // junk single segment, not a numeric TableId
		"12ab",    // junk numeric-ish segment
		"a.b.c.d", // more than 3 segments
		"db.",     // empty table segment
		".tbl",    // empty database segment
		"...",     // all empty segments
		"db..7",   // empty table with account id
		"db.t.x",  // non-numeric account id
	} {
		_, err, _ := flushRun(proc, target)
		require.Error(t, err, "target %q", target)
		require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput),
			"target %q: want ErrInvalidInput 20301, got %v", target, err)
		require.Containsf(t, err.Error(), "TableId", "target %q", target)
		require.Containsf(t, err.Error(), "DbName.TableName", "target %q", target)
	}
}

// StorageEngine.Database reports a database missing from the transaction
// snapshot as the internal OkExpectedEOB sentinel (disttae engine.go). The
// flush handler must translate it to the typed 1049/42000 BadDB error, the
// way the DDL paths do, so the errno-0 sentinel never reaches SQL clients.
func TestHandleFlushTranslatesMissingDBToBadDB(t *testing.T) {
	initTestRuntime(1)
	eng := &mockFlushEngine{databaseErr: moerr.GetOkExpectedEOB()}
	proc := newFlushTestProc(eng, true)

	_, err, _ := flushRun(proc, "nosuch_db_31663.t31663")
	require.Error(t, err)
	require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrBadDB),
		"want ErrBadDB 20402 (wire 1049/42000), got %v", err)
	require.Falsef(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB),
		"internal OkExpectedEOB sentinel must not leak, got %v", err)
	require.Contains(t, err.Error(), "nosuch_db_31663")
}

// Real database errors that are not the missing-db sentinel must pass through
// unchanged.
func TestHandleFlushPreservesOtherDatabaseErrors(t *testing.T) {
	initTestRuntime(1)
	boom := moerr.NewInternalErrorNoCtx("storage error boom 31663")
	eng := &mockFlushEngine{databaseErr: boom}
	proc := newFlushTestProc(eng, true)

	_, err, _ := flushRun(proc, "db31663.t31663")
	require.Error(t, err)
	require.Equal(t, boom, err)
}

// Relation errors (1146 no such table) already carry the right typed contract
// and must keep flowing through unchanged.
func TestHandleFlushPreservesRelationErrors(t *testing.T) {
	initTestRuntime(1)
	noSuch := moerr.NewNoSuchTable(context.Background(), "db31663", "nosuch31663")
	eng := &mockFlushEngine{
		database: &mockFlushDatabase{relationErr: noSuch},
	}
	proc := newFlushTestProc(eng, true)

	_, err, _ := flushRun(proc, "db31663.nosuch31663")
	require.Error(t, err)
	require.Equal(t, noSuch, err)
	require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable),
		"want ErrNoSuchTable (wire 1146), got %v", err)
}

// A missing txn operator is an internal execution invariant, not a client
// input problem; its internal classification stays.
func TestHandleFlushRequiresTxnOperator(t *testing.T) {
	initTestRuntime(1)
	eng := &mockFlushEngine{}
	proc := newFlushTestProc(eng, false)

	_, err, _ := flushRun(proc, "db31663.t31663")
	require.Error(t, err)
	require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrInternal),
		"want ErrInternal 20101, got %v", err)
}

func TestHandleFlushRejectsWrongService(t *testing.T) {
	initTestRuntime(1)
	eng := &mockFlushEngine{}
	proc := newFlushTestProc(eng, true)

	var captured []txn.CNOpRequest
	result, err := handleFlush()(proc, cn, "db31663.t31663", flushCaptureSender(&captured))
	require.Error(t, err)
	require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported),
		"want ErrNotSupported, got %v", err)
	assert.Nil(t, result.Data)
}

// Valid flows keep their exact payload semantics: numeric TableId encodes
// directly; Db.Table resolves the table id through the storage engine;
// Db.Table.AccountId additionally pins the tenant in the lookup context.
func TestHandleFlushValidTargetsBuildRealPayloads(t *testing.T) {
	initTestRuntime(1)
	eng := &mockFlushEngine{
		database: &mockFlushDatabase{
			relation: &mockFlushRelation{tableID: 77},
		},
	}
	proc := newFlushTestProc(eng, true)

	// numeric TableId
	captured, err, result := flushRun(proc, "42")
	require.NoError(t, err)
	require.Len(t, captured, 1)
	assert.Equal(t, uint32(api.OpCode_OpFlush), captured[0].OpCode)
	var ft cmd_util.FlushTable
	require.NoError(t, types.Decode(captured[0].Payload, &ft))
	assert.Equal(t, uint64(42), ft.TableID)
	assert.Equal(t, "Flush", result.Method)
	assert.Len(t, result.Data, 1)

	// DbName.TableName
	captured, err, _ = flushRun(proc, "db31663.t31663")
	require.NoError(t, err)
	require.Len(t, captured, 1)
	require.NoError(t, types.Decode(captured[0].Payload, &ft))
	assert.Equal(t, uint64(77), ft.TableID)
	assert.Equal(t, "db31663", eng.gotDB)
	// 2-part form must not pin a tenant id into the lookup context
	_, hasTenant := eng.gotCtx.Value(defines.TenantIDKey{}).(uint32)
	assert.False(t, hasTenant)

	// DbName.TableName.AccountId pins the tenant context
	captured, err, _ = flushRun(proc, "db31663.t31663.99")
	require.NoError(t, err)
	require.Len(t, captured, 1)
	require.NoError(t, types.Decode(captured[0].Payload, &ft))
	assert.Equal(t, uint64(77), ft.TableID)
	assert.Equal(t, "db31663", eng.gotDB)
	tenantID, ok := eng.gotCtx.Value(defines.TenantIDKey{}).(uint32)
	require.Truef(t, ok, "3-part form must pin TenantIDKey, ctx=%v", eng.gotCtx)
	assert.Equal(t, uint32(99), tenantID)
}

// With no TN shards registered, GetTNHandlerFunc never invokes the payload
// builder and succeeds with empty data. That zero-shard path is why the
// error-path fixtures above must register a real TN shard: the payload
// branch, and therefore the classification under test, only executes when
// the handler has actual TN metadata to drive it.
func TestHandleFlushNoTNShardSucceedsWithoutPayload(t *testing.T) {
	initTestRuntime()
	eng := &mockFlushEngine{databaseErr: moerr.GetOkExpectedEOB()}
	proc := newFlushTestProc(eng, true)

	captured, err, result := flushRun(proc, "nosuch_db_31663.t31663")
	require.NoError(t, err)
	require.Empty(t, captured)
	assert.Empty(t, result.Data)
}

// A long malformed target must keep the 20301 classification, the usage hint
// and a bounded echo: the message has a fixed size ceiling, the tail of the
// oversized input never leaks, and the same driver keeps serving valid
// payloads after the error.
func TestHandleFlushBoundsLongTargetEcho(t *testing.T) {
	initTestRuntime(1)
	eng := &mockFlushEngine{
		database: &mockFlushDatabase{
			relation: &mockFlushRelation{tableID: 42},
		},
	}
	proc := newFlushTestProc(eng, true)

	for _, target := range []string{
		// oversized single-segment junk, not a numeric TableId
		strings.Repeat("z", 1024),
		// oversized junk with far more than three segments
		strings.Repeat("db.t.", 256) + "x",
	} {
		_, err, _ := flushRun(proc, target)
		require.Error(t, err)
		require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput),
			"want ErrInput 20301, got %v", err)
		require.Contains(t, err.Error(), "TableId")
		require.Contains(t, err.Error(), "DbName.TableName")
		require.Contains(t, err.Error(), "...", "oversized echo must carry the truncation mark")
		require.LessOrEqualf(t, len(err.Error()), 512,
			"error message must stay bounded, got len=%d", len(err.Error()))
		require.NotContains(t, err.Error(), strings.Repeat("z", 128),
			"tail of the oversized input must not leak into the message")
	}

	captured, err, _ := flushRun(proc, "42")
	require.NoError(t, err)
	require.Len(t, captured, 1)
	var ft cmd_util.FlushTable
	require.NoError(t, types.Decode(captured[0].Payload, &ft))
	assert.Equal(t, uint64(42), ft.TableID)
}
