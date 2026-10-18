// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestNativePADCreateTableWriteGateBeforeSideEffects(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	key := moruntime.PersistedExpressionProtocolAuthoringFloor
	previous, exists := rt.GetGlobalVariables(key)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(key, previous)
		} else if current, ok := rt.GetGlobalVariables(key); ok {
			rt.CompareAndDeleteGlobalVariables(key, current)
		}
	})
	c.e = nil
	for _, create := range []*planpb.CreateTable{
		{TableDef: &planpb.TableDef{Name: "n", Cols: []*planpb.ColDef{{Name: "s", Typ: planpb.Type{Id: int32(types.T_varchar), Charset: 3, CollationVersion: 1}}}}},
		{TableDef: &planpb.TableDef{Name: "n", Cols: []*planpb.ColDef{{Name: "b", Typ: types.T_bool.ToType().PlanType()}}}, CreateAsSelectSql: "insert into n select 'A' collate utf8mb4_general_ci='a'", CollationBindingRevision: 1, RequiredProtocolVersion: defines.MORPCVersion119},
	} {
		rt.SetGlobalVariables(key, defines.MORPCVersion119)
		require.NoError(t, plan2.RequireCreateTableCollationsForAuthoring(c.proc.Ctx, c.proc, create))
		rt.SetGlobalVariables(key, defines.MORPCVersion118)
		scope := &Scope{Proc: c.proc, Plan: &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{Definition: &planpb.DataDefinition_CreateTable{CreateTable: create}}}}}
		require.ErrorContains(t, scope.CreateTable(c), "protocol version 119")
	}
}

func TestNativePADViewWriteGateBeforeSideEffects(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	key := moruntime.PersistedExpressionProtocolAuthoringFloor
	previous, existed := rt.GetGlobalVariables(key)
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(key, previous)
		} else if current, ok := rt.GetGlobalVariables(key); ok {
			rt.CompareAndDeleteGlobalVariables(key, current)
		}
	})
	table := &planpb.TableDef{Name: "v", ViewSql: &planpb.ViewDef{View: `{"Stmt":"create view v as select 'A' collate utf8mb4_general_ci = 'a' as b","collation_binding_revision":1,"required_protocol_version":119}`}, Cols: []*planpb.ColDef{{Name: "b", Typ: types.T_bool.ToType().PlanType()}}}
	rt.SetGlobalVariables(key, defines.MORPCVersion119)
	require.NoError(t, plan2.RequireCatalogCollationsForAuthoring(c.proc.Ctx, c.proc, table))
	// 计划生成后写门回落：即使只剩bool类型，CREATE OR REPLACE不能先DROP，
	// ALTER不能先查/删旧对象。nil engine意味着任何触达存储都会使本例失败。
	c.e = nil
	rt.SetGlobalVariables(key, defines.MORPCVersion118)
	for _, alter := range []bool{false, true} {
		ddl := &planpb.DataDefinition{}
		if alter {
			ddl.Definition = &planpb.DataDefinition_AlterView{AlterView: &planpb.AlterView{TableDef: table}}
		} else {
			ddl.Definition = &planpb.DataDefinition_CreateView{CreateView: &planpb.CreateView{TableDef: table, Replace: true}}
		}
		scope := &Scope{Proc: c.proc, Plan: &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: ddl}}}
		var err error
		if alter {
			err = scope.AlterView(c)
		} else {
			err = scope.CreateView(c)
		}
		require.ErrorContains(t, err, "protocol version 119")
	}
}

func TestNativePADProtocolPlacementSourceDestinationReceiver(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	native := planpb.Type{Id: int32(types.T_varchar), Width: 8, Charset: 3, CollationVersion: 1}
	expr := &planpb.Expr{Typ: native, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "A"}}}}
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.NativePADCollation)
	require.Equal(t, defines.MORPCVersion119, remoteExpressionProtocolVersion(features))
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, version := range []int64{117, 118, 119, 118} {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{scope.NodeInfo}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		_, err = encodeRemoteScope(scope, c.proc)
		if version < 119 {
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			require.ErrorContains(t, err, "version 119")
		} else {
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			require.NoError(t, err)
		}
	}
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, features), "versioned remote destination")
	require.ErrorContains(t, validateRemoteExpressionDestination(nil, &pipeline.Pipeline{Node: &pipeline.NodeInfo{}}, features), "version 119")
	client.customResponse = true
	client.response = &query.Response{}
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 119")
	require.Equal(t, client.calls, client.releases)
	client.customResponse = false
	client.version = defines.MORPCVersion119
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, version := range []any{nil, "119", int64(118), int64(119), int64(118)} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		_, err = encodeRemoteScope(scope, c.proc)
		if version == int64(119) {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "version 119")
		}
	}
	for _, instruction := range []*pipeline.Instruction{
		{Op: int32(vm.Projection), ProjectList: []*planpb.Expr{expr}},
		{Op: int32(vm.Group), Agg: &pipeline.Group{Types: []planpb.Type{native}}},
		{Op: int32(vm.HashJoin), HashJoin: &pipeline.HashJoin{LeftTypes: []planpb.Type{native}}},
	} {
		for _, nested := range []bool{false, true} {
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{instruction}}
			if nested {
				p = &pipeline.Pipeline{Children: []*pipeline.Pipeline{p}}
			}
			f, err := requiredRemotePipelineExpressionFeatures(p)
			require.NoError(t, err)
			require.True(t, f.NativePADCollation, "没有Expr的聚合/join类型也必须进入协议门")
			data, err := p.Marshal()
			require.NoError(t, err)
			require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(nil, p), "version 119")
			for _, version := range []any{nil, "119", int64(118), int64(119)} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				if version == int64(119) {
					require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
					// 完整算子解码正例使用可执行projection；裸类型另行验证完整owner遍历。
					if instruction.Op != int32(vm.Projection) {
						continue
					}
				}
				decoded, err := decodeScope(data, c.proc, true, nil)
				if decoded != nil {
					decoded.release()
				}
				if version == int64(119) {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "version 119")
					require.Nil(t, decoded, "拒绝发生在算子owner分配前")
				}
			}
		}
	}
	// 既有raw/二进制和UCA400不是新协议特性。
	for _, typ := range []planpb.Type{{Id: 61, Charset: 3}, {Id: 64, Charset: 1}, {Id: 61, Charset: 10, CollationVersion: 1}} {
		f, err := planpb.RequiredRemoteExpressionFeatures(typ)
		require.NoError(t, err)
		require.False(t, f.NativePADCollation)
		require.NoError(t, validateRemoteExpressionFeatures(nil, f))
	}
}

func TestNativePADResultBatchProtocolAndCleanup(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	native := types.NewWithCharset(types.T_varchar, 8, 0, 3)
	native.CollationVersion = 1
	for _, rows := range []int{0, 1, 2} {
		source := batch.NewWithSize(2)
		source.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		source.Vecs[1] = vector.NewVec(native)
		for i := range rows {
			require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte("raw"), false, mp))
			require.NoError(t, vector.AppendBytes(source.Vecs[1], []byte("A"), i == 0, mp))
		}
		source.SetRowCount(rows)
		data, err := source.MarshalBinaryForPipeline(&bytes.Buffer{}, true, true)
		require.NoError(t, err)
		before := mp.CurrNB()
		for _, version := range []any{nil, "119", int64(118), int64(119), int64(118)} {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			decoded, err := decodeBatch(mp, data, c.proc)
			if version == int64(119) {
				require.NoError(t, err)
				require.Equal(t, native, *decoded.Vecs[1].GetType())
				decoded.Clean(mp)
			} else {
				require.ErrorContains(t, err, "version 119")
				require.Nil(t, decoded)
			}
			require.Equal(t, before, mp.CurrNB(), "所有列必须在拒绝后释放，包括空/all-NULL批次")
		}
		source.Clean(mp)
	}
	// codec合法性与执行能力仍分离：119也不能接受已知但禁用的0900域。
	native.Charset = 4
	source := batch.NewWithSize(1)
	source.Vecs[0] = vector.NewVec(native)
	data, err := source.MarshalBinaryForPipeline(&bytes.Buffer{}, true, true)
	require.NoError(t, err)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(119))
	decoded, err := decodeBatch(mp, data, c.proc)
	require.ErrorContains(t, err, "disabled")
	require.Nil(t, decoded)
	source.Clean(mp)
}

func TestNativePADResultSendRejectsBeforePublication(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime("")
	old, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, old) })
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	typ := types.NewWithCharset(types.T_varchar, 8, 0, 2)
	typ.CollationVersion = 1
	source := batch.NewWithSize(1)
	defer source.Clean(mp)
	source.Vecs[0] = vector.NewVec(typ)
	// 空批次仍携带新Type，不能绕过发送门。
	session := mock_morpc.NewMockClientSession(gomock.NewController(t))
	receiver := &messageReceiverOnServer{messageCtx: context.Background(), connectionCtx: context.Background(),
		clientSession: session, messageAcquirer: func() morpc.Message { return &pipeline.Message{} }, maxMessageSize: 1 << 20}
	for _, version := range []any{nil, "119", int64(118)} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		require.ErrorContains(t, receiver.sendBatch(source), "version 119")
	}
	session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, message any) error {
		decoded, err := decodeBatch(mp, message.(*pipeline.Message).Data, c.proc)
		require.NoError(t, err)
		decoded.Clean(mp)
		return nil
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(119))
	moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(moruntime.MOProtocolVersion, int64(119))
	require.NoError(t, receiver.sendBatch(source))
	source.Vecs[0].GetType().Charset = 4
	require.ErrorContains(t, receiver.sendBatch(source), "disabled", "高版本也不能发布未准入域")
}

func TestNativePADWireRejectsInvalidUTF8(t *testing.T) {
	for _, charset := range []uint8{2, 3} {
		for _, revision := range []uint8{0, 1} {
			for _, boundary := range []string{"literal", "batch", "send"} {
				t.Run(fmt.Sprintf("charset_%d/revision_%d/%s", charset, revision, boundary), func(t *testing.T) {
					c, _ := expressionProtocolTestCompile(t)
					rt := moruntime.ServiceRuntime("")
					old, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion119)
					t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, old) })
					mp := mpool.MustNewZero()
					defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
					typ := types.NewWithCharset(types.T_varchar, 8, 0, charset)
					source := batch.NewWithSize(1)
					defer source.Clean(mp)
					source.Vecs[0] = vector.NewVec(typ)
					require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte{0xff}, false, mp))
					typ.CollationVersion = revision
					source.Vecs[0].SetType(typ)
					source.SetRowCount(1)
					before := mp.CurrNB()
					var err error
					switch boundary {
					case "literal":
						data, encodeErr := source.Vecs[0].MarshalBinary()
						require.NoError(t, encodeErr, "generic codec保留合法metadata")
						var executor colexec.ExpressionExecutor
						executor, err = colexec.NewExpressionExecutor(c.proc, &planpb.Expr{Typ: typ.PlanType(), Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Data: data}}})
						if executor != nil {
							executor.Free()
						}
					case "batch":
						data, encodeErr := source.MarshalBinaryForPipeline(&bytes.Buffer{}, true, true)
						require.NoError(t, encodeErr)
						var decoded *batch.Batch
						decoded, err = decodeBatch(mp, data, c.proc)
						if decoded != nil {
							decoded.Clean(mp)
						}
					case "send":
						session := mock_morpc.NewMockClientSession(gomock.NewController(t))
						if revision == 0 {
							session.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil)
						}
						receiver := &messageReceiverOnServer{messageCtx: t.Context(), connectionCtx: t.Context(), clientSession: session,
							messageAcquirer: func() morpc.Message { return &pipeline.Message{} }, maxMessageSize: 1 << 20}
						err = receiver.sendBatch(source)
					}
					if revision == 1 {
						require.ErrorContains(t, err, "UTF-8")
					} else {
						require.NoError(t, err, "legacy原始bytes不改变")
					}
					require.Equal(t, before, mp.CurrNB())
				})
			}
		}
	}
}

func TestLiteralVectorCannotHideNativePADDomain(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	native := types.NewWithCharset(types.T_varchar, 8, 0, 3)
	native.CollationVersion = 1
	legacy := native
	legacy.CollationVersion = 0
	bin := native
	bin.Charset = 2
	before := c.proc.Mp().CurrNB()
	for _, pair := range [][2]types.Type{{legacy, native}, {native, legacy}, {native, bin}, {native, native}} {
		for _, rows := range []int{0, 1} {
			payload := vector.NewVec(pair[1])
			if rows != 0 {
				require.NoError(t, vector.AppendBytes(payload, nil, true, c.proc.Mp()))
			}
			data, err := payload.MarshalBinary()
			require.NoError(t, err)
			executor, err := colexec.NewExpressionExecutor(c.proc, &planpb.Expr{Typ: pair[0].PlanType(), Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Data: data}}})
			if pair[0] == pair[1] {
				require.NoError(t, err)
				executor.Free()
			} else {
				require.ErrorContains(t, err, "collation differs")
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				require.Nil(t, executor)
			}
			payload.Free(c.proc.Mp())
			require.Equal(t, before, c.proc.Mp().CurrNB())
		}
	}
}
