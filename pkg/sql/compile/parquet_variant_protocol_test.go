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

package compile

import (
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestParquetVariantReaderProtocolPlacementAndWire(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.PARQUET}}
	createSQL, err := resolvedExternParamJSON(param)
	require.NoError(t, err)
	qry := &planpb.Query{Nodes: []*planpb.Node{{NodeType: planpb.Node_EXTERNAL_SCAN,
		ExternScan: &planpb.ExternScan{Type: int32(planpb.ExternType_LOAD)}, TableDef: &planpb.TableDef{Createsql: createSQL}}}}
	op := external.NewArgument().WithEs(&external.ExternalParam{
		ExParamConst: external.ExParamConst{CreateSql: createSQL},
		ExParam:      external.ExParam{Filter: &external.FilterParam{}},
	})
	defer op.Release()
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	client.version = defines.MORPCVersion118
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	require.Equal(t, c.addr, c.cnList[0].Addr)
	data, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "Parquet VARIANT")
	require.Nil(t, data)
	client.version = defines.MORPCVersion117
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	data, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	wire := new(pipeline.Pipeline)
	require.NoError(t, wire.Unmarshal(data))
	// Child scans are covered independently after wire decoding as well.
	parent := &pipeline.Pipeline{Children: []*pipeline.Pipeline{wire}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion118)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, parent), "version 119")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion117)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, parent))
	require.Equal(t, client.calls, client.releases)
}

func TestParquetVariantReaderProtocolControls(t *testing.T) {
	for _, format := range []string{tree.CSV, tree.JSONLINE, tree.ARROW, tree.PARQUET} {
		param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: format}}
		createSQL, err := resolvedExternParamJSON(param)
		require.NoError(t, err)
		needs, err := parquetScanRequiresVariantReader(createSQL)
		require.NoError(t, err)
		require.Equal(t, format == tree.PARQUET, needs)
	}
	for _, sql := range []string{"{", `{"Format":123}`} {
		_, err := parquetScanRequiresVariantReader(sql)
		require.Error(t, err)
	}
	needs, err := parquetPipelineRequiresVariantReader(&pipeline.Pipeline{InstructionList: []*pipeline.Instruction{
		nil, {ExternalScan: &pipeline.ExternalScan{}}, // typed Iceberg task control
	}})
	require.NoError(t, err)
	require.False(t, needs)
	for _, kind := range []planpb.ExternType{planpb.ExternType_ICEBERG_TB, planpb.ExternType_MONGODB_TB,
		planpb.ExternType_DATASTREAM_TB, planpb.ExternType_FOREIGN_TB, planpb.ExternType_KAFKA_TB} {
		needs, err := parquetQueryRequiresVariantReader(&planpb.Query{Nodes: []*planpb.Node{{
			ExternScan: &planpb.ExternScan{Type: int32(kind)},
			TableDef:   &planpb.TableDef{Createsql: "create table service_owned_metadata"},
		}}})
		require.NoError(t, err)
		require.False(t, needs)
	}
}
