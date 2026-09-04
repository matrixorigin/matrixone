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
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestArrowExecutionScopeRequiresPositiveCompileEvidence(t *testing.T) {
	node := &plan.Node{ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_LOAD)}}
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.ARROW}}
	newCompile := func(query *plan.Query) *Compile {
		return &Compile{anal: &AnalyzeModule{qry: query}}
	}

	require.Equal(t, pipeline.ArrowExecutionScope_ArrowLoadData,
		newCompile(&plan.Query{LoadTag: true, StmtType: plan.Query_INSERT}).arrowExecutionScope(node, param))

	tests := []struct {
		name  string
		c     *Compile
		node  *plan.Node
		param *tree.ExternParam
	}{
		{"nil-compile", nil, node, param},
		{"nil-analysis", &Compile{}, node, param},
		{"missing-load-tag", newCompile(&plan.Query{StmtType: plan.Query_INSERT}), node, param},
		{"wrong-statement", newCompile(&plan.Query{LoadTag: true, StmtType: plan.Query_SELECT}), node, param},
		{"wrong-scan", newCompile(&plan.Query{LoadTag: true, StmtType: plan.Query_INSERT}), &plan.Node{ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_EXTERNAL_TB)}}, param},
		{"wrong-format", newCompile(&plan.Query{LoadTag: true, StmtType: plan.Query_INSERT}), node, &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.PARQUET}}},
		{"nil-node", newCompile(&plan.Query{LoadTag: true, StmtType: plan.Query_INSERT}), nil, param},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, pipeline.ArrowExecutionScope_UnknownArrowExecutionScope,
				test.c.arrowExecutionScope(test.node, test.param))
		})
	}
}

func TestArrowLoadRolloutGateFailsClosedAndSerializesWhenDistributedOff(t *testing.T) {
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{Format: tree.ARROW, ScanType: tree.INFILE},
		ExParam:      tree.ExParam{Parallel: true},
	}
	compile := &Compile{proc: testutil.NewProc(t)}

	_, err := compile.requireArrowLoadEnabled(param)
	require.ErrorContains(t, err, "configuration is unavailable")

	frontend := &config.FrontendParameters{}
	frontend.SetDefaultValues()
	compile.proc.Ctx = context.WithValue(
		context.Background(), config.ParameterUnitKey,
		config.NewParameterUnit(frontend, nil, nil, nil),
	)
	settings, err := compile.requireArrowLoadEnabled(param)
	require.NoError(t, err)
	require.True(t, settings.Enabled)
	require.True(t, settings.S3Enabled)
	require.True(t, settings.DistributedEnabled)
	require.Same(t, param, arrowParamForRollout(param, settings))

	frontend.ArrowLoad.Enabled = false
	_, err = compile.requireArrowLoadEnabled(param)
	require.ErrorContains(t, err, "disabled by configuration")
	frontend.ArrowLoad.Enabled = true

	frontend.ArrowLoad.DistributedEnabled = false
	settings, err = compile.requireArrowLoadEnabled(param)
	require.NoError(t, err)
	serial := arrowParamForRollout(param, settings)
	require.NotSame(t, param, serial)
	require.True(t, param.Parallel, "rollout fallback must not mutate the reusable parser parameter")
	require.False(t, serial.Parallel)

	s3 := new(tree.ExternParam)
	*s3 = *param
	s3.ScanType = tree.S3
	frontend.ArrowLoad.S3Enabled = false
	_, err = compile.requireArrowLoadEnabled(s3)
	require.ErrorContains(t, err, "S3 or stage")
	dynamicMinIO := new(tree.ExternParam)
	*dynamicMinIO = *param
	dynamicMinIO.Filepath = "minio,localhost:9000,us-east-1,bucket,key,secret,prefix:input.arrow"
	_, err = compile.requireArrowLoadEnabled(dynamicMinIO)
	require.ErrorContains(t, err, "S3 or stage")
	frontend.ArrowLoad.S3Enabled = true
	_, err = compile.requireArrowLoadEnabled(s3)
	require.NoError(t, err)
	_, err = compile.requireArrowLoadEnabled(dynamicMinIO)
	require.NoError(t, err)

	frontend.ArrowLoad.DistributedEnabled = true
	require.Same(t, param, arrowParamForRollout(param, frontend.ArrowLoad))

	frontend.ArrowLoad.ForceMaterialize = true
	materialized := arrowParamForRollout(param, frontend.ArrowLoad)
	require.NotSame(t, param, materialized)
	require.False(t, param.ArrowForceMaterialize,
		"rollout fallback must not mutate the reusable parser parameter")
	require.True(t, materialized.Parallel)
	require.True(t, materialized.ArrowForceMaterialize)
}

func TestArrowCompileRuntimeRemapsFileIndicesPerScope(t *testing.T) {
	runtime := &arrowCompileRuntime{
		identitiesByPath: map[string]*pipeline.ArrowObjectIdentity{
			"a.arrow": {FileIndex: 4, Etag: "a", Size: 10},
			"b.arrow": {FileIndex: 9, VersionId: "b", Size: 20},
		},
		shardsByPath: map[string][]*pipeline.ArrowRecordBatchShard{
			"b.arrow": {{FileIndex: 9, RecordBatchStart: 3, RecordBatchEnd: 4, RequiredDictionaryBlockIndices: []int32{1}}},
		},
		conversionPlanVersion: arrowConversionPlanVersion,
	}
	identities := runtime.identitiesFor([]string{"b.arrow", "a.arrow"})
	require.Equal(t, int32(0), identities[0].FileIndex)
	require.Equal(t, "b", identities[0].VersionId)
	require.Equal(t, int32(1), identities[1].FileIndex)
	require.Equal(t, "a", identities[1].Etag)

	shards := runtime.shardsFor([]string{"b.arrow", "a.arrow"})
	require.Len(t, shards, 1)
	require.Equal(t, int32(0), shards[0].FileIndex)
	shards[0].RequiredDictionaryBlockIndices[0] = 99
	require.Equal(t, int32(1), runtime.shardsByPath["b.arrow"][0].RequiredDictionaryBlockIndices[0],
		"scope metadata must not alias the compile planner owner")
}

func TestBuildArrowExternalAttrsUsesOnlyBinderProvenSourceColumns(t *testing.T) {
	node := &plan.Node{
		ExternScan: &plan.ExternScan{TbColToDataCol: map[string]int32{"payload": 0, "id": 1}},
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{
			{Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "generated", Typ: plan.Type{Id: int32(types.T_int64)}, GeneratedCol: &plan.GeneratedCol{}},
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "hidden", Typ: plan.Type{Id: int32(types.T_int64)}, Hidden: true},
			{Name: "defaulted", Typ: plan.Type{Id: int32(types.T_int64)}},
		}},
	}
	require.Equal(t, []plan.ExternAttr{
		{ColName: "payload", ColIndex: 0, ColFieldIndex: 0},
		{ColName: "id", ColIndex: 2, ColFieldIndex: 1},
	}, buildArrowExternalAttrs(node))
}

func TestPlanArrowCompileRuntimeBuildsFingerprintAndRecordShards(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("arrow-plan", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	payload := makeCompileArrowFile(t, nil, 4)
	path := "arrow-plan:input.arrow"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: path,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(payload)), Data: payload}},
	}))
	node := &plan.Node{
		ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_LOAD), TbColToDataCol: map[string]int32{"id": 0}},
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{{
			Name: "id", Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true},
		}}},
	}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE, Format: tree.ARROW,
			ArrowContainer: tree.ARROW_CONTAINER_FILE,
		},
		ExParam: tree.ExParam{ExternType: int32(plan.ExternType_LOAD), FileService: fs, Parallel: true},
	}
	c := &Compile{proc: testutil.NewProc(t), ncpu: 3, addr: "local-cn"}
	runtime, err := c.planArrowCompileRuntime(node, param, []string{path}, []int64{int64(len(payload))})
	require.NoError(t, err)
	require.Len(t, runtime.schemaFingerprint, 32)
	require.Equal(t, arrowConversionPlanVersion, runtime.conversionPlanVersion)
	require.Len(t, runtime.shardsByPath[path], 3)
	require.Equal(t, int32(0), runtime.shardsByPath[path][0].RecordBatchStart)
	require.Equal(t, int32(4), runtime.shardsByPath[path][2].RecordBatchEnd)
	var rows int64
	for _, shard := range runtime.shardsByPath[path] {
		rows += shard.EstimatedRows
		require.Positive(t, shard.EstimatedWireBytes)
	}
	require.Equal(t, int64(4), rows)
}

func TestPlanArrowCompileRuntimeBalancesSkewedRecordWireBytes(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("arrow-skew", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	payload := makeCompileArrowVarlenFile(t, []int{64 << 10, 1, 1, 1})
	path := "arrow-skew:input.arrow"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: path,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(payload)), Data: payload}},
	}))
	node := &plan.Node{
		ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_LOAD), TbColToDataCol: map[string]int32{"payload": 0}},
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{{
			Name: "payload", Typ: plan.Type{Id: int32(types.T_varbinary)},
		}}},
	}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE, Format: tree.ARROW,
			ArrowContainer: tree.ARROW_CONTAINER_FILE,
		},
		ExParam: tree.ExParam{ExternType: int32(plan.ExternType_LOAD), FileService: fs, Parallel: true},
	}
	c := &Compile{proc: testutil.NewProc(t), ncpu: 2, addr: "local-cn"}
	runtime, err := c.planArrowCompileRuntime(node, param, []string{path}, []int64{int64(len(payload))})
	require.NoError(t, err)
	require.Len(t, runtime.shardsByPath[path], 2)
	require.Equal(t, int32(0), runtime.shardsByPath[path][0].RecordBatchStart)
	require.Equal(t, int32(1), runtime.shardsByPath[path][0].RecordBatchEnd,
		"the dominant record should not be grouped with tiny records")
	require.Equal(t, int32(1), runtime.shardsByPath[path][1].RecordBatchStart)
	require.Equal(t, int32(4), runtime.shardsByPath[path][1].RecordBatchEnd)
}

func TestPlanArrowCompileRuntimeRejectsCrossObjectSchemaDrift(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("arrow-drift", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	baseMetadata := arrow.NewMetadata([]string{"source"}, []string{"first"})
	changedMetadata := arrow.NewMetadata([]string{"source"}, []string{"second"})
	payloads := [][]byte{
		makeCompileArrowFile(t, &baseMetadata, 1),
		makeCompileArrowFile(t, &changedMetadata, 1),
	}
	paths := []string{"arrow-drift:first.arrow", "arrow-drift:second.arrow"}
	sizes := make([]int64, len(paths))
	for index := range paths {
		sizes[index] = int64(len(payloads[index]))
		require.NoError(t, fs.Write(ctx, fileservice.IOVector{
			FilePath: paths[index],
			Entries:  []fileservice.IOEntry{{Offset: 0, Size: sizes[index], Data: payloads[index]}},
		}))
	}
	node := &plan.Node{
		ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_LOAD), TbColToDataCol: map[string]int32{"id": 0}},
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{{
			Name: "id", Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true},
		}}},
	}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE, Format: tree.ARROW, ArrowContainer: tree.ARROW_CONTAINER_FILE,
		},
		ExParam: tree.ExParam{ExternType: int32(plan.ExternType_LOAD), FileService: fs},
	}
	c := &Compile{proc: testutil.NewProc(t), ncpu: 1}
	_, err = c.planArrowCompileRuntime(node, param, paths, sizes)
	require.ErrorContains(t, err, "differs from earlier objects")
}

func TestCompileArrowRecordBatchFanoutPublishesOneShardPerScope(t *testing.T) {
	const path = "arrow-plan:input.arrow"
	node := &plan.Node{
		ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_LOAD), TbColToDataCol: map[string]int32{"id": 0}},
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{{
			Name: "id", Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true},
		}}},
	}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE, Format: tree.ARROW, ArrowContainer: tree.ARROW_CONTAINER_FILE,
		},
		ExParam: tree.ExParam{ExternType: int32(plan.ExternType_LOAD), Parallel: true},
	}
	runtime := &arrowCompileRuntime{
		identitiesByPath: map[string]*pipeline.ArrowObjectIdentity{
			path: {FileIndex: 0, Etag: "etag-v1", Size: 1000},
		},
		shardsByPath: map[string][]*pipeline.ArrowRecordBatchShard{
			path: {
				{FileIndex: 0, RecordBatchStart: 0, RecordBatchEnd: 2, EstimatedRows: 20},
				{FileIndex: 0, RecordBatchStart: 2, RecordBatchEnd: 4, EstimatedRows: 20},
				{FileIndex: 0, RecordBatchStart: 4, RecordBatchEnd: 5, EstimatedRows: 10},
			},
		},
		schemaFingerprint:     bytes.Repeat([]byte{0x5a}, 32),
		conversionPlanVersion: arrowConversionPlanVersion,
	}
	c := NewMockCompile(t)
	c.addr = "local-cn"
	c.ncpu = 3
	c.anal = &AnalyzeModule{isFirst: true, qry: &plan.Query{LoadTag: true, StmtType: plan.Query_INSERT}}

	scopes, err := c.compileExternScanArrowRecordBatchFanout(
		node, param, path, 1000, true, runtime,
	)
	require.NoError(t, err)
	require.Len(t, scopes, 3)
	require.True(t, param.Parallel, "compile must not mutate the reusable parser parameter")
	require.False(t, c.anal.isFirst)
	for index, scope := range scopes {
		require.True(t, scope.IsLoad)
		require.Equal(t, 1, scope.NodeInfo.Mcpu)
		op, ok := scope.RootOp.(*external.External)
		require.True(t, ok)
		require.False(t, op.Es.Extern.Parallel)
		require.Equal(t, []string{path}, op.Es.FileList)
		require.Equal(t, []int64{1000}, op.Es.FileSize)
		require.Equal(t, pipeline.ArrowExecutionScope_ArrowLoadData, op.Es.ArrowExecutionScope)
		require.Equal(t, runtime.schemaFingerprint, op.Es.ArrowSchemaFingerprint)
		require.Equal(t, arrowConversionPlanVersion, op.Es.ArrowConversionPlanVersion)
		require.Len(t, op.Es.ArrowObjectIdentities, 1)
		require.Equal(t, int32(0), op.Es.ArrowObjectIdentities[0].FileIndex)
		require.Len(t, op.Es.ArrowRecordBatchShards, 1)
		got := op.Es.ArrowRecordBatchShards[0]
		want := runtime.shardsByPath[path][index]
		require.Equal(t, int32(0), got.FileIndex)
		require.Equal(t, want.RecordBatchStart, got.RecordBatchStart)
		require.Equal(t, want.RecordBatchEnd, got.RecordBatchEnd)
		require.Equal(t, want.EstimatedRows, got.EstimatedRows)
	}
}

func makeCompileArrowFile(t *testing.T, metadata *arrow.Metadata, recordCount int) []byte {
	t.Helper()
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, metadata)
	var output bytes.Buffer
	writer, err := ipc.NewFileWriter(&output, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
	require.NoError(t, err)
	for index := 0; index < recordCount; index++ {
		builder := array.NewInt64Builder(allocator)
		builder.Append(int64(index))
		values := builder.NewArray()
		record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
		require.NoError(t, writer.Write(record))
		record.Release()
		values.Release()
		builder.Release()
	}
	require.NoError(t, writer.Close())
	return append([]byte(nil), output.Bytes()...)
}

func makeCompileArrowVarlenFile(t *testing.T, sizes []int) []byte {
	t.Helper()
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "payload", Type: arrow.BinaryTypes.Binary}}, nil)
	var output bytes.Buffer
	writer, err := ipc.NewFileWriter(&output, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
	require.NoError(t, err)
	for index, size := range sizes {
		builder := array.NewBinaryBuilder(allocator, arrow.BinaryTypes.Binary)
		builder.Append(bytes.Repeat([]byte{byte(index + 1)}, size))
		values := builder.NewArray()
		record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
		require.NoError(t, writer.Write(record))
		record.Release()
		values.Release()
		builder.Release()
	}
	require.NoError(t, writer.Close())
	return append([]byte(nil), output.Bytes()...)
}
