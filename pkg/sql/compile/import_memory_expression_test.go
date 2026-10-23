// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/fuzzyfilter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type memoryCSVCompilerContext struct {
	*plan2.MockCompilerContext
	proc  *process.Process
	table *plan.TableDef
}

func (c *memoryCSVCompilerContext) GetProcess() *process.Process { return c.proc }
func (c *memoryCSVCompilerContext) Resolve(db, name string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	if name == "memory_csv" {
		return &plan.ObjectRef{SchemaName: "tpch", ObjName: name, Db: 1, Obj: 99001}, plan2.DeepCopyTableDef(c.table, true), nil
	}
	return c.MockCompilerContext.Resolve(db, name, snapshot)
}

// Reuse the lightweight process fixture. Public SQL supplies the expression;
// constructExternal and compileProjection supply the real physical fusion.
// No file is opened, expression evaluated, or external service started.
func memoryCSVProjection(t *testing.T, selectList string, typ plan.Type) (*Compile, *external.External) {
	t.Helper()
	c := memoryCompileFixture(t, 1)
	c.anal = &AnalyzeModule{}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{ScanType: tree.INFILE, Format: tree.CSV, Filepath: "/tmp/not-opened-memory-expression.csv", FileSize: 64, Tail: &tree.TailParameter{}},
		ExParam:      tree.ExParam{ExternType: int32(plan.ExternType_EXTERNAL_TB)},
	}
	encoded, err := json.Marshal(param)
	require.NoError(t, err)
	compilerContext := &memoryCSVCompilerContext{MockCompilerContext: plan2.NewMockCompilerContext(false), proc: c.proc,
		table: &plan.TableDef{TblId: 99001, Name: "memory_csv", DbName: "tpch", TableType: catalog.SystemExternalRel,
			Createsql: string(encoded), Name2ColIndex: map[string]int32{"v": 0}, Cols: []*plan.ColDef{{Name: "v", Typ: typ}}}}
	compilerContext.SetContext(context.Background())
	statements, err := mysql.Parse(context.Background(), "SELECT "+selectList+" FROM memory_csv", 1)
	require.NoError(t, err)
	pn, err := plan2.BuildPlan(compilerContext, statements[0], false)
	require.NoError(t, err)
	var source, project *plan.Node
	for _, node := range pn.GetQuery().Nodes {
		if node.NodeType == plan.Node_EXTERNAL_SCAN {
			source = node
		}
		if node.NodeType == plan.Node_PROJECT {
			project = node
		}
	}
	require.NotNil(t, source)
	if project == nil {
		project = source
	}
	// A final rename/column projection can sit above the expression projection.
	// Exercise the operator which actually owns evaluation of the selected SQL.
	for _, node := range pn.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			if f := expr.GetF(); f != nil && (f.Func.ObjName == "repeat" || f.Func.ObjName == "serial" || f.Func.ObjName == "+") {
				project = node
			}
		}
	}
	op := constructExternal(source, param, c.proc.Ctx, nil, nil, nil, false)
	t.Cleanup(op.Release)
	c.scopes[0].RootOp = op
	c.compileProjection(project, c.scopes)
	require.NotEmpty(t, op.ProjectList, "the real compiler must fuse the public projection into External")
	return c, op
}

func TestImportMemoryPublicCSVProjectionAdmission(t *testing.T) {
	var expanding []string
	for i := 0; i < 16; i++ {
		expanding = append(expanding, fmt.Sprintf("repeat(v,%d) AS expanded_%d", 65535-i, i))
	}
	for _, test := range []struct {
		name, expression string
		typ              plan.Type
		bounded          bool
	}{
		{name: "unknown-varlen-expansion", expression: strings.Join(expanding, ","), typ: plan.Type{Id: int32(types.T_varchar), Width: 1}},
		{name: "borrowed-column", expression: "v", typ: plan.Type{Id: int32(types.T_varchar), Width: 64}, bounded: true},
		{name: "fixed-width-arithmetic", expression: "v + 1", typ: plan.Type{Id: int32(types.T_int64)}, bounded: true},
		{name: "bounded-serialization", expression: "serial(v,v)", typ: plan.Type{Id: int32(types.T_varchar), Width: 64}, bounded: true},
		{name: "btree-literal-prefix", expression: "serial_full('0',v,v)", typ: plan.Type{Id: int32(types.T_varchar), Width: 64}, bounded: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, op := memoryCSVProjection(t, test.expression, test.typ)
			if !test.bounded {
				require.Len(t, op.ProjectList, 16)
			}
			sentinel := errors.New("eligible statement reached admission")
			m := &compilerMemoryAdmission{failure: sentinel}
			err := c.beginStatementImportMemory(context.Background(), m)
			if test.bounded {
				require.ErrorIs(t, err, sentinel)
				require.Equal(t, 1, m.tries)
			} else {
				require.NoError(t, err)
				require.Zero(t, m.tries, "unproven expansion selects whole-query legacy before any Open/reservation")
			}
			require.Nil(t, c.importMemory)
			require.Nil(t, c.proc.Base.ImportMemory)
			require.Empty(t, c.importMemoryControls)
		})
	}
}

func TestImportMemoryPreparedExecutionsKeepWholeStatementLegacy(t *testing.T) {
	c, _ := memoryCSVProjection(t, "v + 1", plan.Type{Id: int32(types.T_int64)})
	c.SetIsPrepare(true)
	c.anal.qry = &plan.Query{}
	c.counterSet = &perfcounter.CounterSet{}
	c.affectRows = &atomic.Uint64{}
	c.MessageBoard = message.NewMessageBoard()
	for execution := 0; execution < 2; execution++ {
		if execution > 0 {
			c.Reset(c.proc, time.Now(), nil, "")
			require.True(t, c.isPrepare)
		}
		m := &compilerMemoryAdmission{failure: errors.New("prepared execution must not reserve a statement ticket")}
		require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
		require.Zero(t, m.tries)
		require.Nil(t, c.importMemory)
		require.Nil(t, c.proc.Base.ImportMemory)
		c.endImportMemory()
	}
}

func TestImportMemoryUnknownExpressionOwnersKeepWholeStatementLegacy(t *testing.T) {
	_, source := memoryCSVProjection(t, "repeat(v,65535)", plan.Type{Id: int32(types.T_varchar), Width: 1})
	exprs := source.ProjectList
	for name, op := range map[string]vm.Operator{
		"standalone-project":          &projection.Projection{ProjectList: exprs},
		"standalone-filter":           &filter.Filter{FilterExprs: exprs},
		"inline-table-filter":         &table_scan.TableScan{Types: []plan.Type{{Id: int32(types.T_int64)}}, FilterExprs: exprs},
		"inline-table-runtime-filter": &table_scan.TableScan{Types: []plan.Type{{Id: int32(types.T_int64)}}, RuntimeFilterExprs: exprs},
	} {
		t.Run(name, func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			op.GetOperatorBase().AppendChild(c.scopes[0].RootOp)
			c.scopes[0].RootOp = op
			m := &compilerMemoryAdmission{failure: errors.New("must not open")}
			require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
			require.Zero(t, m.tries)
			require.Nil(t, c.importMemory)
		})
	}
}

func TestImportMemoryMixedScanSourcesRequirePhysicalBound(t *testing.T) {
	for _, typ := range []types.T{types.T_int64, types.T_varchar, types.T_text, types.T_json} {
		t.Run(typ.String(), func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			c.scopes = append(c.scopes, &Scope{RootOp: &table_scan.TableScan{Types: []plan.Type{{Id: int32(typ), Width: 64}}}})
			sentinel := errors.New("bounded source reached admission")
			m := &compilerMemoryAdmission{failure: sentinel}
			err := c.beginStatementImportMemory(context.Background(), m)
			if typ == types.T_int64 {
				require.ErrorIs(t, err, sentinel)
				require.Equal(t, 1, m.tries)
			} else {
				require.NoError(t, err)
				require.Zero(t, m.tries)
			}
		})
	}
}

func TestImportMemoryDefaultTokenizerProducerEligibility(t *testing.T) {
	for _, test := range []struct {
		name, params string
		body         types.T
		bounded      bool
	}{
		{"default-text", "", types.T_text, true},
		{"ngram-text", `{"parser":"ngram"}`, types.T_text, true},
		{"external-datalink", "", types.T_datalink, false},
		{"typed-json-content", "", types.T_json, false},
		{"typed-array-content", "", types.T_array_float64, false},
		{"unproven-json", `{"parser":"json"}`, types.T_text, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			source := c.scopes[0].RootOp.(*external.External)
			source.Es.Cols = []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}, {Typ: plan.Type{Id: int32(test.body)}}}
			source.Es.Attrs = []plan.ExternAttr{{ColIndex: 0, ColFieldIndex: 0}, {ColIndex: 1, ColFieldIndex: 1}}
			args := []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
				{Typ: plan.Type{Id: int32(test.body)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
			}
			op := &apply.Apply{TableFunction: &table_function.TableFunction{FuncName: "fulltext_index_tokenize", Params: []byte(test.params), Args: args}, Result: []colexec.ResultPos{{Rel: 1, Pos: 0}, {Rel: 1, Pos: 1}, {Rel: 1, Pos: 2}}}
			op.AppendChild(source)
			c.scopes[0].RootOp = op
			sentinel := errors.New("bounded tokenizer reached admission")
			m := &compilerMemoryAdmission{failure: sentinel}
			err := c.beginStatementImportMemory(context.Background(), m)
			if test.bounded {
				require.ErrorIs(t, err, sentinel)
				require.Equal(t, 1, m.tries)
			} else {
				require.NoError(t, err)
				require.Zero(t, m.tries)
			}
		})
	}
}

func TestImportMemoryProducerManifestRoundTripAndValidation(t *testing.T) {
	c, _ := memoryCSVProjection(t, "serial(v,v)", plan.Type{Id: int32(types.T_varchar), Width: 64})
	manifest, err := c.localMemoryManifest(1024)
	require.NoError(t, err)
	require.Equal(t, query.MemoryAdmissionProtocolVersion, manifest.Version)
	wire, err := EncodeMemoryControlRequest(MemoryControlRequest{Action: MemoryControlOpen, Manifest: &manifest})
	require.NoError(t, err)
	decoded, matched, err := DecodeMemoryControlRequest(wire)
	require.NoError(t, err)
	require.True(t, matched)
	_, local, controlled, err := memoryManifestLayout(manifest, 1024)
	require.NoError(t, err)
	require.True(t, controlled)
	_, remote, controlled, err := memoryManifestLayout(*decoded.Manifest, 1024)
	require.NoError(t, err)
	require.True(t, controlled)
	require.Equal(t, local, remote)
	require.Greater(t, len(local.Inputs), 3, "expression output and intermediate storage needs its own root I promise")
	for _, descs := range [][]MemoryProducerDescriptor{
		{{Kind: "expression", Inputs: []uint32{0}}},
		{{Kind: "unknown"}},
		{{Kind: "scan", Schema: []types.Type{types.T_text.ToType()}}},
		{{Kind: "csv", Schema: []types.Type{types.T_varchar.ToType()}}, {Kind: "spool", Inputs: []uint32{0}, Copies: math.MaxUint32}},
	} {
		_, err := memoryProducerBounds(descs, math.MaxInt64)
		require.Error(t, err)
	}
	manifest.Stages = append(manifest.Stages, MemoryStageDescriptor{Role: MemoryStageProducer, Count: 1, Producer: math.MaxUint32})
	_, _, _, err = memoryManifestLayout(manifest, 1024)
	require.ErrorContains(t, err, "invalid producer stage")
}

func TestImportMemoryOldBoolPeerAndManifestAreRejectedBeforeOpen(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	for _, version := range []uint32{0, 1, 2, query.MemoryAdmissionProtocolVersion, 4} {
		client := &memoryCapabilityClient{send: func(addr string, _ *query.Request) (*query.Response, error) {
			return &query.Response{GetProtocolVersion: &query.GetProtocolVersionResponse{MemoryAdmission: true, MemoryAdmissionProcessId: addr, MemoryAdmissionVersion: version}}, nil
		}}
		c.proc.Base.QueryClient = client
		capable, err := c.memoryCapabilities(context.Background(), []*memoryCNManifest{{queryAddr: "A"}, {queryAddr: "B"}})
		require.NoError(t, err)
		require.Equal(t, version == query.MemoryAdmissionProtocolVersion, capable)
		require.Equal(t, 2, client.releases)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	session := &memoryControlSession{ctx: ctx, replies: make(chan *pipeline.Message, 1)}
	wire, err := EncodeMemoryControlRequest(MemoryControlRequest{Action: MemoryControlOpen, Manifest: &MemoryManifest{Version: 0}})
	require.NoError(t, err)
	handled, err := handleMemoryControl(ctx, "no-runtime-for-old-manifest", &pipeline.Message{Id: 19, Cmd: pipeline.Method_StopSending, Data: wire}, session, func() morpc.Message { return &pipeline.Message{} })
	require.True(t, handled)
	require.NoError(t, err)
	require.Contains(t, receiveMemoryControl(t, session).Error, "manifest version")
}

func TestImportMemoryUnprovenEdgesAndRetainingOperatorsStayLegacy(t *testing.T) {
	for name, configure := range map[string]func(*Compile){
		"remote-dispatch": func(c *Compile) {
			op := &dispatch.Dispatch{RemoteRegs: []colexec.ReceiveInfo{{}}}
			op.AppendChild(c.scopes[0].RootOp)
			c.scopes[0].RootOp = op
		},
		"fuzzy-retention": func(c *Compile) {
			op := &fuzzyfilter.FuzzyFilter{}
			op.AppendChild(c.scopes[0].RootOp)
			c.scopes[0].RootOp = op
		},
		"blocked-output": func(c *Compile) { c.needBlock = true },
		"deferred-runtime-filter": func(c *Compile) {
			c.scopes[0].DataSource = &Source{RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{{Tag: 1}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			configure(c)
			m := &compilerMemoryAdmission{failure: errors.New("must not open")}
			require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
			require.Zero(t, m.tries)
			require.Nil(t, c.importMemory)
		})
	}
}

func TestImportMemoryMultiCSVJoinKeepsBuildSchemaSeparate(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	probe := c.scopes[0].RootOp
	buildScope := memorySourceScope(c.addr, 1, 1)
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	build := &hashbuild.HashBuild{JoinMapTag: 17, Conditions: []*plan.Expr{col}}
	build.AppendChild(buildScope.RootOp)
	buildScope.RootOp = build
	join := &hashjoin.HashJoin{JoinMapTag: 17, LeftTypes: []types.Type{types.T_int64.ToType()}, RightTypes: []types.Type{types.T_int64.ToType()}, EqConds: [][]*plan.Expr{{col}, {col}}, ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}}}
	join.AppendChild(probe)
	c.scopes[0].RootOp = join
	c.scopes[0].PreScopes = []*Scope{buildScope}
	manifest, err := c.localMemoryManifest(1024)
	require.NoError(t, err)
	_, _, supported, err := c.memoryProducers()
	require.NoError(t, err)
	require.True(t, supported)
	bounds, err := memoryProducerBounds(manifest.Producers, 1024)
	require.NoError(t, err)
	found := false
	for i, producer := range manifest.Producers {
		if producer.Kind == "empty" {
			require.Empty(t, bounds[i].Columns)
			require.Zero(t, bounds[i].Batch)
		}
		if producer.Kind == "join" {
			require.Len(t, bounds[i].Columns, 2)
			found = true
		}
	}
	require.True(t, found)
	sentinel := errors.New("pure CSV join reached admission")
	m := &compilerMemoryAdmission{failure: sentinel}
	require.ErrorIs(t, c.beginStatementImportMemory(context.Background(), m), sentinel)
	require.Equal(t, 1, m.tries)
}

func TestImportMemoryMergeBlockUsesFinalOutputFramePromise(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	table := &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}}}}
	writer := &insert.Insert{ToWriteS3: true, InsertCtx: &insert.InsertCtx{TableDef: table}}
	writer.AppendChild(c.scopes[0].RootOp)
	spool := &connector.Connector{Reg: process.NewPipelineEdge(1, 1)}
	spool.AppendChild(writer)
	consumer := &mergeblock.MergeBlock{}
	consumer.AppendChild(spool)
	c.scopes[0].RootOp = consumer
	manifest, err := c.localMemoryManifest(1024)
	require.NoError(t, err)
	meta, spec, controlled, err := memoryManifestLayout(manifest, 1024)
	require.NoError(t, err)
	require.True(t, controlled)
	frame := meta.OutputBytes / 4
	require.Greater(t, frame, int64(1024), "metadata capability is derived from final CN output, not CSV unit")
	expected, ok := mergeblock.MetadataOutputBound(frame)
	require.True(t, ok)
	found := false
	for _, stage := range manifest.Stages {
		if stage.Role != MemoryStageProducer || manifest.Producers[stage.Producer].Kind != "merge-meta" {
			continue
		}
		for _, slot := range spec.Inputs {
			if slot.Name == fmt.Sprintf("producer-%d", stage.Producer) {
				require.Equal(t, expected.ScratchBytes, slot.Bytes)
				found = true
			}
		}
	}
	require.True(t, found)
	// A MergeBlock attached to raw CSV has neither the modern metadata schema
	// nor the ObjectStats guarantee. It keeps the whole statement legacy.
	c.scopes[0].RootOp = &mergeblock.MergeBlock{}
	c.scopes[0].RootOp.AppendChild(memorySourceScope(c.addr, 1, 1).RootOp)
	m := &compilerMemoryAdmission{failure: errors.New("raw metadata must not admit")}
	require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
	require.Zero(t, m.tries)
}

func TestImportMemoryFlushS3InfoUsesModernFrameProvenance(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	ctxs := []*multi_update.MultiUpdateCtx{{TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}}}}}}
	writer := &multi_update.MultiUpdate{Action: multi_update.UpdateWriteS3, MultiUpdateCtx: ctxs}
	writer.AppendChild(c.scopes[0].RootOp)
	spool := &connector.Connector{Reg: process.NewPipelineEdge(1, 1)}
	spool.AppendChild(writer)
	flush := &multi_update.MultiUpdate{Action: multi_update.UpdateFlushS3Info, MultiUpdateCtx: ctxs}
	flush.AppendChild(spool)
	c.scopes[0].RootOp = flush
	manifest, err := c.localMemoryManifest(1024)
	require.NoError(t, err)
	meta, spec, controlled, err := memoryManifestLayout(manifest, 1024)
	require.NoError(t, err)
	require.True(t, controlled)
	expected, ok := multi_update.MetadataOutputBound(meta.OutputBytes/4, 1, 1)
	require.True(t, ok)
	found := false
	for _, stage := range manifest.Stages {
		if stage.Role == MemoryStageProducer && manifest.Producers[stage.Producer].Kind == "flush-meta" {
			for _, slot := range spec.Inputs {
				if slot.Name == fmt.Sprintf("producer-%d", stage.Producer) {
					require.Equal(t, expected.ScratchBytes, slot.Bytes)
					found = true
				}
			}
		}
	}
	require.True(t, found)
	flush.Children = []vm.Operator{memorySourceScope(c.addr, 1, 1).RootOp}
	m := &compilerMemoryAdmission{failure: errors.New("raw flush must stay legacy")}
	require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
	require.Zero(t, m.tries)
}

func TestImportMemoryZeroWidthProducersPreserveLogicalRows(t *testing.T) {
	literal := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}
	encoded, err := encodeMemoryExpressions([]*plan.Expr{literal})
	require.NoError(t, err)
	for _, source := range []MemoryProducerDescriptor{{Kind: "csv"}, {Kind: "csv", Schema: []types.Type{types.T_int64.ToType()}}} {
		descs := []MemoryProducerDescriptor{source, {Kind: "expression", Inputs: []uint32{0}, Expressions: encoded, Project: true}}
		bounds, err := memoryProducerBounds(descs, 1)
		require.NoError(t, err)
		if len(source.Schema) == 0 {
			require.Equal(t, 8192, bounds[1].Rows)
			require.GreaterOrEqual(t, bounds[1].Batch, int64(8192*8))
		} else {
			require.Equal(t, 1, bounds[1].Rows)
		}
	}
	join := []MemoryProducerDescriptor{{Kind: "csv", Schema: []types.Type{types.T_int64.ToType()}}, {Kind: "csv", Schema: []types.Type{types.T_int64.ToType()}}, {Kind: "join", Inputs: []uint32{0, 1}}, {Kind: "expression", Inputs: []uint32{2}, Expressions: encoded, Project: true}}
	bounds, err := memoryProducerBounds(join, 1)
	require.NoError(t, err)
	require.Equal(t, 8192, bounds[3].Rows)
	args, err := encodeMemoryExpressions([]*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
		{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
	})
	require.NoError(t, err)
	applyDescs := []MemoryProducerDescriptor{{Kind: "csv", Schema: []types.Type{types.T_int64.ToType(), types.T_text.ToType()}}, {Kind: "apply", Inputs: []uint32{0}, Function: &MemoryFunctionDescriptor{Name: "fulltext_index_tokenize", Args: args}}, {Kind: "expression", Inputs: []uint32{1}, Expressions: encoded, Project: true}}
	bounds, err = memoryProducerBounds(applyDescs, 1)
	require.NoError(t, err)
	require.Equal(t, 16383, bounds[2].Rows)
}

func TestImportMemoryFixedScanDefaultsReservePreInsertFlatten(t *testing.T) {
	bounds, err := memoryProducerBounds([]MemoryProducerDescriptor{{Kind: "scan", Schema: []types.Type{types.T_int64.ToType()}}, {Kind: "preinsert", Inputs: []uint32{0}}}, 1024)
	require.NoError(t, err)
	require.True(t, bounds[0].Columns[0].Constant, "reader defaults are allowed to remain constant")
	require.False(t, bounds[1].Columns[0].Constant)
	require.Greater(t, bounds[1].Scratch, int64(8192*8), "private flattened copy must coexist with borrowed source and its old generation")
}

func TestImportMemoryExpressionProducerValidatesNestedColumnIndexes(t *testing.T) {
	bad := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "+"}, Args: []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 7}}},
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
	}}}}
	encoded, err := encodeMemoryExpressions([]*plan.Expr{bad})
	require.NoError(t, err)
	_, err = memoryProducerBounds([]MemoryProducerDescriptor{{Kind: "scan", Schema: []types.Type{types.T_int64.ToType()}}, {Kind: "expression", Inputs: []uint32{0}, Expressions: encoded, Project: true}}, 1024)
	require.Error(t, err)
	bad.GetF().Args[0].GetCol().ColPos = 0
	bad.GetF().Args[0].GetCol().RelPos = 17
	encoded, err = encodeMemoryExpressions([]*plan.Expr{bad})
	require.NoError(t, err)
	_, err = memoryProducerBounds([]MemoryProducerDescriptor{{Kind: "scan", Schema: []types.Type{types.T_int64.ToType()}}, {Kind: "expression", Inputs: []uint32{0}, Expressions: encoded, Project: true}}, 1024)
	require.NoError(t, err, "the real single-batch evaluator normalizes nonzero relation tags")
}

func TestImportMemoryFollowsLocalSinkRegistersAcrossSteps(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	source := c.scopes[0]
	consumer := &Scope{Proc: c.proc.NewNoContextChildProc(1)}
	reg := consumer.Proc.Reg.MergeReceivers[0]
	sender := &dispatch.Dispatch{LocalRegs: []*process.WaitRegister{reg}}
	sender.AppendChild(source.RootOp)
	source.RootOp = sender
	read := merge.NewArgument().WithSinkScan(true)
	t.Cleanup(read.Release)
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	project := &projection.Projection{ProjectList: []*plan.Expr{col}}
	project.AppendChild(read)
	consumer.RootOp = project
	// Deliberately reverse the steps and provide no PreScopes edge. This is
	// compileSinkScanNode's real local-channel topology, not a scope tree.
	c.scopes = []*Scope{consumer, source}
	manifest, err := c.localMemoryManifest(1024)
	require.NoError(t, err)
	bounds, err := memoryProducerBounds(manifest.Producers, 1024)
	require.NoError(t, err)
	_, ids, ok, err := c.memoryProducers()
	require.NoError(t, err)
	require.True(t, ok)
	got := bounds[ids[project]]
	require.Len(t, got.Columns, 1)
	require.Equal(t, types.T_int64, got.Columns[0].Type.Oid)
	require.Equal(t, 128, got.Rows)
	for i, desc := range manifest.Producers {
		for _, id := range desc.Inputs {
			require.Less(t, int(id), i)
		}
	}
}

func TestImportMemoryJoinProjectionPreservesSourceRowCorrelation(t *testing.T) {
	schema := make([]types.Type, 10)
	selected := make([]colexec.ResultPos, 0, 2*len(schema))
	for i := range schema {
		schema[i] = types.T_varchar.ToType()
		selected = append(selected, colexec.ResultPos{Rel: 0, Pos: int32(i)}, colexec.ResultPos{Rel: 1, Pos: int32(i)})
	}
	descs := []MemoryProducerDescriptor{{Kind: "csv", Schema: schema}, {Kind: "csv", Schema: schema}, {Kind: "join", Inputs: []uint32{0, 1}, ResultCols: selected}}
	bounds, err := memoryProducerBounds(descs, 1024)
	require.NoError(t, err)
	require.Equal(t, bounds[0].Row+bounds[1].Row, bounds[2].Row, "distinct columns share each source record's aggregate bound")
	descs[2].ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 0, Pos: 0}}
	bounds, err = memoryProducerBounds(descs, 1024)
	require.NoError(t, err)
	require.Greater(t, bounds[2].Row, bounds[0].Row, "repeated columns still count their actual copy amplification")
	descs[2].ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 99}}
	_, err = memoryProducerBounds(descs, 1024)
	require.Error(t, err)
}

func TestImportMemorySpoolPromiseIncludesQuadraticQueueMetadata(t *testing.T) {
	const receivers uint32 = 16385
	descs := []MemoryProducerDescriptor{{Kind: "csv", Schema: []types.Type{types.T_int8.ToType()}}, {Kind: "spool", Inputs: []uint32{0}, Copies: receivers, Receivers: receivers}}
	bounds, err := memoryProducerBounds(descs, 1)
	require.NoError(t, err)
	queueBytes := int64(receivers) * 32768 * 4
	require.GreaterOrEqual(t, bounds[1].Scratch, queueBytes, "all receiver queues coexist with active clones/cache")
}

func TestImportMemoryApplyProjectionPreservesRuntimeBindingTags(t *testing.T) {
	args, err := encodeMemoryExpressions([]*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
		{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
	})
	require.NoError(t, err)
	descs := []MemoryProducerDescriptor{
		{Kind: "csv", Schema: []types.Type{types.T_int64.ToType(), types.T_text.ToType()}},
		{Kind: "apply", Inputs: []uint32{0}, Function: &MemoryFunctionDescriptor{Name: "fulltext_index_tokenize", Args: args}, ResultCols: []colexec.ResultPos{{Rel: 1, Pos: 0}, {Rel: 1, Pos: 1}, {Rel: 1, Pos: 2}}},
	}
	normalized, err := memoryProducerBounds(descs, 1024)
	require.NoError(t, err)
	for _, tag := range []int32{11, 13, -1} {
		for i := range descs[1].ResultCols {
			descs[1].ResultCols[i].Rel = tag
		}
		tagged, err := memoryProducerBounds(descs, 1024)
		require.NoError(t, err, "the actual Apply treats every nonzero tag as its function result")
		require.Equal(t, normalized[1], tagged[1])
	}
	descs[1].ResultCols[0].Pos = 3
	_, err = memoryProducerBounds(descs, 1024)
	require.Error(t, err, "the selected function column still must exist")
}

func TestImportMemoryLockProducerChecksEnabledRefreshColumn(t *testing.T) {
	descs := []MemoryProducerDescriptor{
		{Kind: "scan", Schema: []types.Type{types.T_int64.ToType(), types.T_TS.ToType()}},
		{Kind: "lock", Inputs: []uint32{0}, LockTargets: []lockop.MemoryTarget{{PrimaryColumn: 0, RefreshColumn: 1, Type: types.T_int64.ToType()}}},
	}
	_, err := memoryProducerBounds(descs, 1024)
	require.NoError(t, err)
	descs[1].LockTargets[0].RefreshColumn = 0
	_, err = memoryProducerBounds(descs, 1024)
	require.Error(t, err, "an enabled timestamp append cannot target a BIGINT vector")
	descs[1].LockTargets[0].RefreshColumn = 2
	_, err = memoryProducerBounds(descs, 1024)
	require.Error(t, err, "an enabled timestamp vector must exist")
	descs[1].LockTargets[0].RefreshColumn = -1
	_, err = memoryProducerBounds(descs, 1024)
	require.NoError(t, err)
}

func TestImportMemoryExpressionUsesLogicalReferencedColumns(t *testing.T) {
	key := types.T_varchar.ToType()
	input := memoryProducerBound{Rows: 32, Row: (1 << 20) + 64, Batch: (32 << 20) + 64*32,
		Columns: []table_function.MemoryColumnBound{{Type: key, RowBytes: 64, BatchBytes: 64 * 32}, {Type: types.T_json.ToType(), RowBytes: 1 << 20, BatchBytes: 32 << 20}}}
	expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "serial_full"}, Args: []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}}}}}
	encoded, err := encodeMemoryExpressions([]*plan.Expr{expr})
	require.NoError(t, err)
	bound, ok := expressionProducerBound(input, encoded, true)
	require.True(t, ok)
	require.Less(t, bound.Row, int64(512), "serial reads the bounded key; JSON and allocation-class rounding are not logical result payload")
	require.Greater(t, bound.Scratch, bound.Batch, "actual old/new vector and Packer capacity remains separately promised")
	input.Row += 10 << 20
	input.Batch += 320 << 20
	input.Columns[1].RowBytes += 10 << 20
	input.Columns[1].BatchBytes += 320 << 20
	unchanged, ok := expressionProducerBound(input, encoded, true)
	require.True(t, ok)
	require.Equal(t, bound, unchanged, "an unreferenced JSON column cannot grow serial output or scratch")
	literal := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}
	encoded, err = encodeMemoryExpressions([]*plan.Expr{literal})
	require.NoError(t, err)
	fixed, ok := expressionProducerBound(input, encoded, true)
	require.True(t, ok)
	require.Equal(t, int64(8), fixed.Row)
	require.Equal(t, int64(8*32), fixed.Batch)
}

func TestImportMemoryCSVColumnBatchBoundExcludesWideSiblings(t *testing.T) {
	const unit int64 = 32 << 10
	schema := []types.Type{types.T_varchar.ToType(), types.T_json.ToType(), types.T_array_float64.ToType()}
	bounds, err := memoryProducerBounds([]MemoryProducerDescriptor{{Kind: "csv", Schema: schema}}, unit)
	require.NoError(t, err)
	for _, col := range bounds[0].Columns {
		require.LessOrEqual(t, col.BatchBytes, max(unit, col.RowBytes))
	}
	require.Less(t, bounds[0].Columns[0].BatchBytes, bounds[0].Batch, "a JSON singleton does not multiply the byte-string column's batch bound")
}

func TestImportMemoryInsertMetadataKeepsItsOwnProtocolLimit(t *testing.T) {
	const unit int64 = 1024
	for _, modern := range []bool{false, true} {
		t.Run(fmt.Sprint(modern), func(t *testing.T) {
			writerKind, consumerKind := "insert-meta", "merge-meta"
			if modern {
				writerKind, consumerKind = "multi-meta", "flush-meta"
			}
			descs := []MemoryProducerDescriptor{
				{Kind: "csv", Schema: []types.Type{types.T_int64.ToType()}},
				{Kind: writerKind, Inputs: []uint32{0}, InsertOnly: true, TableCount: 1, ColumnCount: 1},
				{Kind: "spool", Inputs: []uint32{1}, Copies: 2, Receivers: 1},
				{Kind: consumerKind, Inputs: []uint32{2}, InsertOnly: true, TableCount: 1, ColumnCount: 1},
			}
			frame, err := colexec.InsertMetadataFrameLimit(unit, 1)
			require.NoError(t, err)
			bounds, err := memoryProducerBoundsWithFrame(descs, unit, 64<<20)
			require.NoError(t, err)
			require.Equal(t, frame, bounds[1].Batch)
			require.Equal(t, frame, bounds[2].Batch)
			dedicatedScratch := bounds[3].Scratch
			larger, err := memoryProducerBoundsWithFrame(descs, unit, 256<<20)
			require.NoError(t, err)
			require.Equal(t, bounds, larger, "unrelated raw join/delete output cannot enlarge INSERT metadata or its consumers")
			descs[1].InsertOnly = false // Old manifests omit the new positive capability.
			legacy, err := memoryProducerBoundsWithFrame(descs, unit, 64<<20)
			require.NoError(t, err)
			require.Equal(t, int64(64<<20), legacy[1].Batch)
			require.Equal(t, int64(64<<20), legacy[2].Batch)
			if !modern {
				require.Greater(t, legacy[3].Scratch, dedicatedScratch)
			}
		})
	}
}

func TestImportMemoryCompactCSVColumnBoundCoversInlineCopyAndSerial(t *testing.T) {
	oldLimit := config.LargestEntryLimit
	config.LargestEntryLimit = 4
	defer func() { config.LargestEntryLimit = oldLimit }()
	const unit int64 = 1024
	bounds, err := memoryProducerBounds([]MemoryProducerDescriptor{{Kind: "csv", Schema: []types.Type{types.T_varchar.ToType()}}}, unit)
	require.NoError(t, err)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	in := batch.NewWithSize(1)
	in.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { in.Clean(proc.Mp()) })
	for i := 0; i < 42; i++ {
		require.NoError(t, vector.AppendBytes(in.Vecs[0], []byte("a"), false, proc.Mp()))
	}
	in.SetRowCount(42)
	compact := in.Vecs[0].WindowSize(0, 42)
	require.Equal(t, int64(42*types.VarlenaSize), compact)
	require.Greater(t, compact+42, unit, "adding inline values again would use a different, inflated logical namespace")
	require.LessOrEqual(t, compact, bounds[0].Columns[0].BatchBytes)
	duplicate, err := in.Vecs[0].Dup(proc.Mp())
	require.NoError(t, err)
	t.Cleanup(func() { duplicate.Free(proc.Mp()) })
	require.LessOrEqual(t, duplicate.WindowSize(0, 42), bounds[0].Columns[0].BatchBytes)
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "serial_full", []*plan.Expr{col})
	require.NoError(t, err)
	promise, ok := hashbuild.SpillKeyMemoryBoundForColumns([]*plan.Expr{expr}, bounds[0].Batch, 42, []int64{bounds[0].Columns[0].BatchBytes})
	require.True(t, ok)
	eval, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	t.Cleanup(eval.Free)
	out, err := eval.Eval(proc, []*batch.Batch{in}, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, out.WindowSize(0, 42), promise.LogicalResultBytes)
	var values int64
	for i := 0; i < 42; i++ {
		values += int64(len(in.Vecs[0].GetBytesAt(i)))
		require.Equal(t, []byte("a"), duplicate.GetBytesAt(i))
	}
	require.LessOrEqual(t, values, bounds[0].Columns[0].BatchBytes, "compact header storage also bounds every inline value consumed by the encoder")
}

func TestImportMemorySyntheticPathDeclinesBeforeOpen(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	source := c.scopes[0].RootOp.(*external.External)
	source.Es.Attrs[0].ColName = catalog.ExternalFilePath
	source.Es.Cols[0].Typ = plan.Type{Id: int32(types.T_varchar)}
	source.Es.Extern.ExternType = int32(plan.ExternType_EXTERNAL_TB)
	source.Es.Extern.Filepath = "/tmp/" + strings.Repeat("path", 128)
	m := &compilerMemoryAdmission{failure: errors.New("unproven path must not reach Open")}
	require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
	require.Zero(t, m.tries)
	require.Empty(t, c.importMemoryControls)
	require.Nil(t, c.importMemory)
	source.Es.Attrs[0].ColName = "id"
	source.Es.Cols[0].Typ = plan.Type{Id: int32(types.T_int64)}
	source.Es.Extern.ExternType = int32(plan.ExternType_LOAD)
	sentinel := errors.New("ordinary LOAD retains memory admission")
	m = &compilerMemoryAdmission{failure: sentinel}
	require.ErrorIs(t, c.beginStatementImportMemory(context.Background(), m), sentinel)
	require.Equal(t, 1, m.tries)
}

func TestImportMemoryUsesPhysicalParallelInputSchema(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	source := c.scopes[0].RootOp.(*external.External)
	source.Es.ParallelLoad = true
	manifest, err := c.localMemoryManifest(1024)
	require.NoError(t, err)
	var csvStage *MemoryStageDescriptor
	for i := range manifest.Stages {
		if manifest.Stages[i].Role == MemoryStageCSV {
			csvStage = &manifest.Stages[i]
		}
	}
	require.NotNil(t, csvStage)
	require.Equal(t, types.T_varchar, csvStage.Schema[0].Oid)
	found := false
	for _, desc := range manifest.Producers {
		if desc.Kind == "csv" {
			require.Equal(t, csvStage.Schema, desc.Schema)
			found = true
		}
	}
	require.True(t, found)
	meta, _, controlled, err := memoryManifestLayout(manifest, 1024)
	require.NoError(t, err)
	require.True(t, controlled)
	expected, err := external.SourceEnvelopeForFields(csvStage.Schema, 1024, csvStage.RetainedFields)
	require.NoError(t, err)
	require.Equal(t, expected, meta.SourceBytes, "source I and producer lineage must describe the same physical VARCHAR backing")
}

func TestImportMemoryUnprovenJSONLineDeclinesBeforeOpen(t *testing.T) {
	c, source := memoryCSVProjection(t, "v", plan.Type{Id: int32(types.T_int64)})
	source.Es.Extern.Format = tree.JSONLINE
	source.Es.Extern.JsonData = tree.ARRAY
	m := &compilerMemoryAdmission{failure: errors.New("legal JSONLINE must retain legacy before opening a ticket")}
	require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
	require.Zero(t, m.tries)
	require.Empty(t, c.importMemoryControls)
	require.Nil(t, c.importMemory)
	require.Equal(t, tree.JSONLINE, source.Es.Extern.Format, "eligibility does not rewrite the reader or SQL format")
}
