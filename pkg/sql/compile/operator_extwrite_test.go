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

package compile

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func extWriteCreatesql(t *testing.T, pattern string) string {
	opt := []string{"format", "csv"}
	if pattern != "" {
		opt = append(opt, "write_file_pattern", pattern)
	}
	raw, err := json.Marshal(&tree.ExternParam{ExParamConst: tree.ExParamConst{Option: opt}})
	require.NoError(t, err)
	return string(raw)
}

func TestIsExternalWriteInsert(t *testing.T) {
	// nil InsertCtx
	require.False(t, isExternalWriteInsert(&plan.Node{}))

	// nil TableDef
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{}}))

	// regular (non-external) table
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{TableType: catalog.SystemOrdinaryRel},
	}}))

	// external table but read-only (no write_file_pattern)
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, ""),
		},
	}}))

	// external table with malformed Createsql
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			TableType: catalog.SystemExternalRel,
			Createsql: "{not json",
		},
	}}))

	// writable external table
	require.True(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, "stage://s/p-%U.csv"),
		},
	}}))
}

// TestExternalInsertStmtTime ensures the writer evaluates WRITE_FILE_PATTERN
// against one statement-start timestamp shared by all scopes: the frontend's
// defines.StartTS when present, else the Compile's startAt (set on every
// construction path including the internal SQL executor), else the wall clock.
func TestExternalInsertStmtTime(t *testing.T) {
	want := time.Unix(1718000000, 0).UTC()
	startAt := time.Unix(1718000100, 0).UTC()

	// defines.StartTS wins.
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = context.WithValue(context.Background(), defines.StartTS{}, want)
	require.True(t, externalInsertStmtTime(proc, startAt).Equal(want))

	// No StartTS on the context: the Compile's startAt.
	proc2 := &process.Process{}
	proc2.Base = &process.BaseProcess{}
	proc2.Ctx = context.Background()
	require.True(t, externalInsertStmtTime(proc2, startAt).Equal(startAt))

	// Neither: wall clock.
	before := time.Now()
	got := externalInsertStmtTime(proc2, time.Time{})
	require.False(t, got.Before(before))
	require.False(t, got.After(time.Now()))

	// The resolved timestamp lands in the writer config.
	node := &plan.Node{InsertCtx: &plan.InsertCtx{
		Ref: &plan.ObjectRef{ObjName: "wext"},
		TableDef: &plan.TableDef{
			Name:      "wext",
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, "stage://s/p-%U.csv"),
			Cols:      []*plan.ColDef{{Name: "a"}},
		},
	}}
	op, err := constructExternalInsert(proc, node, nil, want)
	require.NoError(t, err)
	arg := op.(*insert.Insert)
	defer arg.Release()
	require.True(t, arg.InsertCtx.ExternalConfig.Stmt.Equal(want))
}

func TestExternalInsertTargetIsLocalFile(t *testing.T) {
	proc := process.NewTopProcess(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	fileURL, err := url.Parse("file:///tmp/wext")
	require.NoError(t, err)
	s3URL, err := url.Parse("s3://bucket/wext")
	require.NoError(t, err)
	proc.GetStageCache().Set("local_stage", stage.StageDef{Name: "local_stage", Url: fileURL})
	proc.GetStageCache().Set("s3_stage", stage.StageDef{Name: "s3_stage", Url: s3URL})

	node := &plan.Node{InsertCtx: &plan.InsertCtx{TableDef: &plan.TableDef{
		TableType: catalog.SystemExternalRel,
		Createsql: extWriteCreatesql(t, "stage://local_stage/p-%U.csv"),
	}}}
	isLocal, err := externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.NoError(t, err)
	require.True(t, isLocal)

	node.InsertCtx.TableDef.Createsql = extWriteCreatesql(t, "stage://s3_stage/p-%U.csv")
	isLocal, err = externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.NoError(t, err)
	require.False(t, isLocal)

	isLocal, err = externalInsertTargetIsLocalFile(proc, nil, time.Unix(1, 0).UTC())
	require.NoError(t, err)
	require.False(t, isLocal)

	node.InsertCtx.TableDef.Createsql = "{not json"
	isLocal, err = externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.Error(t, err)
	require.False(t, isLocal)

	node.InsertCtx.TableDef.Createsql = extWriteCreatesql(t, "")
	isLocal, err = externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.NoError(t, err)
	require.False(t, isLocal)

	node.InsertCtx.TableDef.Createsql = extWriteCreatesql(t, "stage://local_stage/p-%.csv")
	isLocal, err = externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.Error(t, err)
	require.False(t, isLocal)

	node.InsertCtx.TableDef.Createsql = extWriteCreatesql(t, "file:///tmp/wext/p-%U.csv")
	isLocal, err = externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.Error(t, err)
	require.False(t, isLocal)
}

func TestCompileExternalInsertFileStageMergesToCurrentCN(t *testing.T) {
	proc := process.NewTopProcess(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	fileURL, err := url.Parse("file:///tmp/wext")
	require.NoError(t, err)
	proc.GetStageCache().Set("local_stage", stage.StageDef{Name: "local_stage", Url: fileURL})

	c := NewCompile("127.0.0.1:6001", "db", "insert into wext select * from src", "tenant", "uid", nil, proc, nil, false, nil, time.Unix(1, 0).UTC())
	c.anal = &AnalyzeModule{isFirst: true}
	node := &plan.Node{InsertCtx: &plan.InsertCtx{
		Ref:             &plan.ObjectRef{ObjName: "wext"},
		AddAffectedRows: true,
		TableDef: &plan.TableDef{
			Name:      "wext",
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, "stage://local_stage/p-%U.csv"),
			Cols:      []*plan.ColDef{{Name: "a"}},
		},
	}}
	input := []*Scope{
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.2:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.3:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
	}

	out, err := c.compileInsert(nil, node, input)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "127.0.0.1:6001", out[0].NodeInfo.Addr)
	require.Len(t, out[0].PreScopes, 2)
	require.IsType(t, &insert.Insert{}, out[0].RootOp)
	require.Equal(t, vm.Insert, out[0].RootOp.OpType())

	c.anal = &AnalyzeModule{isFirst: true}
	input = []*Scope{
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.2:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
	}
	out, err = c.compileInsert(nil, node, input)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "127.0.0.1:6001", out[0].NodeInfo.Addr)
	require.Len(t, out[0].PreScopes, 1)
}

func TestCompileExternalInsertReturnsFileStageResolveError(t *testing.T) {
	proc := process.NewTopProcess(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	c := NewCompile("127.0.0.1:6001", "db", "insert into wext select * from src", "tenant", "uid", nil, proc, nil, false, nil, time.Unix(1, 0).UTC())
	c.anal = &AnalyzeModule{isFirst: true}
	node := &plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			Name:      "wext",
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, "file:///tmp/wext/p-%U.csv"),
			Cols:      []*plan.ColDef{{Name: "a"}},
		},
	}}
	input := []*Scope{
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.2:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
	}

	out, err := c.compileInsert(nil, node, input)
	require.Error(t, err)
	require.Nil(t, out)
}

func TestCompileExternalInsertS3StageKeepsRemoteScopes(t *testing.T) {
	proc := process.NewTopProcess(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	s3URL, err := url.Parse("s3://bucket/wext")
	require.NoError(t, err)
	proc.GetStageCache().Set("s3_stage", stage.StageDef{Name: "s3_stage", Url: s3URL})

	c := NewCompile("127.0.0.1:6001", "db", "insert into wext select * from src", "tenant", "uid", nil, proc, nil, false, nil, time.Unix(1, 0).UTC())
	c.anal = &AnalyzeModule{isFirst: true}
	node := &plan.Node{InsertCtx: &plan.InsertCtx{
		Ref:             &plan.ObjectRef{ObjName: "wext"},
		AddAffectedRows: true,
		TableDef: &plan.TableDef{
			Name:      "wext",
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, "stage://s3_stage/p-%U.csv"),
			Cols:      []*plan.ColDef{{Name: "a"}},
		},
	}}
	input := []*Scope{
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.2:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.3:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
	}

	out, err := c.compileInsert(nil, node, input)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, "127.0.0.2:6001", out[0].NodeInfo.Addr)
	require.Equal(t, "127.0.0.3:6001", out[1].NodeInfo.Addr)
	require.Empty(t, out[0].PreScopes)
	require.Empty(t, out[1].PreScopes)
	require.IsType(t, &insert.Insert{}, out[0].RootOp)
	require.Equal(t, vm.Insert, out[0].RootOp.OpType())
	require.IsType(t, &insert.Insert{}, out[1].RootOp)
	require.Equal(t, vm.Insert, out[1].RootOp.OpType())
}

// TestExternalInsertDupOperator ensures parallelizing a scope keeps the
// duplicated insert in external-write mode; losing the flag silently turned
// the parallel instances into engine-relation inserts.
func TestExternalInsertDupOperator(t *testing.T) {
	src := insert.NewArgument()
	defer src.Release()
	src.ToExternal = true
	src.InsertCtx = &insert.InsertCtx{Attrs: []string{"a"}}

	dup := dupOperator(src, 1, 2).(*insert.Insert)
	defer dup.Release()
	require.True(t, dup.ToExternal)
	require.Equal(t, src.InsertCtx, dup.InsertCtx)
}

// TestExternalInsertRemoteRunRoundtrip ensures the external-write insert
// survives pipeline encode/decode: a remote CN must rebuild the operator with
// ToExternal set and the same writer config (pattern, format, statement
// timestamp) instead of a plain engine-relation insert.
func TestExternalInsertRemoteRunRoundtrip(t *testing.T) {
	stmtAt := time.Unix(1718000000, 12345).UTC()
	tableDef := &plan.TableDef{
		Name:      "wext",
		TableType: catalog.SystemExternalRel,
		Createsql: extWriteCreatesql(t, "stage://s/p-%U.csv"),
		Cols: []*plan.ColDef{
			{Name: "a"},
			{Name: catalog.Row_ID, Hidden: true},
		},
	}
	arg, err := buildExternalInsertArg(t.Context(), &plan.ObjectRef{ObjName: "wext"},
		tableDef, true, nil, stmtAt)
	require.NoError(t, err)
	defer arg.Release()
	require.True(t, arg.ToExternal)
	require.Equal(t, []string{"a"}, arg.InsertCtx.Attrs)

	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = context.Background()
	proc.Base.SessionInfo.TimeZone = time.UTC

	_, pipeInstr, err := convertToPipelineInstruction(arg, proc, ctx, 1)
	require.NoError(t, err)
	require.True(t, pipeInstr.Insert.ToExternal)
	require.Equal(t, stmtAt.UnixNano(), pipeInstr.Insert.ExternalStmtUnixNano)

	restored, err := convertToVmOperator(pipeInstr, ctx, nil)
	require.NoError(t, err)
	restoredOp := restored.(*insert.Insert)
	defer restoredOp.Release()
	require.True(t, restoredOp.ToExternal)
	cfg := restoredOp.InsertCtx.ExternalConfig
	require.Equal(t, "stage://s/p-%U.csv", cfg.Pattern)
	require.Equal(t, "csv", cfg.Format)
	require.True(t, cfg.Stmt.Equal(stmtAt))
	require.Equal(t, []string{"a"}, restoredOp.InsertCtx.Attrs)
}

// TestExternalInsertRemoteRunTailParity: remote CNs RE-DERIVE the writer
// config from the serialized TableDef instead of receiving it, so every
// FIELDS/LINES option the local path resolves must come out identical on
// decode — and the session time zone must survive via the explicit proto
// fields (the generic session codec round-trips zones lossily).
func TestExternalInsertRemoteRunTailParity(t *testing.T) {
	raw, err := json.Marshal(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Option: []string{"format", "csv", "write_file_pattern", "stage://s/p-%U.csv"},
			Tail: &tree.TailParameter{
				Fields: &tree.Fields{
					Terminated: &tree.Terminated{Value: "||"},
					EnclosedBy: &tree.EnclosedBy{Value: '\''},
					EscapedBy:  &tree.EscapedBy{Value: '!'},
				},
				Lines: &tree.Lines{
					StartingBy:   "R>",
					TerminatedBy: &tree.Terminated{Value: "\r\n"},
				},
			},
		},
	})
	require.NoError(t, err)
	tableDef := &plan.TableDef{
		Name:      "wext",
		TableType: catalog.SystemExternalRel,
		Createsql: string(raw),
		Cols:      []*plan.ColDef{{Name: "a"}},
	}

	stmtAt := time.Unix(1718000000, 0).UTC()
	local, err := buildExternalInsertArg(t.Context(), &plan.ObjectRef{ObjName: "wext"},
		tableDef, true, nil, stmtAt)
	require.NoError(t, err)
	defer local.Release()

	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = context.Background()
	proc.Base.SessionInfo.TimeZone = time.UTC

	_, pipeInstr, err := convertToPipelineInstruction(local, proc, ctx, 1)
	require.NoError(t, err)
	require.Equal(t, "UTC", pipeInstr.Insert.ExternalTzName)

	restored, err := convertToVmOperator(pipeInstr, ctx, nil)
	require.NoError(t, err)
	remote := restored.(*insert.Insert)
	defer remote.Release()

	lc, rc := local.InsertCtx.ExternalConfig, remote.InsertCtx.ExternalConfig
	require.Equal(t, lc.Pattern, rc.Pattern)
	require.Equal(t, lc.Format, rc.Format)
	require.Equal(t, lc.FieldTerminator, rc.FieldTerminator)
	require.Equal(t, lc.LineTerminator, rc.LineTerminator)
	require.Equal(t, lc.LineStartingBy, rc.LineStartingBy)
	require.Equal(t, lc.EnclosedBy, rc.EnclosedBy)
	require.Equal(t, lc.EscapedBy, rc.EscapedBy)
	require.Equal(t, lc.NoEscape, rc.NoEscape)
	require.Equal(t, time.UTC, rc.TimeZone)

	// ESCAPED BY '' (NoEscape) survives the round-trip too.
	raw2, err := json.Marshal(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Option: []string{"format", "csv", "write_file_pattern", "stage://s/p-%U.csv"},
			Tail: &tree.TailParameter{
				Fields: &tree.Fields{EscapedBy: &tree.EscapedBy{Value: 0}},
			},
		},
	})
	require.NoError(t, err)
	tableDef2 := &plan.TableDef{
		Name:      "wext2",
		TableType: catalog.SystemExternalRel,
		Createsql: string(raw2),
		Cols:      []*plan.ColDef{{Name: "a"}},
	}
	local2, err := buildExternalInsertArg(t.Context(), &plan.ObjectRef{ObjName: "wext2"},
		tableDef2, true, nil, stmtAt)
	require.NoError(t, err)
	defer local2.Release()
	require.True(t, local2.InsertCtx.ExternalConfig.NoEscape)

	_, pipeInstr2, err := convertToPipelineInstruction(local2, proc, ctx, 1)
	require.NoError(t, err)
	restored2, err := convertToVmOperator(pipeInstr2, ctx, nil)
	require.NoError(t, err)
	remote2 := restored2.(*insert.Insert)
	defer remote2.Release()
	require.True(t, remote2.InsertCtx.ExternalConfig.NoEscape)
}
