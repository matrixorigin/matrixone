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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
