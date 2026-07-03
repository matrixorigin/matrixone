// Copyright 2021 Matrix Origin
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

package interactive

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
	"github.com/stretchr/testify/require"
)

func TestObjectUnifiedModelUpdateAndCommandMode(t *testing.T) {
	dir := t.TempDir()
	objectPath := createTestObjectFileWithRowsAndCols(t, dir, "unified_model.obj", 3, 3)

	reader, err := objecttool.Open(context.Background(), objectPath)
	require.NoError(t, err)
	defer reader.Close()

	model := NewObjectUnifiedModel(context.Background(), reader, &ViewOptions{
		StartRow:    1,
		EndRow:      2,
		ColumnNames: map[uint16]string{0: "id"},
		ColumnExpander: &ColumnExpander{
			SourceCol: 2,
			NewCols:   []string{"left", "right"},
			NewTypes:  []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
			ExpandFunc: func(any) []any {
				return []any{"l", "r"}
			},
		},
		ObjectNameCol:  0,
		BaseDir:        dir,
		CustomOverview: func([][]string) string { return "overview" },
	})
	_ = model.Init()

	updated, cmd := model.Update(tea.WindowSizeMsg{Width: 100, Height: 30})
	require.Nil(t, cmd)
	model = updated.(*ObjectUnifiedModel)

	updated, cmd = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("m")})
	require.Nil(t, cmd)
	model = updated.(*ObjectUnifiedModel)
	require.Equal(t, ViewModeBlkMeta, model.state.viewMode)

	updated, cmd = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("M")})
	require.Nil(t, cmd)
	model = updated.(*ObjectUnifiedModel)
	require.Equal(t, ViewModeObjMeta, model.state.viewMode)

	updated, cmd = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")})
	require.Nil(t, cmd)
	model = updated.(*ObjectUnifiedModel)
	require.Equal(t, ViewModeData, model.state.viewMode)

	updated, cmd = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(":")})
	require.Nil(t, cmd)
	model = updated.(*ObjectUnifiedModel)
	require.True(t, model.cmdMode)

	updated, cmd = model.handleCommandMode(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("2")})
	require.Nil(t, cmd)
	model = updated.(*ObjectUnifiedModel)
	require.Equal(t, "2", model.cmdInput)

	updated, cmd = model.handleCommandMode(tea.KeyMsg{Type: tea.KeyBackspace})
	require.Nil(t, cmd)
	model = updated.(*ObjectUnifiedModel)
	require.Empty(t, model.cmdInput)

	updated, cmd = model.handleCommandMode(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("1")})
	require.Nil(t, cmd)
	model = updated.(*ObjectUnifiedModel)
	updated, cmd = model.handleCommandMode(tea.KeyMsg{Type: tea.KeyEnter})
	require.Nil(t, cmd)
	model = updated.(*ObjectUnifiedModel)
	require.False(t, model.cmdMode)
	require.Equal(t, int64(1), model.state.GlobalRowOffset())

	model.state.objectToOpen = "nested.obj"
	require.Equal(t, dir+"/nested.obj", model.GetObjectToOpen())
	model.ClearObjectToOpen()
	require.Empty(t, model.GetObjectToOpen())
	require.Contains(t, model.View(), "overview")
}

func TestObjectUnifiedEntrypointsReturnOpenErrors(t *testing.T) {
	ctx := context.Background()
	require.Error(t, Run("/path/that/does/not/exist.obj"))

	fs, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.Error(t, RunWithFS(ctx, fs, "missing.obj"))
	require.Error(t, RunUnifiedWithFS(ctx, fs, "missing.obj", nil))
}

func TestRunUnifiedWithFSSuccessQuitsFromInput(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	createTestObjectFileWithRowsAndCols(t, dir, "run_unified.obj", 2, 2)
	objectPath := dir + string(os.PathSeparator) + "run_unified.obj"

	oldNewObjectProgram := newObjectProgram
	t.Cleanup(func() { newObjectProgram = oldNewObjectProgram })
	newObjectProgram = func(m tea.Model) *tea.Program {
		return tea.NewProgram(m, tea.WithInput(strings.NewReader("q")), tea.WithOutput(io.Discard))
	}

	fs, err := fileservice.NewFileService(ctx, fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
		Cache:   fileservice.DisabledCacheConfig,
	}, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)

	require.NoError(t, RunUnifiedWithFS(ctx, fs, "run_unified.obj", &ViewOptions{
		StartRow: 0,
		EndRow:   1,
	}))
	require.NoError(t, RunUnified(ctx, objectPath, nil))
}

func TestStateColumnsForMetadataAndExpandedData(t *testing.T) {
	state := &State{
		viewMode: ViewModeObjMeta,
		metaCols: []ColInfo{
			{Idx: 0, Name: "Property", Type: types.T_varchar.ToType()},
			{Idx: 1, Name: "Value", Type: types.T_varchar.ToType()},
		},
	}
	require.Len(t, state.Columns(), 2)

	state.viewMode = ViewModeData
	state.colExpander = &ColumnExpander{
		SourceCol: 1,
		NewCols:   []string{"a", "b"},
		NewTypes:  []types.Type{types.T_int32.ToType(), types.T_varchar.ToType()},
	}
	cols := state.expandColumns([]objecttool.ColInfo{
		{Idx: 0, SeqNum: 0, Type: types.T_int32.ToType()},
		{Idx: 1, SeqNum: 1, Type: types.T_varchar.ToType()},
		{Idx: 2, SeqNum: 2, Type: types.T_bool.ToType()},
	})
	require.Len(t, cols, 4)
	require.Equal(t, uint16(2), cols[2].Idx)
	require.Equal(t, types.T_varchar, cols[2].Type.Oid)
	require.Equal(t, uint16(3), cols[3].Idx)
}

func TestParseNumber(t *testing.T) {
	var n int64
	rest, err := parseNumber("123abc", &n)
	require.NoError(t, err)
	require.Equal(t, int64(123), n)
	require.Equal(t, "abc", rest)

	rest, err = parseNumber("", &n)
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Equal(t, int64(0), n)
}

func TestRunUnifiedOpenErrorForDirectory(t *testing.T) {
	dir := t.TempDir()
	require.Error(t, RunUnified(context.Background(), dir+string(os.PathSeparator)+"missing.obj", nil))
}
