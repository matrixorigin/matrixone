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
	"errors"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	objectinteractive "github.com/matrixorigin/matrixone/pkg/tools/objecttool/interactive"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/stretchr/testify/require"
)

type fakeCheckpointProgram struct {
	model tea.Model
	err   error
}

func (p fakeCheckpointProgram) Run() (tea.Model, error) {
	return p.model, p.err
}

type nonUnifiedModel struct{}

func (nonUnifiedModel) Init() tea.Cmd                       { return nil }
func (nonUnifiedModel) Update(tea.Msg) (tea.Model, tea.Cmd) { return nonUnifiedModel{}, nil }
func (nonUnifiedModel) View() string                        { return "" }

func TestRunOpensObjectAndContinues(t *testing.T) {
	oldNewProgram := newCheckpointProgram
	oldRunObject := runObjectViewerWithFS
	defer func() {
		newCheckpointProgram = oldNewProgram
		runObjectViewerWithFS = oldRunObject
	}()

	reader := &checkpointtool.CheckpointReader{}
	first := NewUnifiedModel(reader)
	rng := ckputil.TableRange{ObjectType: ckputil.ObjectType_Data}
	rng.Start.SetRowOffset(3)
	rng.End.SetRowOffset(9)
	first.objectToOpen = "nested.obj"
	first.rangeToOpen = &rng
	second := NewUnifiedModel(reader)
	models := []tea.Model{first, second}
	newCheckpointProgram = func(tea.Model) checkpointProgram {
		require.NotEmpty(t, models)
		model := models[0]
		models = models[1:]
		return fakeCheckpointProgram{model: model}
	}

	var openedPath string
	runObjectViewerWithFS = func(ctx context.Context, fs fileservice.FileService, path string, opts *objectinteractive.ViewOptions) error {
		require.NotNil(t, ctx)
		require.Nil(t, fs)
		openedPath = path
		require.Equal(t, int64(3), opts.StartRow)
		require.Equal(t, int64(9), opts.EndRow)
		require.Empty(t, opts.Kind)
		require.Equal(t, 4, opts.ObjectNameCol)
		require.NotNil(t, opts.ColumnExpander)
		require.NotNil(t, opts.CustomOverview)
		return nil
	}

	require.NoError(t, Run(reader))
	require.Equal(t, "nested.obj", openedPath)
	require.Empty(t, first.GetObjectToOpen())
	require.Nil(t, first.GetRangeToOpen())
	require.Empty(t, models)
}

func TestRunProgramAndViewerExitBranches(t *testing.T) {
	oldNewProgram := newCheckpointProgram
	oldRunObject := runObjectViewerWithFS
	defer func() {
		newCheckpointProgram = oldNewProgram
		runObjectViewerWithFS = oldRunObject
	}()

	runErr := errors.New("program failed")
	newCheckpointProgram = func(tea.Model) checkpointProgram {
		return fakeCheckpointProgram{err: runErr}
	}
	require.ErrorIs(t, Run(&checkpointtool.CheckpointReader{}), runErr)

	newCheckpointProgram = func(tea.Model) checkpointProgram {
		return fakeCheckpointProgram{model: nonUnifiedModel{}}
	}
	require.NoError(t, Run(&checkpointtool.CheckpointReader{}))

	viewerErr := errors.New("viewer failed")
	first := NewUnifiedModel(&checkpointtool.CheckpointReader{})
	rng := ckputil.TableRange{}
	first.objectToOpen = "nested.obj"
	first.rangeToOpen = &rng
	newCheckpointProgram = func(tea.Model) checkpointProgram {
		return fakeCheckpointProgram{model: first}
	}
	runObjectViewerWithFS = func(context.Context, fileservice.FileService, string, *objectinteractive.ViewOptions) error {
		return viewerErr
	}
	require.ErrorIs(t, Run(&checkpointtool.CheckpointReader{}), viewerErr)
}

func TestRunCancelsAndJoinsBlockedLogicalLoad(t *testing.T) {
	oldNewProgram := newCheckpointProgram
	defer func() { newCheckpointProgram = oldNewProgram }()

	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	newCheckpointProgram = func(model tea.Model) checkpointProgram {
		m := model.(*UnifiedModel)
		m.state.selectedEntry = 0
		m.state.selectedTable = 42
		m.state.buildLogicalViewForTest = func(ctx context.Context, _ int, _ uint64) (*checkpointtool.LogicalTableView, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-release
			return nil, ctx.Err()
		}
		_, cmd := m.Update(openLogicalTableMsg{})
		go cmd()
		<-started
		return fakeCheckpointProgram{model: m}
	}

	runDone := make(chan error, 1)
	go func() { runDone <- Run(&checkpointtool.CheckpointReader{}) }()
	<-canceled
	select {
	case err := <-runDone:
		require.Failf(t, "Run returned before logical load joined", "err=%v", err)
	default:
	}
	close(release)
	released = true
	require.NoError(t, <-runDone)
}
