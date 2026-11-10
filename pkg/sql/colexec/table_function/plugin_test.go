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

package table_function

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	extism "github.com/extism/go-sdk"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPluginStateResetClosesPlugin(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	wasmPath := filepath.Join(filepath.Dir(sourceFile), "../../../../test/distributed/resources/plugin/hello.wasm")

	plugin, err := extism.NewPlugin(
		context.Background(),
		extism.Manifest{Wasm: []extism.Wasm{extism.WasmFile{Path: wasmPath}}},
		extism.PluginConfig{EnableWasi: true},
		nil,
	)
	require.NoError(t, err)

	state := &pluginState{plugin: plugin}
	state.reset(nil, nil)
	require.Nil(t, state.plugin)
}

func TestPluginResetRechecksDatalinkAuthorization(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	wasmPath := filepath.Join(filepath.Dir(sourceFile), "../../../../test/distributed/resources/plugin/hello.wasm")
	proc := testutil.NewProc(t)
	proc.Base.IsFrontend = true
	proc.Base.SessionInfo.Account = "sys"
	proc.Base.SessionInfo.Role = "moadmin"

	wasm, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("file://"+wasmPath), 1, proc.Mp())
	require.NoError(t, err)
	fn, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("mowasm_hello"), 1, proc.Mp())
	require.NoError(t, err)
	cfg, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("{}"), 1, proc.Mp())
	require.NoError(t, err)
	// Use ordinary text input so the first authorization boundary reached by the
	// test is the wasm image itself. The datalink input path is a separate,
	// already-covered boundary and would otherwise reject before image admission.
	input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("plugin input"), 1, proc.Mp())
	require.NoError(t, err)
	for _, vec := range []*vector.Vector{wasm, fn, cfg, input} {
		t.Cleanup(func() { vec.Free(proc.Mp()) })
	}

	tf := &TableFunction{
		Attrs: []string{"result"},
		ctr: container{
			argVecs:   []*vector.Vector{wasm, fn, cfg, input},
			retSchema: []types.Type{types.T_json.ToType()},
		},
	}
	state := &pluginState{}
	require.NoError(t, state.start(tf, proc, 0, nil), "privileged consumer admission")
	state.reset(tf, proc)

	proc.Base.SessionInfo.Role = "public"
	err = state.start(tf, proc, 0, nil)
	require.ErrorContains(t, err, "only the sys account moadmin role")
	state.free(tf, proc, false, err)
}
