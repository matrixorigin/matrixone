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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/monlp/llm"
	"github.com/stretchr/testify/require"
	"go.starlark.net/starlark"
)

// Non-string @llm_* session variables must produce an error, not a Go
// type-assertion panic (issue #29154).
func TestStarlarkLLMGetVarValidatesSessionVariableType(t *testing.T) {
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	si := &starlarkInterpreter{interp: &Interpreter{ctx: context.Background(), ses: ses}}

	for _, tc := range []struct {
		value   any
		want    string
		wantErr bool
	}{
		{value: "echo", want: "echo"},
		{value: []byte("echo"), want: "echo"},
		{value: nil, want: ""},
		{value: int64(1), wantErr: true},
		{value: true, wantErr: true},
		{value: 1.5, wantErr: true},
	} {
		require.NoError(t, ses.SetUserDefinedVar("llm_model", tc.value, "set @llm_model"))
		var got string
		var err error
		require.NotPanics(t, func() { got, err = si.llmGetVar("llm_model", "") })
		if tc.wantErr {
			require.Error(t, err, "%T", tc.value)
			require.Contains(t, err.Error(), "@llm_model must be a string")
			continue
		}
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}

	// an explicit argument wins over the session variable
	got, err := si.llmGetVar("llm_model", "explicit")
	require.NoError(t, err)
	require.Equal(t, "explicit", got)
	// unset variable
	got, err = si.llmGetVar("llm_unset", "")
	require.NoError(t, err)
	require.Equal(t, "", got)

	// llmConnect surfaces the error instead of panicking
	require.NoError(t, ses.SetUserDefinedVar("llm_model", int64(1), "set @llm_model = 1"))
	require.NotPanics(t, func() {
		_, err = si.llmConnect("", "", "", "")
	})
	require.Error(t, err)
}

func TestStarlarkLLMFailedConnectionCanBeRetried(t *testing.T) {
	for _, explicit := range []bool{true, false} {
		t.Run(map[bool]string{true: "explicit reconnect", false: "implicit chat"}[explicit], func(t *testing.T) {
			ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
			si := &starlarkInterpreter{interp: &Interpreter{ctx: context.Background(), ses: ses}}
			require.NoError(t, ses.SetUserDefinedVar("llm_model", nil, "set @llm_model = NULL"))
			if explicit {
				client, err := llm.NewLLMClient("", "", "echo", "")
				require.NoError(t, err)
				si.llm = client
				ret, err := si.moLlmConnect(nil, nil, starlark.Tuple{starlark.String(""), starlark.String(""), starlark.String(""), starlark.String("")}, nil)
				require.NoError(t, err)
				require.Contains(t, ret.(*starlark.List).Index(1).String(), "invalid model")
				require.True(t, si.llm == nil)
			}
			prompt := starlark.Tuple{starlark.String(`[{"role":"user","content":"ok"}]`)}
			for i := 0; i < 2; i++ {
				ret, err := si.moLlmChat(nil, nil, prompt, nil)
				require.NoError(t, err)
				require.Equal(t, starlark.None, ret.(*starlark.List).Index(0))
				require.Contains(t, ret.(*starlark.List).Index(1).String(), "invalid model")
				require.True(t, si.llm == nil)
			}
			require.NoError(t, ses.SetUserDefinedVar("llm_model", "echo", "set @llm_model = 'echo'"))
			ret, err := si.moLlmChat(nil, nil, prompt, nil)
			require.NoError(t, err)
			require.Equal(t, starlark.String("ok"), ret.(*starlark.List).Index(0))
			require.Equal(t, starlark.None, ret.(*starlark.List).Index(1))
		})
	}
}
