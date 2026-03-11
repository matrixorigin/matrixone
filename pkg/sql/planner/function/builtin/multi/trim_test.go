// Copyright 2022 Matrix Origin
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

package multi

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTrim(t *testing.T) {
	kases := []struct {
		mode     string
		input    string
		trimWord string
		output   string
		info     string
	}{
		{
			mode:     "both",
			input:    "   hello world   ",
			trimWord: " ",
			output:   "hello world",
		},
		{
			mode:     "leading",
			input:    "   hello world   ",
			trimWord: " ",
			output:   "hello world   ",
		},
		{
			mode:     "trailing",
			input:    "   hello world   ",
			trimWord: " ",
			output:   "   hello world",
		},
		{
			mode:     "both",
			input:    "   hello world   ",
			trimWord: "h",
			output:   "   hello world   ",
		},
		{
			mode:     "trailing",
			input:    "   hello world",
			trimWord: "d",
			output:   "   hello worl",
		},
		{
			mode:     "leading",
			input:    "hello world   ",
			trimWord: "h",
			output:   "ello world   ",
		},
		{
			mode:     "both",
			input:    "嗷嗷0k七七",
			trimWord: "七",
			output:   "嗷嗷0k",
		},
		{
			mode:     "leading",
			input:    "嗷嗷0k七七",
			trimWord: "七",
			output:   "嗷嗷0k七七",
		},
		{
			mode:     "trailing",
			input:    "嗷嗷0k七七",
			trimWord: "七",
			output:   "嗷嗷0k",
		},
		{
			mode:     "both",
			input:    "嗷嗷0k七七",
			trimWord: "k七七",
			output:   "嗷嗷0",
		},
		{
			mode:     "leading",
			input:    "嗷嗷0k七七",
			trimWord: "",
			output:   "嗷嗷0k七七",
		},
		{
			mode:     "trailing",
			input:    "",
			trimWord: "嗷嗷0k七七",
			output:   "",
		},
	}
	proc := testutil.NewProc()
	for idx, kase := range kases {
		inputs := []testutil.FunctionTestInput{
			testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{kase.mode}, nil),
			testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{kase.trimWord}, nil),
			testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{kase.input}, nil),
		}
		expect := testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{kase.output}, nil)
		kaseNow := testutil.NewFunctionTestCase(proc,
			inputs, expect, Trim)
		s, info := kaseNow.Run()
		require.True(t, s, fmt.Sprintf("case %d, err info is '%s'", idx, info))
	}
}
