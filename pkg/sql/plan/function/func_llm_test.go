// Copyright 2024 Matrix Origin
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

package function

import (
	"fmt"
	"testing"
  "fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestChunkString(t *testing.T) {
	type testCase struct {
		testName   string
		text       string
		mode       string
		wantReturn string
		wantErr    string
	}
	tests := []testCase{
		{
			testName:   "correct chunk with fixed width",
			text:       "12345678901234567890",
			mode:       "fixed_width; 2",
			wantReturn: "[[0, 2, \"12\"], [2, 2, \"34\"], [4, 2, \"56\"], [6, 2, \"78\"], [8, 2, \"90\"], [10, 2, \"12\"], [12, 2, \"34\"], [14, 2, \"56\"], [16, 2, \"78\"], [18, 2, \"90\"]]",
			wantErr:    "",
		},
		{
			testName:   "correct chunk with fixed width",
			text:       "1234567890",
			mode:       "fixed_width; 21",
			wantReturn: "[[0, 10, \"1234567890\"]]",
			wantErr:    "",
		},
		{
			testName:   "chinese character",
			text:       "mo数据库",
			mode:       "fixed_width; 2",
			wantReturn: "[[0, 2, \"mo\"], [2, 2, \"数据\"], [4, 1, \"库\"]]",
			wantErr:    "",
		},
		{
			testName: "correct chunk with paragraph",
			text: "12345\n" +
				"678901234\n" +
				"567890",

			mode:       "paragraph",
			wantReturn: "[[0, 6, \"12345\\n\"], [6, 10, \"678901234\\n\"], [16, 7, \"567890\\n\"]]",
			wantErr:    "",
		},
		{
			testName:   "correct chunk with sentence",
			text:       "Welcome to the MatrixOne documentation center!\n\nThis center holds related concepts and technical architecture introductions, product features, user guides, and reference manuals to help you work with MatrixOne.",
			mode:       "sentence",
			wantReturn: "[[0, 46, \"Welcome to the MatrixOne documentation center!\"], [46, 164, \"\\n\\nThis center holds related concepts and technical architecture introductions, product features, user guides, and reference manuals to help you work with MatrixOne.\"]]",
			wantErr:    "",
		},
		{
			testName:   "invalid width argument",
			text:       "hello. hello world",
			mode:       "fixed_width; a",
			wantReturn: "",
			wantErr:    "invalid input: 'fixed_width; a' is not a valid chunk strategy",
		},
		{
			testName:   "invalid width argument",
			text:       "hello. hello world",
			mode:       "fixed_width; -1",
			wantReturn: "",
			wantErr:    "invalid input: 'fixed_width; -1' is not a valid chunk strategy",
		},
		{
			testName:   "invalid chunk strategy",
			text:       "hello. hello world",
			mode:       "matrixone",
			wantReturn: "",
			wantErr:    "invalid input: 'matrixone' is not a valid chunk strategy",
		},
		{
			testName:   "empty chunk strategy",
			text:       "hello. hello world",
			mode:       "",
			wantReturn: "",
			wantErr:    "invalid input: '' is not a valid chunk strategy",
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			got, err := ChunkString(tc.text, tc.mode)
			if err != nil {
				if fmt.Sprintf("%v", err) != tc.wantErr {
					t.Errorf("ChunkString() error = %v, wantErr %v", err, tc.wantErr)
				}
				return
			}
			if got != tc.wantReturn {
				t.Errorf("ChunkString() = %v, want %v", got, tc.wantReturn)
			}
		})
	}

}

func TestLLMExtractText(t *testing.T) {
	testCases := initLLMExtractTextCase()
	wrongTestCases := initLLMExtractWrongTextCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, LLMExtractText)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}

	for _, tc := range wrongTestCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, LLMExtractText)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}

}

func initLLMExtractTextCase() []tcTemp {
	regularCases := []struct {
		info          string
		input         []string
		output        []string
		extractorType []string
		wants         []bool
	}{
		{
			info: "test encode - simple text",
			input: []string{
				fmt.Sprintf("file://%s/../../../../test/distributed/resources/llm_test/extract_text/MODocs1.pdf?offset=0&size=4", GetFilePath()),
				fmt.Sprintf("file://%s/../../../../test/distributed/resources/llm_test/extract_text/example.pdf?offset=0&size=4", GetFilePath()),
			},
			output: []string{
				fmt.Sprintf("file://%s/../../../../test/distributed/resources/llm_test/extract_text/MODocs1.txt", GetFilePath()),
				fmt.Sprintf("file://%s/../../../../test/distributed/resources/llm_test/extract_text/example.txt", GetFilePath()),
			},
			extractorType: []string{
				"pdf",
				"pdf",
			},
			wants: []bool{
				true,
				true,
			},
		},
	}

	var testInputs = make([]tcTemp, 0, len(regularCases))
	for _, c := range regularCases {
		testInputs = append(testInputs, tcTemp{
			info: c.info,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datalink.ToType(), c.input, []bool{}),
				NewFunctionTestInput(types.T_datalink.ToType(), c.output, []bool{}),
				NewFunctionTestInput(types.T_varchar.ToType(), c.extractorType, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false, c.wants, []bool{}),
		})
	}

	return testInputs
}

func initLLMExtractWrongTextCase() []tcTemp {
	regularCases := []struct {
		info          string
		input         []string
		output        []string
		extractorType []string
		wants         []bool
	}{
		{
			info: "test encode - simple text",
			input: []string{
				fmt.Sprintf("file://%s/../../../../test/distributed/resources/llm_test/extract_text/MODocs1.txt?offset=0&size=4", GetFilePath()),
				"",
				fmt.Sprintf("file://%s/../../../../test/distributed/resources/llm_test/extract_text/example.pdf?offset=0&size=4", GetFilePath()),
			},
			output: []string{
				fmt.Sprintf("file://%s/../../../../test/distributed/resources/llm_test/extract_text/MODocs1.txt", GetFilePath()),
				"",
				fmt.Sprintf("file://%s/../../../../test/distributed/resources/llm_test/extract_text/example.txt", GetFilePath()),
			},
			extractorType: []string{
				"pdf",
				"",
				"txt",
			},
			wants: []bool{
				true,
				true,
			},
		},
	}

	var testInputs = make([]tcTemp, 0, len(regularCases))
	for _, c := range regularCases {
		testInputs = append(testInputs, tcTemp{
			info: c.info,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datalink.ToType(), c.input, []bool{}),
				NewFunctionTestInput(types.T_datalink.ToType(), c.output, []bool{}),
				NewFunctionTestInput(types.T_varchar.ToType(), c.extractorType, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), true, c.wants, []bool{}),
		})
	}

	return testInputs
}

func GetFilePath() string {
	dir, _ := os.Getwd()
	return dir
}