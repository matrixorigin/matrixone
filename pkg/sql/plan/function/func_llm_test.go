package function

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

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
