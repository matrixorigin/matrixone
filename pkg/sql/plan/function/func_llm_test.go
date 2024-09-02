package function

import (
	"fmt"
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
