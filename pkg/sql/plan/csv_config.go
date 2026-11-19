// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
)

// CSVConfigForLoad maps LOAD options to the parser grammar shared by prescan and execution.
func CSVConfigForLoad(extern *tree.ExternParam) *csvparser.CSVConfig {
	fieldsTerminatedBy := tree.DefaultFieldsTerminated
	fieldsEnclosedBy := tree.DefaultFieldsEnclosedBy
	fieldsEscapedBy := tree.DefaultFieldsEscapedBy

	linesTerminatedBy := "\n"
	linesStartingBy := ""

	if extern.Tail.Fields != nil {
		if terminated := extern.Tail.Fields.Terminated; terminated != nil && terminated.Value != "" {
			fieldsTerminatedBy = terminated.Value
		}
		if enclosed := extern.Tail.Fields.EnclosedBy; enclosed != nil && enclosed.Value != 0 {
			fieldsEnclosedBy = string(enclosed.Value)
		}
		if escaped := extern.Tail.Fields.EscapedBy; escaped != nil {
			if escaped.Value == 0 {
				fieldsEscapedBy = ""
			} else {
				fieldsEscapedBy = string(escaped.Value)
			}
		}
	}

	if extern.Tail.Lines != nil {
		if terminated := extern.Tail.Lines.TerminatedBy; terminated != nil && terminated.Value != "" {
			linesTerminatedBy = extern.Tail.Lines.TerminatedBy.Value
		}
		if extern.Tail.Lines.StartingBy != "" {
			linesStartingBy = extern.Tail.Lines.StartingBy
		}
	}

	if extern.Format == tree.JSONLINE {
		fieldsTerminatedBy = "\t"
		fieldsEscapedBy = ""
	}

	config := csvparser.CSVConfig{
		FieldsTerminatedBy: fieldsTerminatedBy,
		FieldsEnclosedBy:   fieldsEnclosedBy,
		FieldsEscapedBy:    fieldsEscapedBy,
		LinesTerminatedBy:  linesTerminatedBy,
		LinesStartingBy:    linesStartingBy,
		NotNull:            false,
		Null:               []string{`\N`},
		UnescapedQuote:     true,
		// Comment marker comes from the table's COMMENT option; empty (the
		// default) means no marker, so every line is data. A configured marker
		// skips lines whose raw prefix matches it (checked before unquoting).
		Comment: GetCSVComment(extern),
	}

	if extern.Format == tree.JSONLINE {
		config.SkipWhitespaceRecords = true
		config.Context = extern.Ctx
	}

	return &config
}
