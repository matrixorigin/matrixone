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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestValidateExplainOptionsBeforePlan(t *testing.T) {
	tests := []struct {
		name    string
		options []tree.OptionElem
		mode    string
		wantErr bool
	}{
		{
			name: "analyze false JSON remains ordinary",
			options: []tree.OptionElem{
				tree.MakeOptionElem(tree.AnalyzeOption, "FALSE"),
				tree.MakeOptionElem(tree.FormatOption, "JSON"),
			},
		},
		{
			name: "duplicate is rejected before execution mode",
			options: []tree.OptionElem{
				tree.MakeOptionElem(tree.FormatOption, "JSON"),
				tree.MakeOptionElem(tree.FormatOption, "TEXT"),
			},
			wantErr: true,
		},
		{
			name: "analyze JSON is rejected",
			options: []tree.OptionElem{
				tree.MakeOptionElem(tree.FormatOption, "JSON"),
			},
			mode:    "ANALYZE",
			wantErr: true,
		},
		{
			name: "phyplan JSON is rejected",
			options: []tree.OptionElem{
				tree.MakeOptionElem(tree.FormatOption, "JSON"),
			},
			mode:    "PHYPLAN",
			wantErr: true,
		},
		{
			name: "JSON check is rejected",
			options: []tree.OptionElem{
				tree.MakeOptionElem(tree.FormatOption, "JSON"),
				tree.MakeOptionElem(tree.CheckOption, "[]"),
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateExplainOptions(context.Background(), test.options, test.mode)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
