// Copyright 2026 Matrix Origin
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

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestMemberOfSyntaxAndFormatting(t *testing.T) {
	tests := []struct {
		input  string
		output string
	}{
		{
			input:  `select 1 member of ('[1]')`,
			output: `select 1 member of ('[1]')`,
		},
		{
			input:  `select 1 member ('[1]')`,
			output: `select 1 member of ('[1]')`,
		},
		{
			input:  `select 1 + 1 member of ('[2]') and 3 member of ('[3]')`,
			output: `select 1 + 1 member of ('[2]') and 3 member of ('[3]')`,
		},
	}

	for _, test := range tests {
		stmt, err := ParseOne(context.Background(), test.input, 1)
		require.NoError(t, err, test.input)
		formatted := tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithSingleQuoteString())
		require.Equal(t, test.output, formatted)
		stmt.Free()

		roundTrip, err := ParseOne(context.Background(), formatted, 1)
		require.NoError(t, err, formatted)
		require.Equal(t, formatted, tree.StringWithOpts(roundTrip, dialect.MYSQL, tree.WithSingleQuoteString()))
		roundTrip.Free()
	}
}

func TestMemberRemainsNonReservedIdentifier(t *testing.T) {
	for _, input := range []string{
		"select member from member",
		"select member as member from member",
		"select member.member from member.member",
	} {
		stmt, err := ParseOne(context.Background(), input, 1)
		require.NoError(t, err, input)
		stmt.Free()
	}
}

func TestMemberOfSyntaxErrors(t *testing.T) {
	for _, input := range []string{
		"select 1 member of '[1]'",
		"select 1 member of ()",
		"select 1 member of ('[1]', '[2]')",
		"select 1 not member of ('[1]')",
	} {
		_, err := ParseOne(context.Background(), input, 1)
		require.Error(t, err, input)
	}
}
