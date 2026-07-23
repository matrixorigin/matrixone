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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestExternalFileLevelColumnNameMatchesOnlyVirtualColumns(t *testing.T) {
	tests := []struct {
		name string
		col  string
		want bool
	}{
		{name: "account", col: STATEMENT_ACCOUNT, want: true},
		{name: "qualified account", col: "ext." + STATEMENT_ACCOUNT, want: true},
		{name: "filepath", col: catalog.ExternalFilePath, want: true},
		{name: "qualified filepath", col: "ext." + catalog.ExternalFilePath, want: true},
		{name: "account id", col: "account_id", want: false},
		{name: "qualified account id", col: "ext.account_id", want: false},
		{name: "accounting", col: "accounting", want: false},
		{name: "customer account", col: "customer_account", want: false},
		{name: "account in qualifier", col: "account_source.payload", want: false},
		{name: "filepath suffix", col: catalog.ExternalFilePath + "_suffix", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isFileLevelColumnName(tt.col))
		})
	}
}

func TestExternalFileLevelFilterRejectsPhysicalColumns(t *testing.T) {
	column := func(name string) *pbplan.Expr {
		return &pbplan.Expr{Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{Name: name}}}
	}
	literal := func() *pbplan.Expr {
		return &pbplan.Expr{Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{}}}
	}
	function := func(name string, args ...*pbplan.Expr) *pbplan.Expr {
		return &pbplan.Expr{Expr: &pbplan.Expr_F{F: &pbplan.Function{
			Func: &pbplan.ObjectRef{ObjName: name},
			Args: args,
		}}}
	}

	virtualAccountFilter := function("=", column("ext."+STATEMENT_ACCOUNT), literal())
	virtualFilepathFilter := function("=", column("ext."+catalog.ExternalFilePath), literal())
	physicalFilter := function("=", column("ext.account_id"), literal())

	tests := []struct {
		name string
		expr *pbplan.Expr
		want bool
	}{
		{name: "account and literal", expr: virtualAccountFilter, want: true},
		{name: "filepath and literal", expr: virtualFilepathFilter, want: true},
		{name: "physical only", expr: physicalFilter, want: false},
		{
			name: "virtual compared with physical",
			expr: function("=", column("ext."+STATEMENT_ACCOUNT), column("ext.account_id")),
			want: false,
		},
		{
			name: "nested and with physical",
			expr: function("and", virtualAccountFilter, physicalFilter),
			want: false,
		},
		{
			name: "or with virtual branches",
			expr: function("or", virtualAccountFilter, virtualFilepathFilter),
			want: true,
		},
		{
			name: "or with physical branch",
			expr: function("or", virtualAccountFilter, physicalFilter),
			want: false,
		},
		{
			name: "or with literal branch",
			expr: function("or", virtualAccountFilter, literal()),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isFileLevelFilter(tt.expr))
		})
	}
}
