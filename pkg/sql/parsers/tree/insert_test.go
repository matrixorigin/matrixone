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

package tree

import "testing"

func TestInsertValuesSourcePreservesStatementShape(t *testing.T) {
	for _, tc := range []struct {
		source SelectStatement
		values bool
	}{
		{&ValuesClause{}, true}, {&SelectClause{}, false},
		{&ParenSelect{Select: &Select{Select: &ValuesClause{}}}, true},
		{&ParenSelect{Select: &Select{Select: &SelectClause{}}}, false},
		{&ParenSelect{}, false},
	} {
		stmt := &Insert{Rows: &Select{Select: tc.source}}
		if got := stmt.IsValuesSource(); got != tc.values {
			t.Fatalf("source %T: got %v, want %v", tc.source, got, tc.values)
		}
	}
	var stmt *Insert
	if stmt.IsValuesSource() || (&Insert{}).IsValuesSource() {
		t.Fatal("missing source is not VALUES")
	}
}
