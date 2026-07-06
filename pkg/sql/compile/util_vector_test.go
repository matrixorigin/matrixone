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

package compile

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// gen{Delete,Build}{Cagra,Ivfpq}Index tests were removed when those
// functions moved into pkg/vectorindex/<algo>/plugin/compile/. The
// end-to-end SQL suite in test/distributed/cases/vector/ exercises the
// same paths.

// filterColumnsFromParams ---------------------------------------------------

func TestFilterColumnsFromParams_Empty(t *testing.T) {
	require.Equal(t, "", filterColumnsFromParams("", "src"))
}

func TestFilterColumnsFromParams_BadJSON(t *testing.T) {
	require.Equal(t, "", filterColumnsFromParams("not json", "src"))
}

func TestFilterColumnsFromParams_KeyMissing(t *testing.T) {
	require.Equal(t, "", filterColumnsFromParams(`{"m":"32"}`, "src"))
}

func TestFilterColumnsFromParams_NotString(t *testing.T) {
	// included_columns present but not a string → StrictString fails.
	require.Equal(t, "", filterColumnsFromParams(`{"included_columns": 42}`, "src"))
}

func TestFilterColumnsFromParams_EmptyString(t *testing.T) {
	require.Equal(t, "", filterColumnsFromParams(`{"included_columns": ""}`, "src"))
}

func TestFilterColumnsFromParams_Single(t *testing.T) {
	require.Equal(t, ", src.price",
		filterColumnsFromParams(`{"included_columns":"price"}`, "src"))
}

func TestFilterColumnsFromParams_MultipleAndTrim(t *testing.T) {
	out := filterColumnsFromParams(`{"included_columns":" price , category , "}`, "src")
	require.Equal(t, ", src.price, src.category", out)
}
