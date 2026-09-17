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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestParseJSONValueDateRejectsTimeComponent(t *testing.T) {
	for _, input := range []string{
		"2024-01-02 12:34:56",
		"2024-01-02T12:34:56",
	} {
		_, err := parseJSONValueDate(jsonValueExtracted{text: input}, types.T_date.ToType())
		require.Error(t, err, input)
	}

	got, err := parseJSONValueDate(jsonValueExtracted{text: "2024-01-02"}, types.T_date.ToType())
	require.NoError(t, err)
	require.Equal(t, "2024-01-02", got.String())
}
