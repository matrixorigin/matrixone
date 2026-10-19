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

package moerr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDigestParseErrors(t *testing.T) {
	disclosed := NewParseErrorInDigestFunction(context.Background(), "bad SQL")
	require.Equal(t, ER_PARSE_ERROR_IN_DIGEST_FN, disclosed.MySQLCode())
	require.Equal(t, MySQLDefaultSqlState, disclosed.SqlState())
	require.Equal(t, `Could not parse argument to digest function: "bad SQL".`, disclosed.Error())

	undisclosed := NewUndisclosedParseErrorInDigestFunction(context.Background())
	require.Equal(t, ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN, undisclosed.MySQLCode())
	require.Equal(t, MySQLDefaultSqlState, undisclosed.SqlState())
	require.Equal(t, "Could not parse argument to digest function.", undisclosed.Error())
}
