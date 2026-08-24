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

package explain

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestMongoDBExtraInfoShowsOperationAndDigestWithoutQueryText(t *testing.T) {
	rawQuery := `{"pipeline":[{"$count":"secret_literal"}]}`
	node := &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		ExternScan: &plan.ExternScan{
			Type: int32(plan.ExternType_MONGODB_TB),
			MongodbScan: &plan.MongoScan{
				TableId:         7,
				Columns:         []*plan.MongoColumnMapping{{Name: "count", Path: "count"}},
				UserQueryKind:   2,
				UserQueryDigest: strings.Repeat("a", 64),
			},
		},
	}
	lines, err := NewNodeDescriptionImpl(node).GetExtraInfo(context.Background(), &ExplainOptions{})
	require.NoError(t, err)
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], "operation=aggregate")
	require.Contains(t, lines[0], "query_digest=aaaaaaaaaaaa")
	require.NotContains(t, lines[0], rawQuery)
	require.NotContains(t, lines[0], "secret_literal")
}
