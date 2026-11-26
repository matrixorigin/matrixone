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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestIncompleteTemporalExecutionStreamControls(t *testing.T) {
	for _, name := range []string{"typed week", "typed to_days", "literal"} {
		t.Run(name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.version = defines.MORPCVersion130
			fn := "week"
			if name == "typed to_days" {
				fn = "to_days"
			}
			scope := incompleteTemporalStreamScope(t, c, fn, types.T_date)
			if name == "literal" {
				scope.RootOp.(*projection.Projection).ProjectList = []*planpb.Expr{plan2.MakePlan2StringConstExprWithType("2001-00-00")}
			}
			stream := &fakeStreamSender{}
			installIncompleteTemporalTestStream(t, c, stream)
			require.NoError(t, scope.RemoteRun(c))
			require.Len(t, stream.sent, 1)
			require.Equal(t, pipeline.Method_PipelineMessage, stream.sent[0].(*pipeline.Message).GetCmd(), "unaffected projections must not add a handshake")
		})
	}
}
