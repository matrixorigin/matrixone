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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativePADDiagnosticMetadataAdmission(t *testing.T) {
	for _, charset := range []uint32{2, 3} {
		for _, preinsert := range []bool{false, true} {
			for _, revision := range []uint32{0, 1, 2} {
				t.Run(fmt.Sprintf("charset=%d/preinsert=%v/revision=%d", charset, preinsert, revision), func(t *testing.T) {
					expr := &Expr{Typ: Type{Id: planVarcharTypeID, Charset: charset, CollationVersion: revision}}
					node := &Node{DedupKeyExprs: []*Expr{expr}}
					if preinsert {
						node = &Node{PreInsertUkCtx: &PreInsertUkCtx{KeyDiagnosticExprs: []*ExprList{{List: []*Expr{expr}}}}}
					}
					wire, err := node.Marshal()
					require.NoError(t, err)
					var restored Node
					err = restored.Unmarshal(wire)
					if revision == 2 {
						require.Error(t, err, "新增诊断容器也必须拒绝未知revision")
						return
					}
					require.NoError(t, err)
					features, err := RequiredExecutableRemoteExpressionFeatures(&restored)
					require.NoError(t, err)
					require.Equal(t, revision == 1, features.NativePADCollation)
					if revision == 1 {
						require.Error(t, RequireLegacyCollations(&restored))
					} else {
						require.NoError(t, RequireLegacyCollations(&restored))
					}
					require.Equal(t, revision, expr.Typ.CollationVersion)
				})
			}
		}
	}
}
