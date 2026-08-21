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

package substrait

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	planbuilder "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

func TestExportCanonicalTPCHPlans(t *testing.T) {
	mock := planbuilder.NewMockOptimizer(false)
	for queryNumber := 1; queryNumber <= 22; queryNumber++ {
		t.Run(fmt.Sprintf("q%d", queryNumber), func(t *testing.T) {
			path := filepath.Join("..", "tpch", fmt.Sprintf("q%d.sql", queryNumber))
			wire, err := os.ReadFile(path)
			require.NoError(t, err)
			statements, err := parsers.Parse(context.Background(), dialect.MYSQL, string(wire), 1)
			require.NoError(t, err)
			query, err := mock.Optimize(statements[0])
			require.NoError(t, err)

			// MockOptimizer shares catalog pointers between equivalent scans. One
			// harmless identity keeps those aliases internally consistent while the
			// test remains about the logical Substrait coverage, not catalog setup.
			for _, read := range query.Nodes {
				if read == nil || read.TableDef == nil || read.ObjRef == nil {
					continue
				}
				read.TableDef.DbId = 7
				read.TableDef.TblId = 42
				read.ObjRef.Obj = 42
			}

			candidate, err := Export(query)
			require.NoError(t, err)
			readValues := make(map[int32][]byte, len(candidate.Reads()))
			for _, read := range candidate.Reads() {
				readValues[read.NodeID] = []byte{1}
			}
			wirePlan, err := candidate.Build(readValues)
			require.NoError(t, err)
			require.LessOrEqual(t, len(wirePlan), MaxPlanBytes)
			plan := new(spb.Plan)
			require.NoError(t, proto.Unmarshal(wirePlan, plan))
			require.Len(t, plan.Relations, len(query.Steps))
			require.Equal(t, query.Headings, plan.Relations[len(plan.Relations)-1].GetRoot().Names)
		})
	}
}
