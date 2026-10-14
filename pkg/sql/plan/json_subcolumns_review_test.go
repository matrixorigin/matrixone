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
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func jsonReviewProjectionQuery(columns int, path string) (*pb.Query, *pb.Node) {
	scan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, BindingTags: []int32{7}, TableDef: &pb.TableDef{}}
	project := &pb.Node{NodeType: pb.Node_PROJECT, Children: []int32{0}}
	for i := 0; i < columns; i++ {
		scan.TableDef.Cols = append(scan.TableDef.Cols, &pb.ColDef{Seqnum: uint32(i), Typ: pb.Type{Id: int32(types.T_json)}})
		col := &pb.Expr{Typ: pb.Type{Id: int32(types.T_json)}, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 7, ColPos: int32(i)}}}
		project.ProjectList = append(project.ProjectList, jpCallExpr("json_extract", col, jpStrLit(path)))
	}
	return &pb.Query{Steps: []int32{1}, Nodes: []*pb.Node{scan, project}}, scan
}

func TestJSONSubcolumnProjectionAdmission(t *testing.T) {
	for _, columns := range []int{1, 63, 64, 65, 4096} {
		t.Run(fmt.Sprintf("columns_%d", columns), func(t *testing.T) {
			q, scan := jsonReviewProjectionQuery(columns, "$.x")
			proveJSONPathProjection(q)
			if columns > 64 {
				require.Empty(t, scan.ExtraOptions, "valid wide queries must use full JSON")
				return
			}
			p, err := objectio.DecodeJSONPathProjection(scan.ExtraOptions)
			require.NoError(t, err)
			require.Len(t, p, columns)
		})
	}
	base, err := json.Marshal(objectio.JSONPathProjection{0: {"$.x"}})
	require.NoError(t, err)
	baseBytes := len(objectio.JSONProjectionPrefix) + len(base)
	for _, size := range []int{(64 << 10) - 1, 64 << 10, (64 << 10) + 1} {
		t.Run(fmt.Sprintf("bytes_%d", size), func(t *testing.T) {
			path := "$." + strings.Repeat("x", size-baseBytes+1)
			raw, err := json.Marshal(objectio.JSONPathProjection{0: {path}})
			require.NoError(t, err)
			require.Equal(t, size, len(objectio.JSONProjectionPrefix)+len(raw))
			q, scan := jsonReviewProjectionQuery(1, path)
			proveJSONPathProjection(q)
			if size > 64<<10 {
				require.Empty(t, scan.ExtraOptions, "oversized optimization must decline")
				return
			}
			require.Len(t, scan.ExtraOptions, size)
			p, err := objectio.DecodeJSONPathProjection(scan.ExtraOptions)
			require.NoError(t, err)
			require.Equal(t, []string{path}, p[0])
		})
	}
	t.Run("encoded_escape_expansion", func(t *testing.T) {
		key, err := json.Marshal(strings.Repeat("\"", 20000))
		require.NoError(t, err)
		path := "$." + string(key)
		require.Less(t, len(path), 64<<10)
		raw, err := json.Marshal(objectio.JSONPathProjection{0: {path}})
		require.NoError(t, err)
		require.Greater(t, len(objectio.JSONProjectionPrefix)+len(raw), 64<<10)
		q, scan := jsonReviewProjectionQuery(1, path)
		proveJSONPathProjection(q)
		require.Empty(t, scan.ExtraOptions)
	})
}
