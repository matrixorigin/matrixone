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

package sidecar

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBuildManifestIsDeterministic(t *testing.T) {
	a := objectStats(t, 3)
	b := objectStats(t, 7)
	def := &planpb.TableDef{TblId: 42, DbName: "db", Name: "t", Cols: []*planpb.ColDef{{Name: "a", Typ: planpb.Type{Id: int32(types.T_int64)}}}}
	one, names, err := buildManifest(def, "shared", []objectio.ObjectStats{b, a})
	require.NoError(t, err)
	two, names2, err := buildManifest(def, "shared", []objectio.ObjectStats{a, b})
	require.NoError(t, err)
	require.True(t, bytes.Equal(one, two))
	require.Equal(t, names, names2)
	require.Less(t, names[0], names[1])
	require.Contains(t, string(one), `"total_rows":10`)
}

func objectStats(t *testing.T, rows uint32) objectio.ObjectStats {
	id := types.NewObjectid()
	s := objectio.NewObjectStatsWithObjectID(&id, false, true, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(s, rows))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(s, 1))
	return *s
}
