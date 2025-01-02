// Copyright 2021 Matrix Origin
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

package ckputil

import (
	"bytes"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func Test_ClusterKey1(t *testing.T) {
	packer := types.NewPacker()
	defer packer.Close()

	tableId := uint64(20)
	obj := types.NewObjectid()

	EncodeCluser(packer, tableId, obj)

	buf := packer.Bytes()
	packer.Reset()

	tuple, _, schemas, err := types.DecodeTuple(buf)
	require.NoError(t, err)
	require.Equalf(t, 2, len(schemas), "schemas: %v", schemas)
	require.Equalf(t, types.T_uint64, schemas[0], "schemas: %v", schemas)
	require.Equalf(t, types.T_Objectid, schemas[1], "schemas: %v", schemas)

	t.Log(tuple.SQLStrings(nil))
	require.Equal(t, tableId, tuple[0].(uint64))
	oid := tuple[1].(types.Objectid)
	require.True(t, obj.EQ(&oid))
}

func Test_ClusterKey2(t *testing.T) {
	packer := types.NewPacker()
	defer packer.Close()
	cnt := 5000
	clusters := make([][]byte, 0, cnt)
	objTemplate := types.NewObjectid()
	for i := cnt; i >= 1; i-- {
		obj := objTemplate.Copy(uint16(i))
		EncodeCluser(packer, 1, &obj)
		clusters = append(clusters, packer.Bytes())
		packer.Reset()
	}
	sort.Slice(clusters, func(i, j int) bool {
		return bytes.Compare(clusters[i], clusters[j]) < 0
	})

	last := uint16(0)
	for _, cluster := range clusters {
		tuple, _, _, err := types.DecodeTuple(cluster)
		require.NoError(t, err)
		require.Equalf(t, 2, len(tuple), "%v", tuple)
		require.Equal(t, uint64(1), tuple[0].(uint64))
		obj := tuple[1].(types.Objectid)
		curr := obj.Offset()
		require.Truef(t, curr > last, "%v,%v", curr, last)
		last = curr
	}
}
