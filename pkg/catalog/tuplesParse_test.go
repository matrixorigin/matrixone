// Copyright 2024 Matrix Origin
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

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func mustToPBBatch(bat *batch.Batch) *api.Batch {
	rbat := new(api.Batch)
	rbat.Attrs = bat.Attrs
	for _, vec := range bat.Vecs {
		pbVector, err := vector.VectorToProtoVector(vec)
		if err != nil {
			panic(err)
		}
		rbat.Vecs = append(rbat.Vecs, pbVector)
	}
	return rbat
}

// improve test coverage
func TestParseError(t *testing.T) {
	m := mpool.MustNew("test")
	packer := types.NewPacker()
	defer packer.Close()

	{
		bat1, _ := GenCreateTableTuple(Table{TableId: 10001}, m, packer)
		bat2, _ := GenCreateTableTuple(Table{TableId: 10002}, m, packer)
		require.NoError(t, bat1.UnionOne(bat2, 0, m))

		e := &api.Entry{
			EntryType:  api.Entry_Alter,
			DatabaseId: MO_CATALOG_ID,
			TableId:    MO_TABLES_ID,
			Bat:        mustToPBBatch(bat1),
		}
		_, _, err := ParseEntryList([]*api.Entry{e})
		t.Log(err)
		require.Error(t, err) // bad mo_tables entry type '3'

		e.TableName = "alter"
		_, _, err = ParseEntryList([]*api.Entry{e})
		require.Error(t, err) // bad alter table batch size (2 rows)
	}

	{
		bat2, _ := GenCreateColumnTuples([]Column{
			{
				DatabaseId: 2000,
				TableId:    10001,
				Name:       "col1",
			},
		}, m, packer)

		e2 := &api.Entry{
			EntryType:  api.Entry_Insert,
			DatabaseId: MO_CATALOG_ID,
			TableId:    MO_COLUMNS_ID,
			Bat:        mustToPBBatch(bat2),
		}

		_, _, err := ParseEntryList([]*api.Entry{e2})
		require.Error(t, err) // dangling column entry
	}

	{
		bat1, _ := GenCreateTableTuple(Table{TableId: 10001}, m, packer)
		e1 := &api.Entry{
			EntryType:  api.Entry_Insert,
			DatabaseId: MO_CATALOG_ID,
			TableId:    MO_TABLES_ID,
			Bat:        mustToPBBatch(bat1),
		}
		typ := types.T_int64.ToType()
		typBytes, _ := types.Encode(&typ)
		bat2, _ := GenCreateColumnTuples([]Column{
			{
				TableId: 10001,
				Name:    "col1",
				Typ:     typBytes,
			},
			{
				TableId: 10001,
				Name:    "col2",
				Typ:     typBytes,
			},
		}, m, packer)

		e2 := &api.Entry{
			EntryType:  api.Entry_Insert,
			DatabaseId: MO_CATALOG_ID,
			TableId:    100000, // bad target table id
			Bat:        mustToPBBatch(bat2),
		}
		_, _, err := ParseEntryList([]*api.Entry{e1, e2})
		require.Error(t, err) // mismatched column batch

		bat3, _ := GenCreateColumnTuples([]Column{
			{
				TableId: 10002, // bad target table id
				Name:    "col3",
				Typ:     typBytes,
			},
		}, m, packer)
		require.NoError(t, bat2.UnionOne(bat3, 0, m))

		e3 := &api.Entry{
			EntryType:  api.Entry_Insert,
			DatabaseId: MO_CATALOG_ID,
			TableId:    MO_COLUMNS_ID,
			Bat:        mustToPBBatch(bat2),
		}
		_, _, err = ParseEntryList([]*api.Entry{e1, e3})
		t.Log(err)
		require.Error(t, err) // mismatched column id
	}
}

func TestShowReqs(t *testing.T) {
	reqs := []any{
		&CreateDatabaseReq{Cmds: []CreateDatabase{{DatabaseId: 10000, Name: "db0"}}},
		&DropDatabaseReq{Cmds: []DropDatabase{{Id: 10000, Name: "db9"}}},
		&CreateTableReq{Cmds: []CreateTable{{TableId: 10001, DatabaseId: 10000, Name: "tbl0"}}},
		&DropTableReq{Cmds: []DropTable{{Id: 10001, DatabaseId: 10000, Name: "tbl9"}}},
		&api.AlterTableReq{DbId: 100, TableId: 101, Kind: api.AlterKind_UpdateComment},
		"write req",
	}
	t.Log(ShowReqs(reqs))
}
