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

package moengine

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Relation = (*txnRelation)(nil)
)

func newRelation(h handle.Relation) *txnRelation {
	r := &txnRelation{}
	r.handle = h
	r.nodes = append(r.nodes, engine.Node{
		Addr: ADDR,
	})
	return r
}

func (rel *txnRelation) Write(ctx context.Context, bat *batch.Batch) error {
	taeBatch := containers.NewEmptyBatch()
	defer taeBatch.Close()
	for i, vec := range bat.Vecs {
		v := containers.NewVectorWithSharedMemory(vec)
		taeBatch.AddVector(bat.Attrs[i], v)
	}
	return rel.handle.Append(taeBatch)
}

func (rel *txnRelation) AddBlksWithMetaLoc(
	ctx context.Context,
	zm []objectio.ZoneMap,
	metaLocs []objectio.Location,
) error {
	return rel.handle.AddBlksWithMetaLoc(zm, metaLocs)
}

func (rel *txnRelation) Update(_ context.Context, data *batch.Batch) error {
	return moerr.NewNYINoCtx("Update not supported")
}

func (rel *txnRelation) DeleteByPhyAddrKeys(_ context.Context, keys *vector.Vector) error {
	tvec := containers.NewVectorWithSharedMemory(keys)
	defer tvec.Close()
	return rel.handle.DeleteByPhyAddrKeys(tvec)
}

func (rel *txnRelation) Delete(_ context.Context, bat *batch.Batch, col string) error {
	data := bat.Vecs[0]
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	logutil.Debugf("Delete col: %v", col)
	idx := catalog.GetAttrIdx(schema.AllNames(), col)
	if data.GetType().Oid == types.T_any {
		data.SetType(schema.ColDefs[idx].Type)
	}
	vec := containers.NewVectorWithSharedMemory(data)
	defer vec.Close()
	if schema.PhyAddrKey.Name == col {
		return rel.handle.DeleteByPhyAddrKeys(vec)
	}
	if !schema.HasPK() {
		panic(any("No valid primary key found"))
	}
	if schema.SortKey.Defs[0].Name == col {
		for i := 0; i < vec.Length(); i++ {
			filter := handle.NewEQFilter(vec.Get(i))
			err := rel.handle.DeleteByFilter(filter)
			if err != nil {
				return err
			}
		}
		return nil
	}
	panic(any("Key not found"))
}
