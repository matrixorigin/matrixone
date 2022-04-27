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

package engine

import (
	"errors"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
	"strings"
)

var (
	errorInvalidKVType                        = errors.New("invalid kv type")
	errorTheInternalDatabaseHasExisted        = errors.New("the internal database has existed")
	errorTheInternalDescriptorTableHasExisted = errors.New("the internal descriptor table has existed")
	errorTheInternalAsyncGCTableHasExisted    = errors.New("the internal asyncgc table has existed")
	errorInvalidSerializerType                = errors.New("invalid serializer type")
	errorInvalidValueLayoutSerializerType     = errors.New("invalid value layout serializer type")
)

func NewTpeEngine(tc *TpeConfig) (*TpeEngine, error) {
	te := &TpeEngine{tpeConfig: tc}
	tch := tuplecodec.NewTupleCodecHandler(tuplecodec.SystemTenantID)
	kvLimit := tc.KVLimit
	var kv tuplecodec.KVHandler
	var err error
	if tc.KvType == tuplecodec.KV_MEMORY {
		kv = tuplecodec.NewMemoryKV()
	} else if tc.KvType == tuplecodec.KV_CUBE {
		kv, err = tuplecodec.NewCubeKV(tc.Cube, uint64(kvLimit),
			tc.TpeDedupSetBatchTimeout, tc.TpeDedupSetBatchTrycount,
			tc.TpeScanTimeout, tc.TpeScanTryCount)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errorInvalidKVType
	}

	var serial tuplecodec.ValueSerializer
	if tc.SerialType == tuplecodec.ST_JSON {
		serial = &tuplecodec.DefaultValueSerializer{}
	} else if tc.SerialType == tuplecodec.ST_CONCISE {
		serial = &tuplecodec.ConciseSerializer{}
	} else if tc.SerialType == tuplecodec.ST_FLAT {
		serial = &tuplecodec.FlatSerializer{}
	} else {
		return nil, errorInvalidSerializerType
	}

	var valueLayout tuplecodec.ValueLayoutSerializer

	if tc.ValueLayoutSerializerType == "default" {
		valueLayout = &tuplecodec.DefaultValueLayoutSerializer{
			Serializer: serial,
		}
	} else if tc.ValueLayoutSerializerType == "compact" {
		valueLayout = &tuplecodec.CompactValueLayoutSerializer{
			Serializer: serial,
		}
	} else {
		return nil, errorInvalidValueLayoutSerializerType
	}

	dh := tuplecodec.NewDescriptorHandlerImpl(tch, kv, serial, uint64(kvLimit))
	rcc := &tuplecodec.RowColumnConverterImpl{}
	ihi := tuplecodec.NewIndexHandlerImpl(tch, nil, kv, uint64(kvLimit), serial, valueLayout, rcc)
	ihi.PBKV = tc.PBKV
	epoch := tuplecodec.NewEpochHandler(tch, dh, kv)
	ch := tuplecodec.NewComputationHandlerImpl(dh, kv, tch, serial, ihi, epoch, tc.ParallelReader, tc.MultiNode)
	te.computeHandler = ch
	te.dh = dh
	return te, nil
}

func (te *TpeEngine) Delete(epoch uint64, name string) error {
	err := te.computeHandler.DropDatabase(epoch, name)
	if err != nil {
		return err
	}
	return nil
}

func (te *TpeEngine) Create(epoch uint64, name string, typ int) error {
	_, err := te.computeHandler.CreateDatabase(epoch, name, typ)
	if err != nil {
		return err
	}
	return err
}

func (te *TpeEngine) Databases() []string {
	var names []string
	if dbDescs, err := te.computeHandler.ListDatabases(); err == nil {
		for _, desc := range dbDescs {
			names = append(names, desc.Name)
		}
	}
	return names
}

func (te *TpeEngine) Database(name string) (engine.Database, error) {
	dbDesc, err := te.computeHandler.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	storeID := uint64(0)
	if te.tpeConfig.Cube != nil {
		storeID = te.tpeConfig.Cube.RaftStore().Meta().ID
	}
	return &TpeDatabase{
			id:             uint64(dbDesc.ID),
			desc:           dbDesc,
			computeHandler: te.computeHandler,
			storeID:        storeID,
		},
		nil
}

func (te *TpeEngine) Node(ip string) *engine.NodeInfo {
	var ni *engine.NodeInfo
	if te.tpeConfig.Cube != nil {
		te.tpeConfig.Cube.RaftStore().GetRouter().Every(uint64(pb.KVGroup), true, func(shard metapb.Shard, store metapb.Store) bool {
			if ni != nil {
				return false
			}
			if strings.HasPrefix(store.ClientAddress, ip) {
				stats := te.tpeConfig.Cube.RaftStore().GetRouter().GetStoreStats(store.ID)
				ni = &engine.NodeInfo{
					Mcpu: len(stats.GetCpuUsages()),
				}
			}
			return true
		})
	} else {
		return &engine.NodeInfo{
			Mcpu: 1,
		}
	}
	return ni
}

func (te *TpeEngine) RemoveDeletedTable(epoch uint64) error {
	_, err := te.computeHandler.RemoveDeletedTable(epoch)
	if err != nil {
		return err
	}
	return nil
}

//Bootstrap initializes the tpe
func (te *TpeEngine) Bootstrap() error {
	//create internal database 'system'
	_, err := te.dh.LoadDatabaseDescByID(tuplecodec.InternalDatabaseID)
	if err == nil { //it denotes the 'system' exists
		return errorTheInternalDatabaseHasExisted
	}

	//the database does not exist
	err = te.dh.StoreDatabaseDescByID(tuplecodec.InternalDatabaseID, tuplecodec.InternalDatabaseDesc)
	if err != nil {
		return err
	}

	//create internal table 'descriptor' for the 'system'
	_, err = te.dh.LoadRelationDescByID(tuplecodec.InternalDatabaseID, tuplecodec.InternalDescriptorTableID)
	if err == nil {
		return errorTheInternalDescriptorTableHasExisted
	}

	err = te.dh.StoreRelationDescByID(tuplecodec.InternalDatabaseID, tuplecodec.InternalDescriptorTableID, tuplecodec.InternalDescriptorTableDesc)
	if err != nil {
		return err
	}

	//create internal table 'asyncGC' for the 'system'
	_, err = te.dh.LoadRelationDescByID(tuplecodec.InternalDatabaseID, tuplecodec.InternalAsyncGCTableID)
	if err == nil {
		return errorTheInternalAsyncGCTableHasExisted
	}

	err = te.dh.StoreRelationDescByID(tuplecodec.InternalDatabaseID, tuplecodec.InternalAsyncGCTableID, tuplecodec.InternalAsyncGCTableDesc)
	if err != nil {
		return err
	}

	return nil
}

//Destroy delete the tpe
func (te *TpeEngine) Destroy() error {
	return nil
}

func (te *TpeEngine) Open() error {
	_ = te.Bootstrap()
	return nil
}

func (te *TpeEngine) Close() error {
	return nil
}
