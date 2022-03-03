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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
)

var (
	errorInvalidKVType = errors.New("invalid kv type")
)

func NewTpeEngine(tc *TpeConfig) (*TpeEngine, error) {
	te := &TpeEngine{tpeConfig: tc}
	tch := tuplecodec.NewTupleCodecHandler(tuplecodec.SystemTenantID)
	var kv tuplecodec.KVHandler
	var err error
	if tc.KvType == tuplecodec.KV_MEMORY {
		kv = tuplecodec.NewMemoryKV()
	}else if tc.KvType == tuplecodec.KV_CUBE{
		kv, err = tuplecodec.NewCubeKV(tc.Cube)
		if err != nil {
			return nil, err
		}
	}else{
		return nil, errorInvalidKVType
	}

	serial := &tuplecodec.DefaultValueSerializer{}
	kvLimit := 10000
	dh := tuplecodec.NewDescriptorHandlerImpl(tch,kv,serial,uint64(kvLimit))
	rcc := &tuplecodec.RowColumnConverterImpl{}
	ihi := tuplecodec.NewIndexHandlerImpl(tch,nil,kv,uint64(kvLimit),serial,rcc)
	ch := tuplecodec.NewComputationHandlerImpl(dh, kv, tch, serial, ihi)
	te.computeHandler = ch
	return te, nil
}

func (te * TpeEngine) Delete(epoch uint64, name string) error {
	err := te.computeHandler.DropDatabase(epoch, name)
	if err != nil {
		return err
	}
	return nil
}

func (te * TpeEngine) Create(epoch uint64, name string, typ int) error {
	_, err := te.computeHandler.CreateDatabase(epoch,name,typ)
	if err != nil {
		return err
	}
	return err
}

func (te * TpeEngine) Databases() []string {
	var names []string
	if dbDescs,err := te.computeHandler.ListDatabases(); err == nil {
		for _, desc := range dbDescs {
			names = append(names,desc.Name)
		}
	}
	return names
}

func (te * TpeEngine) Database(name string) (engine.Database, error) {
	dbDesc, err := te.computeHandler.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	return &TpeDatabase{
		id: uint64(dbDesc.ID),
		desc: dbDesc,
		computeHandler: te.computeHandler,
	},
	nil
}

func (te * TpeEngine) Node(s string) *engine.NodeInfo {
	return &engine.NodeInfo{Mcpu: 1}
}