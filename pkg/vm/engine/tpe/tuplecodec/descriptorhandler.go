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

package tuplecodec

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
)

/*
Internal descriptor table for schema management.
Attribute  PrimaryKey
---------------------
parentID        Y
ID              Y
Name            N
DescriptorBytes N
 */

var _ descriptor.DescriptorHandler = &DescriptorHandlerImpl{}

type DescriptorHandlerImpl struct {
	codecHandler *TupleCodecHandler
	kvHandler    KVHandler
	kvLimit uint64
}

func NewDescriptorHandlerImpl(codec*TupleCodecHandler,
		kv KVHandler,
		limit uint64) *DescriptorHandlerImpl {
	return &DescriptorHandlerImpl{
		codecHandler: codec,
		kvHandler: kv,
		kvLimit: limit,
	}
}

func (dhi *DescriptorHandlerImpl) LoadRelationDescByName(parentID uint64, name string) (*descriptor.RelationDesc, error) {
	/*
	1,make prefix (tenantID,dbID)
	2,get keys with prefix
	3,decode the ID and the Name and find the desired name
	 */
	tke := dhi.codecHandler.GetEncoder()

	//make prefix
	var prefix TupleKey
	prefix,_ = tke.EncodeDatabasePrefix(prefix, parentID)

	//get keys with prefix
	prefixLen := len(prefix)
	for {
		_, _, err := dhi.kvHandler.GetWithPrefix(prefix,prefixLen,dhi.kvLimit)
		if err != nil {
			return nil, err
		}

		//decode the ID which is in the key
		//TODO:
		//decode the name which is in the value
		//TODO:
	}


	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) LoadRelationDescByID(tableID uint64) (*descriptor.RelationDesc, error) {
	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) StoreRelationDescByName(parentID uint64, name string, table *descriptor.RelationDesc) error {
	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) StoreRelationDescByID(tableID uint64, table *descriptor.RelationDesc) error {
	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) LoadDatabaseDescByName(parentID uint64, name string) (*descriptor.DatabaseDesc, error) {
	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) LoadDatabaseDescByID(dbID uint64) (*descriptor.DatabaseDesc, error) {
	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) StoreDatabaseDescByName(parentID uint64, name string, db *descriptor.DatabaseDesc) error {
	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) StoreDatabaseDescByID(dbID uint64, db *descriptor.DatabaseDesc) error {
	panic("implement me")
}
