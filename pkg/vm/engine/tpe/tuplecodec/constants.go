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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
	"math"
)

const (
	SystemTenantID uint64 = 1

	//holding the schema of the descriptor table
	InternalDatabaseID uint64 = 0

	//holding the schema of the table
	InternalDescriptorTableID uint64 = 0

	InternalDescriptorTable_parentID_ID        = 0
	InternalDescriptorTable_id_ID              = 1
	InternalDescriptorTable_name_ID            = 2
	InternalDescriptorTable_desc_ID            = 3
	PrimaryIndexID                      uint32 = 1

	//holding the epochgced table
	InternalAsyncGCTableID uint64 = 1
	InternalAsyncGCTable_epoch_ID = 0
	InternalAsyncGCTable_dbID_ID = 1
	InternalAsyncGCTable_tableID_ID = 2
	InternalAsyncGCTable_desc_ID = 3

	//user table id offset
	UserTableIDOffset uint64 = 3
)

func toTypesType(t types.T) types.Type {
	return t.ToType()
}

var (
	InternalDatabaseDesc = &descriptor.DatabaseDesc{
		ID:           uint32(InternalDatabaseID),
		Name:         "system",
		Update_time:  0,
		Create_epoch: math.MaxUint64,
		Is_deleted:   false,
		Drop_epoch:   0,
	}
	InternalDescriptorTableDesc = &descriptor.RelationDesc{
		ID: uint32(InternalDescriptorTableID),
		Name:                    "descriptor",
		Update_time:             0,
		Next_attribute_id:       4,
		Attributes:              []descriptor.AttributeDesc{
			{
				ID: 0,
				Name: "parentID",
				Ttype: orderedcodec.VALUE_TYPE_UINT64,
				TypesType: toTypesType(types.T_uint64),
				Is_null: false,
				Is_hidden: false,
				Is_auto_increment: false,
				Is_unique: false,
				Is_primarykey: true,
				Comment: "the parent ID of the table and database",
			},
			{
				ID: 1,
				Name: "ID",
				Ttype: orderedcodec.VALUE_TYPE_UINT64,
				TypesType: toTypesType(types.T_uint64),
				Is_null: false,
				Is_hidden: false,
				Is_auto_increment: false,
				Is_unique: false,
				Is_primarykey: true,
				Comment: "the ID of the table and the database",
			},
			{
				ID: 2,
				Name: "Name",
				Ttype: orderedcodec.VALUE_TYPE_STRING,
				TypesType: toTypesType(types.T_varchar),
				Is_null: false,
				Is_hidden: false,
				Is_auto_increment: false,
				Is_unique: false,
				Is_primarykey: false,
				Comment: "the name of the table and the database",
			},
			{
				ID: 3,
				Name: "desc",
				Ttype: orderedcodec.VALUE_TYPE_BYTES,
				TypesType: toTypesType(types.T_varchar),
				Is_null: false,
				Is_hidden: false,
				Is_auto_increment: false,
				Is_unique: false,
				Is_primarykey: false,
				Comment: "the serialized bytes of the descriptor of the table and the database",
			},
		},
		IDependsOnRelations:     nil,
		RelationsDependsOnMe:    nil,
		Next_attribute_group_id: 0,
		AttributeGroups:         nil,
		Primary_index:           descriptor.IndexDesc{
			Name: "primary",
			ID: PrimaryIndexID,
			Is_unique: true,
			Attributes: []descriptor.IndexDesc_Attribute{
				{
					Name: "parentID",
					ID:0,
					Type: orderedcodec.VALUE_TYPE_UINT64,
					TypesType: toTypesType(types.T_uint64),
				},
				{
					Name: "ID",
					ID:1,
					Type: orderedcodec.VALUE_TYPE_UINT64,
					TypesType: toTypesType(types.T_uint64),
				},
			},
		},
		Next_index_id:           2,
		Indexes:                 nil,
		Create_sql:              "hard code",
		Create_time:             0,
		Drop_time:               0,
		Create_epoch:            math.MaxUint64,
		Is_deleted:              false,
		Drop_epoch:              0,
		Max_access_epoch:        0,
		Table_options:           nil,
		Partition_options:       nil,
	}

	InternalAsyncGCTableDesc = &descriptor.RelationDesc{
		ID:                      uint32(InternalAsyncGCTableID),
		Name:                    "asyncgc",
		Update_time:             0,
		Next_attribute_id:       0,
		Attributes:              []descriptor.AttributeDesc{
			{
				ID:                0,
				Name:              "epoch",
				Ttype:             orderedcodec.VALUE_TYPE_UINT64,
				TypesType: toTypesType(types.T_uint64),
				Is_null:           false,
				Default_value:     "",
				Is_hidden:         false,
				Is_auto_increment: false,
				Is_unique:         false,
				Is_primarykey:     true,
				Comment:           "epoch when the delete happens",
				References:        nil,
				Constrains:        nil,
			},
			{
				ID:                1,
				Name:              "dbID",
				Ttype:             orderedcodec.VALUE_TYPE_UINT64,
				TypesType: toTypesType(types.T_uint64),
				Is_null:           false,
				Default_value:     "",
				Is_hidden:         false,
				Is_auto_increment: false,
				Is_unique:         false,
				Is_primarykey:     true,
				Comment:           "the dbID that the table belongs to",
				References:        nil,
				Constrains:        nil,
			},
			{
				ID:                2,
				Name:              "tableID",
				Ttype:             orderedcodec.VALUE_TYPE_UINT64,
				TypesType: toTypesType(types.T_uint64),
				Is_null:           false,
				Default_value:     "the tableID to be dropped",
				Is_hidden:         false,
				Is_auto_increment: false,
				Is_unique:         false,
				Is_primarykey:     true,
				Comment:           "",
				References:        nil,
				Constrains:        nil,
			},
			{
				ID:                3,
				Name:              "desc",
				Ttype:             orderedcodec.VALUE_TYPE_BYTES,
				TypesType: toTypesType(types.T_varchar),
				Default:           engine.DefaultExpr{},
				Is_null:           false,
				Default_value:     "",
				Is_hidden:         false,
				Is_auto_increment: false,
				Is_unique:         false,
				Is_primarykey:     false,
				Comment:           "the descriptor of the table dropped",
				References:        nil,
				Constrains:        nil,
			},
		},
		IDependsOnRelations:     nil,
		RelationsDependsOnMe:    nil,
		Next_attribute_group_id: 0,
		AttributeGroups:         nil,
		Primary_index:           descriptor.IndexDesc{
			Name:                 "primary",
			ID:                   PrimaryIndexID,
			Is_unique:            true,
			Attributes:           []descriptor.IndexDesc_Attribute{
				{
					Name:      "epoch",
					ID:        0,
					Type:      orderedcodec.VALUE_TYPE_UINT64,
					TypesType: toTypesType(types.T_uint64),
				},
				{
					Name:      "dbID",
					ID:        1,
					Type:      orderedcodec.VALUE_TYPE_UINT64,
					TypesType: toTypesType(types.T_uint64),
				},
				{
					Name:      "tableID",
					ID:        2,
					Type:      orderedcodec.VALUE_TYPE_UINT64,
					TypesType: toTypesType(types.T_uint64),
				},
			},
			Impilict_attributes:  nil,
			Composite_attributes: nil,
			Store_attributes:     nil,
			Key_encoding_type:    0,
			Value_encoding_type:  0,
		},
		Next_index_id:           0,
		Indexes:                 nil,
		Create_sql:              "",
		Create_time:             0,
		Drop_time:               0,
		Create_epoch:            math.MaxUint64,
		Is_deleted:              false,
		Drop_epoch:              0,
		Max_access_epoch:        0,
		Table_options:           nil,
		Partition_options:       nil,
	}
)
