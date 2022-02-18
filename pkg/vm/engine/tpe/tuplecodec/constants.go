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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

const (
	SystemTenantID uint64 = 1

	//holding the schema of the descriptor table
	InternalDatabaseID uint64 = 0

	//holding the schema of the table
	InternalDescriptorTableID uint64 = 0

	PrimaryIndexID uint32 = 1
)

var (
	internalDescriptorTableDesc = &descriptor.RelationDesc{
		ID: uint32(InternalDescriptorTableID),
		Name:                    "descriptor",
		Update_time:             0,
		Next_attribute_id:       4,
		Attributes:              []descriptor.AttributeDesc{
			{
				ID: 0,
				Name: "parentID",
				Ttype: orderedcodec.VALUE_TYPE_UINT64,
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
				},
				{
					Name: "ID",
					ID:1,
					Type: orderedcodec.VALUE_TYPE_UINT64,
				},
			},
		},
		Next_index_id:           2,
		Indexes:                 nil,
		Create_sql:              "hard code",
		Create_time:             0,
		Drop_time:               0,
		Create_epoch:            0,
		Is_deleted:              false,
		Drop_epoch:              0,
		Max_access_epoch:        0,
		Table_options:           nil,
		Partition_options:       nil,
	}
)
