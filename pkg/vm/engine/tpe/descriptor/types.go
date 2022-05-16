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

package descriptor

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

type DatabaseDesc struct {
	ID uint32 `json:"ID,string"`

	Name string `json:"name"`

	Update_time int64 `json:"update_time"`

	Create_epoch uint64 `json:"create_epoch,string"`

	Is_deleted bool `json:"is_deleted,string"`

	Drop_epoch uint64 `json:"drop_epoch,string"`

	Max_access_epoch uint64 `json:"max_access_epoch,string"`

	Typ int `json:"typ,string"`
}

type RelationDesc struct {
	ID uint32 `json:"ID,string"`

	Name string `json:"name"`

	Comment string `json:"comment"`

	Update_time int64 `json:"update_time"`

	Next_attribute_id uint32 `json:"next_attribute_id,string"`

	Attributes []AttributeDesc `json:"attributes"`

	IDependsOnRelations []uint32 `json:"i_depends_on_relations,string"`

	RelationsDependsOnMe []DependsOnDesc `json:"relations_depends_on_me"`

	Next_attribute_group_id uint32 `json:"next_attribute_group_id,string"`

	AttributeGroups []AttributeGroupDesc `json:"attribute_groups"`

	Primary_index IndexDesc `json:"primary_index"`

	Next_index_id uint32 `json:"next_index_id,string"`

	Indexes []IndexDesc `json:"indexes"`

	Create_sql string `json:"create_sql"`

	Create_time int64 `json:"create_time"`

	Drop_time int64 `json:"drop_time"`

	Create_epoch uint64 `json:"create_epoch,string"`

	Is_deleted bool `json:"is_deleted,string"`

	Drop_epoch uint64 `json:"drop_epoch,string"`

	//for epochgc
	Max_access_epoch uint64 `json:"max_access_epoch,string"`

	Table_options []TableOptionDesc `json:"table_options"`

	Partition_options []PartitionOptionDesc `json:"partition_options"`
}

type TableOptionDesc struct {
	Kind int `json:"kind,string"`

	Value string `json:"value"`

	Name string `json:"name"`
}

type PartitionOptionDesc struct {
	Algo string `json:"algo"`
}

type DependsOnDesc struct {
	ID uint32 `json:"ID,string"`

	AttributeID []uint32 `json:"attribute_id,string"`
}

type DefaultValue struct {
	ValueType string
}

type AttributeDesc struct {
	ID uint32 `json:"id,string"`

	Name string `json:"name"`

	Ttype orderedcodec.ValueType `json:"type,string"`

	TypesType types.Type `json:"types_type,string"`

	Default engine.DefaultExpr `json:"default"`

	DefaultVal DefaultValue `json:"defaultValue"`

	Is_null bool `json:"is_null,string"`

	Default_value string `json:"default_value"`

	Is_hidden bool `json:"is_hidden,string"`

	Is_auto_increment bool `json:"is_auto_increment,string"`

	Is_unique bool `json:"is_unique,string"`

	Is_primarykey bool `json:"is_primarykey,string"`

	Comment string `json:"comment"`

	References []ReferenceDesc `json:"references"`

	Constrains []ConstrainDesc `json:"constrains"`
}

type ReferenceDesc struct {
	Table_name string `json:"table_name"`

	Key_parts []ReferenceDesc_KeyPart `json:"key_parts"`
}

type ReferenceDesc_KeyPart struct {
	Column_name string `json:"column_name"`

	Length int `json:"length,string"`

	Direction int `json:"direction,string"`

	//expr ast
}

type ConstrainDesc struct {
	Symbol string `json:"symbol"`

	//Expr ast
}

type AttributeGroupDesc struct {
	ID uint32 `json:"id,string"`

	Attributes []AttributeGroupDesc_Attribute `json:"attributes"`
}

type AttributeGroupDesc_Attribute struct {
	Name string `json:"name"`

	//serial number of the attribute in the relation
	ID uint32 `json:"id,string"`

	Type orderedcodec.ValueType `json:"type,string"`
}

type IndexDirectionType int

const (
	ASC  IndexDirectionType = 0
	DESC IndexDirectionType = 1
)

type IndexDesc struct {
	Name string `json:"name"`

	ID uint32 `json:"ID,string"`

	//unique index or not
	Is_unique bool `json:"is_unique,string"`

	//components of the index
	Attributes []IndexDesc_Attribute `json:"attributes"`

	//components of the secondary index.
	//from the primary index for ensuring the unique.
	Impilict_attributes []IndexDesc_Attribute `json:"impilict_attributes"`

	//for compositing encoding, it is put into the value.
	Composite_attributes []IndexDesc_Attribute `json:"composite_attributes"`

	//for storing attributes in the value.
	Store_attributes []IndexDesc_Attribute `json:"store_attributes"`

	//the type of encoding key
	Key_encoding_type int `json:"key_encoding_type,string"`

	//the type of encoding value
	Value_encoding_type int `json:"value_encoding_type,string"`
}

type IndexDesc_Attribute struct {
	Name string `json:"name"`

	Direction IndexDirectionType `json:"direction"`

	//serial number of the attribute in the relation
	ID uint32 `json:"id,string"`

	Type orderedcodec.ValueType `json:"type,string"`

	TypesType types.Type `json:"types_type,string"`
}

func ExtractIndexAttributeIDs(attrs []IndexDesc_Attribute) map[uint32]int {
	ids := make(map[uint32]int)
	for posInIndex, attr := range attrs {
		ids[attr.ID] = posInIndex
	}
	return ids
}

func ExtractIndexAttributeDescIDs(attrs []*AttributeDesc) []int {
	ids := make([]int, 0, len(attrs))
	for _, attr := range attrs {
		ids = append(ids, int(attr.ID))
	}
	return ids
}

type EpochGCItem struct {
	Epoch   uint64
	DbID    uint64
	TableID uint64
}

// DescriptorHandler loads and updates the descriptors
type DescriptorHandler interface {

	//LoadRelationDescByName gets the descriptor of the table by the name of the table
	LoadRelationDescByName(parentID uint64, name string) (*RelationDesc, error)

	//LoadRelationDescByID gets the descriptor of the table by the tableid
	LoadRelationDescByID(parentID uint64, tableID uint64) (*RelationDesc, error)

	//StoreRelationDescByName first gets the descriptor of the table by name, then save the descriptor.
	StoreRelationDescByName(parentID uint64, name string, tableDesc *RelationDesc) error

	//StoreRelationDescByID saves the descriptor
	StoreRelationDescByID(parentID uint64, tableID uint64, table *RelationDesc) error

	//DeleteRelationDescByID deletes the table
	DeleteRelationDescByID(parentID uint64, tableID uint64) error

	LoadDatabaseDescByName(name string) (*DatabaseDesc, error)

	LoadDatabaseDescByID(dbID uint64) (*DatabaseDesc, error)

	StoreDatabaseDescByName(name string, db *DatabaseDesc) error

	StoreDatabaseDescByID(dbID uint64, db *DatabaseDesc) error

	//DeleteDatabaseDescByID deletes the database
	DeleteDatabaseDescByID(dbID uint64) error

	MakePrefixWithOneExtraID(dbID uint64, tableID uint64, indexID uint64, extraID uint64) ([]byte, *orderedcodec.EncodedItem)

	GetValuesWithPrefix(parentID uint64, callbackCtx interface{}, callback func(callbackCtx interface{}, dis []*orderedcodec.DecodedItem) ([]byte, error)) ([]byte, error)

	//StoreRelationDescIntoAsyncGC stores the table into the asyncgc table
	StoreRelationDescIntoAsyncGC(epoch uint64, dbID uint64, desc *RelationDesc) error

	//ListRelationDescFromAsyncGC gets all the tables need to deleted from the asyncgc table
	ListRelationDescFromAsyncGC(epoch uint64) ([]EpochGCItem, error)
}
