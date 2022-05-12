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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
)

var (
	errorUnsupportedTableDef     = errors.New("unsupported tableDef")
	errorDuplicatePrimaryKeyName = errors.New("duplicate primary key name")
	errorDuplicateAttributeName  = errors.New("duplicate attribute name")
)

func (td *TpeDatabase) Relations() []string {
	tableDescs, err := td.computeHandler.ListTables(td.id)
	if err != nil {
		return nil
	}

	names := make([]string, 0, len(tableDescs))
	for _, desc := range tableDescs {
		names = append(names, desc.Name)
	}
	return names
}

func (td *TpeDatabase) Relation(name string) (engine.Relation, error) {
	tableDesc, err := td.computeHandler.GetTable(td.id, name)
	if err != nil {
		return nil, err
	}

	//load nodes for the table
	nodes, shardsHandler, err := td.computeHandler.GetNodesHoldTheTable(td.id, tableDesc)
	if err != nil {
		return nil, err
	}

	shards, ok := shardsHandler.(*tuplecodec.Shards)
	if !ok {
		return nil, tuplecodec.ErrorIsNotShards
	}

	logutil.Infof("cube_store_id %d", td.storeID)

	return &TpeRelation{
		id:             uint64(tableDesc.ID),
		dbDesc:         td.desc,
		desc:           tableDesc,
		computeHandler: td.computeHandler,
		nodes:          nodes,
		shards:         shards,
		storeID:        td.storeID,
	}, nil
}

func (td *TpeDatabase) Delete(epoch uint64, name string) error {
	_, err := td.computeHandler.DropTable(epoch, td.id, name)
	if err != nil {
		return err
	}
	return nil
}

func (td *TpeDatabase) Create(epoch uint64, name string, defs []engine.TableDef) error {
	//convert defs into desc
	tableDesc := &descriptor.RelationDesc{}

	tableDesc.Name = name

	//list primary key name
	var primaryKeys []string
	dedupPKs := make(map[string]int8)
	for _, def := range defs {
		if pk, ok := def.(*engine.PrimaryIndexDef); ok {
			for _, pkName := range pk.Names {
				if _, exist := dedupPKs[pkName]; exist {
					return errorDuplicatePrimaryKeyName
				} else {
					dedupPKs[pkName] = 1
				}
			}
			primaryKeys = append(primaryKeys, pk.Names...)
		}
		if cmt, ok := def.(*engine.CommentDef); ok {
			tableDesc.Comment = cmt.Comment
		}
	}

	pkDesc := &tableDesc.Primary_index
	pkDesc.ID = tuplecodec.PrimaryIndexID
	pkDesc.Name = "primary"
	pkDesc.Is_unique = true

	columnIdx := 0

	//tpe must need primary key
	if len(primaryKeys) == 0 {
		//add implicit primary key
		pkFieldName := "rowid" + fmt.Sprintf("%d", time.Now().Unix())
		pkAttrDesc := descriptor.AttributeDesc{
			ID:                uint32(columnIdx),
			Name:              pkFieldName,
			Ttype:             orderedcodec.VALUE_TYPE_UINT64,
			TypesType:         tuplecodec.TpeTypeToEngineType(orderedcodec.VALUE_TYPE_UINT64),
			Is_null:           false,
			Default_value:     "",
			Is_hidden:         true,
			Is_auto_increment: false,
			Is_unique:         true,
			Is_primarykey:     true,
			Comment:           "implicit primary key",
			References:        nil,
			Constrains:        nil,
		}

		tableDesc.Attributes = append(tableDesc.Attributes, pkAttrDesc)

		indexDesc := descriptor.IndexDesc_Attribute{
			Name:      pkAttrDesc.Name,
			Direction: 0,
			ID:        pkAttrDesc.ID,
			Type:      orderedcodec.VALUE_TYPE_UINT64,
			TypesType: tuplecodec.TpeTypeToEngineType(orderedcodec.VALUE_TYPE_UINT64),
		}

		pkDesc.Attributes = append(pkDesc.Attributes, indexDesc)

		columnIdx++
	}

	dedupAttrNames := make(map[string]int8)
	for _, def := range defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			//attribute has exists?
			if _, exist := dedupAttrNames[attr.Attr.Name]; exist {
				return errorDuplicateAttributeName
			} else {
				dedupAttrNames[attr.Attr.Name] = 1
			}

			var isPrimaryKey bool = false
			//the attribute is the primary key
			if _, exist := dedupPKs[attr.Attr.Name]; exist {
				isPrimaryKey = true
			}

			attrDesc := descriptor.AttributeDesc{
				ID:                uint32(columnIdx),
				Name:              attr.Attr.Name,
				Ttype:             tuplecodec.EngineTypeToTpeType(&attr.Attr.Type),
				TypesType:         attr.Attr.Type,
				Default:           attr.Attr.Default,
				Is_null:           !isPrimaryKey,
				Default_value:     "",
				Is_hidden:         false,
				Is_auto_increment: false,
				Is_unique:         isPrimaryKey,
				Is_primarykey:     isPrimaryKey,
				Comment:           "",
				References:        nil,
				Constrains:        nil,
			}

			tableDesc.Attributes = append(tableDesc.Attributes, attrDesc)

			if isPrimaryKey {
				indexDesc := descriptor.IndexDesc_Attribute{
					Name:      attr.Attr.Name,
					Direction: 0,
					ID:        uint32(columnIdx),
					Type:      tuplecodec.EngineTypeToTpeType(&attr.Attr.Type),
					TypesType: attr.Attr.Type,
				}
				pkDesc.Attributes = append(pkDesc.Attributes, indexDesc)
			}

			columnIdx++
		}
	}

	//create table
	_, err := td.computeHandler.CreateTable(epoch, td.id, tableDesc)
	if err != nil {
		return err
	}
	return err
}
