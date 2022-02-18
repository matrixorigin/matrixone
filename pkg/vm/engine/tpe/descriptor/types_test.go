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
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
	"time"
)

func TestDescriptors(t *testing.T) {
	convey.Convey("database desc serialization",t, func() {
		desc  := DatabaseDesc{
			ID:           10,
			Name:         "T",
			Update_time: time.Now().Unix(),
			Create_epoch: 10,
			Is_deleted:   false,
			Drop_epoch:   100,
		}

		marshal, err := json.Marshal(desc)
		convey.So(err,convey.ShouldBeNil)

		var want DatabaseDesc
		err = json.Unmarshal(marshal,&want)
		convey.So(err,convey.ShouldBeNil)

		convey.So(reflect.DeepEqual(desc,want),convey.ShouldBeTrue)
	})

	convey.Convey("relation desc serialization",t, func() {
		attrs :=[]AttributeDesc{
			{
				ID:                0,
				Name:              "a",
				Ttype:             orderedcodec.VALUE_TYPE_STRING,
				Is_null:           false,
				Default_value:     "",
				Is_hidden:         false,
				Is_auto_increment: false,
				Is_unique:         false,
				Is_primarykey:     false,
				Comment:           "field a",
				References:        []ReferenceDesc{
					{
						Table_name: "B",
						Key_parts:  []ReferenceDesc_KeyPart{
							{
								Column_name: "a",
								Length:      10,
								Direction:   int(DESC),
							},
						},
					},
					{
						Table_name: "C",
						Key_parts:  []ReferenceDesc_KeyPart{
							{
								Column_name: "a",
								Length:      10,
								Direction:   int(ASC),
							},
						},
					},
				},
				Constrains:        []ConstrainDesc{
					{"abc"},
					{"cde"},
				},
			},
			{
				ID:                1,
				Name:              "b",
				Ttype:             orderedcodec.VALUE_TYPE_BYTES,
				Is_null:           true,
				Default_value:     "",
				Is_hidden:         false,
				Is_auto_increment: false,
				Is_unique:         false,
				Is_primarykey:     false,
				Comment:           "field a",
				References:        []ReferenceDesc{
					{
						Table_name: "B",
						Key_parts:  []ReferenceDesc_KeyPart{
							{
								Column_name: "a",
								Length:      10,
								Direction:   int(DESC),
							},
						},
					},
					{
						Table_name: "C",
						Key_parts:  []ReferenceDesc_KeyPart{
							{
								Column_name: "a",
								Length:      10,
								Direction:   int(ASC),
							},
						},
					},
				},
				Constrains:        []ConstrainDesc{
					{"abc"},
					{"cde"},
				},
			},
			{
				ID:                2,
				Name:              "c",
				Ttype:             orderedcodec.VALUE_TYPE_UINT64,
				Is_null:           false,
				Default_value:     "",
				Is_hidden:         true,
				Is_auto_increment: false,
				Is_unique:         false,
				Is_primarykey:     false,
				Comment:           "field a",
				References:        []ReferenceDesc{
					{
						Table_name: "B",
						Key_parts:  []ReferenceDesc_KeyPart{
							{
								Column_name: "a",
								Length:      10,
								Direction:   int(DESC),
							},
						},
					},
					{
						Table_name: "C",
						Key_parts:  []ReferenceDesc_KeyPart{
							{
								Column_name: "a",
								Length:      10,
								Direction:   int(ASC),
							},
						},
					},
				},
				Constrains:        []ConstrainDesc{
					{"abc"},
					{"cde"},
				},
			},
			{
				ID:                3,
				Name:              "d",
				Ttype:             orderedcodec.VALUE_TYPE_STRING,
				Is_null:           true,
				Default_value:     "",
				Is_hidden:         false,
				Is_auto_increment: false,
				Is_unique:         false,
				Is_primarykey:     true,
				Comment:           "field a",
				References:        []ReferenceDesc{
					{
						Table_name: "B",
						Key_parts:  []ReferenceDesc_KeyPart{
							{
								Column_name: "a",
								Length:      10,
								Direction:   int(DESC),
							},
						},
					},
					{
						Table_name: "C",
						Key_parts:  []ReferenceDesc_KeyPart{
							{
								Column_name: "a",
								Length:      10,
								Direction:   int(ASC),
							},
						},
					},
				},
				Constrains:        []ConstrainDesc{
					{"abc"},
					{"cde"},
				},
			},
		}

		indexAttrs := []IndexDesc_Attribute{
			{
				Name:      "a",
				Direction: ASC,
				ID:        0,
				Type:      orderedcodec.VALUE_TYPE_STRING,
			},
			{
				Name:      "b",
				Direction: ASC,
				ID:        1,
				Type:      orderedcodec.VALUE_TYPE_BYTES,
			},
		}
		desc  := RelationDesc{
			ID:                      0xff,
			Name:                    "A",
			Update_time:             time.Now().Unix(),
			Next_attribute_id:       3,
			Attributes:              attrs,
			IDependsOnRelations:     nil,
			RelationsDependsOnMe:    nil,
			Next_attribute_group_id: 0,
			AttributeGroups:         nil,
			Primary_index:           IndexDesc{
				Name:                 "primay",
				ID:                   0,
				Is_unique:            true,
				Attributes:           indexAttrs,
				Impilict_attributes:  nil,
				Composite_attributes: nil,
				Store_attributes:     nil,
				Key_encoding_type:    0,
				Value_encoding_type:  0,
			},
			Next_index_id:           2,
			Indexes:                 nil,
			Create_sql:              "create table A(a varchar,b uint64,c uint64,d varchar)",
			Create_time:             time.Now().Unix(),
			Drop_time:               time.Now().Unix(),
			Create_epoch:            1000,
			Is_deleted:              true,
			Drop_epoch:              1001,
			Max_access_epoch:        1002,
			Table_options:           nil,
			Partition_options:       nil,
		}

		marshal, err := json.Marshal(desc)
		convey.So(err,convey.ShouldBeNil)

		var want RelationDesc
		err = json.Unmarshal(marshal,&want)
		convey.So(err,convey.ShouldBeNil)

		convey.So(reflect.DeepEqual(desc,want),convey.ShouldBeTrue)
	})
}