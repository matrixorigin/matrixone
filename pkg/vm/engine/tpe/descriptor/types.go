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

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"

type IndexDirectionType int
const (
	ASC  IndexDirectionType = 0
	DESC IndexDirectionType = 1
)

type IndexDesc struct {
	Name string `json:"name"`

	ID uint32 `json:"ID,string"`

	//unique index or not
	Is_unique bool `json:"is_unique"`

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
	Name string	`json:"name"`

	Direction IndexDirectionType `json:"direction"`

	//serial number of the attribute in the relation
	ID uint32 `json:"id,string"`

	Type orderedcodec.ValueType `json:"type,string"`
}