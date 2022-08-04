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

package minus

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/encoding"
)

const (
	buildingHashMap = iota
	probingHashMap
	operatorEnd
)

type Argument struct {
	ctr container

	// hash table bucket related information.
	IBucket, NBucket uint64
}

func (arg *Argument) MarshalBinary() ([]byte, error) {
	return encoding.Encode(&Argument{
		IBucket: arg.IBucket,
		NBucket: arg.NBucket,
	})
}

func (arg *Argument) UnmarshalBinary(data []byte) error {
	rs := new(Argument)
	if err := encoding.Decode(data, rs); err != nil {
		return err
	}
	arg.IBucket = rs.IBucket
	arg.NBucket = rs.NBucket
	return nil
}

type container struct {
	// operator execution stage.
	state int

	// hash table related
	hashTable *hashmap.StrHashMap

	// result batch of minus column execute operator
	bat *batch.Batch
}
