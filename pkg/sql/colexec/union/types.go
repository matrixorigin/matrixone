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

package union

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/encoding"
)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state int

	// hash table related.
	hashTable *hashmap.StrHashMap

	// bat records the final result of union operator.
	bat *batch.Batch
}

type Argument struct {
	//  attribute which need not do serialization work
	ctr     *container
	Ibucket uint64 // index in buckets
	Nbucket uint64 // buckets count
}

func (arg *Argument) MarshalBinary() ([]byte, error) {
	return encoding.Encode(&Argument{
		Ibucket: arg.Ibucket,
		Nbucket: arg.Nbucket,
	})
}

func (arg *Argument) UnmarshalBinary(data []byte) error {
	rs := new(Argument)
	if err := encoding.Decode(data, rs); err != nil {
		return err
	}
	arg.Ibucket = rs.Ibucket
	arg.Nbucket = rs.Nbucket
	return nil
}