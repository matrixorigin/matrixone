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

package batch

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

// BatchToProtoBatch converts bat to its protobuf form. attrs names bat's
// columns on the wire: a batch carries no names of its own.
func BatchToProtoBatch(bat *Batch, attrs []string) (*api.Batch, error) {
	rbat := new(api.Batch)
	rbat.Attrs = attrs
	if len(bat.Vecs) > 0 {
		rbat.Vecs = make([]api.Vector, len(bat.Vecs))
	}
	for i, vec := range bat.Vecs {
		pbVector, err := vector.VectorToProtoVector(vec)
		if err != nil {
			return nil, err
		}
		rbat.Vecs[i] = pbVector
	}
	return rbat, nil

}

// ProtoBatchToBatch copies bat into vectors of mp. With a nil mp the vectors
// are read-only references to bat's buffers.
func ProtoBatchToBatch(bat *api.Batch, mp *mpool.MPool) (*Batch, error) {
	rbat := NewWithSize(len(bat.Vecs))
	for i, v := range bat.Vecs {
		vec, err := vector.ProtoVectorToVector(v, mp)
		if err != nil {
			return nil, err
		}
		rbat.SetVector(int32(i), vec)
	}
	rbat.SetRowCount(rbat.GetVector(0).Length())
	return rbat, nil
}
