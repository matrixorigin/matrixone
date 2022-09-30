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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

func BatchToProtoBatch(bat *Batch) (*api.Batch, error) {
	rbat := new(api.Batch)
	rbat.Attrs = bat.Attrs
	for _, vec := range bat.Vecs {
		pbVector, err := vector.VectorToProtoVector(vec)
		if err != nil {
			return nil, err
		}
		rbat.Vecs = append(rbat.Vecs, pbVector)
	}
	return rbat, nil

}

func ProtoBatchToBatch(bat *api.Batch) (*Batch, error) {
	rbat := NewWithSize(len(bat.Attrs))
	rbat.Attrs = append(rbat.Attrs, bat.Attrs...)
	for i, v := range bat.Vecs {
		vec, err := vector.ProtoVectorToVector(v)
		if err != nil {
			return nil, err
		}
		rbat.SetVector(int32(i), vec)
	}
	rbat.InitZsOne(rbat.GetVector(0).Length())
	return rbat, nil
}
