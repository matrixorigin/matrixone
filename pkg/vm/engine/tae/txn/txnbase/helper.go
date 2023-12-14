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

package txnbase

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

const (
	IDSize = 8 + types.UuidSize + types.BlockidSize + 4 + 2 + 1
)

func MarshalID(id *common.ID) []byte {
	var err error
	var w bytes.Buffer
	_, err = w.Write(common.EncodeID(id))
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func UnmarshalID(buf []byte) *common.ID {
	var err error
	r := bytes.NewBuffer(buf)
	id := common.ID{}
	_, err = r.Read(common.EncodeID(&id))
	if err != nil {
		panic(err)
	}
	return &id
}
