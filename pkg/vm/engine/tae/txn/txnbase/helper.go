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
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

const (
	IDSize = 8 + types.UuidSize + types.BlockidSize + 4 + 2 + 1
)

func MarshalID(id *common.ID) []byte {
	var err error
	var w bytes.Buffer
	if err = binary.Write(&w, binary.BigEndian, id.TableID); err != nil {
		panic(err)
	}
	if _, err = w.Write(id.SegmentID[:]); err != nil {
		panic(err)
	}
	if _, err = w.Write(id.BlockID[:]); err != nil {
		panic(err)
	}
	if err = binary.Write(&w, binary.BigEndian, id.PartID); err != nil {
		panic(err)
	}
	if err = binary.Write(&w, binary.BigEndian, id.Idx); err != nil {
		panic(err)
	}
	if err = binary.Write(&w, binary.BigEndian, id.Iter); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func UnmarshalID(buf []byte) *common.ID {
	var err error
	r := bytes.NewBuffer(buf)
	id := common.ID{}
	if err = binary.Read(r, binary.BigEndian, &id.TableID); err != nil {
		panic(err)
	}
	if _, err = r.Read(id.SegmentID[:]); err != nil {
		panic(err)
	}
	if _, err = r.Read(id.BlockID[:]); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &id.PartID); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &id.Idx); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &id.Iter); err != nil {
		panic(err)
	}
	return &id
}
