// Copyright 2022 Matrix Origin
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

package txnengine

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
)

type ID int64

func init() {
	var seed int64
	binary.Read(crand.Reader, binary.LittleEndian, &seed)
	rand.Seed(seed)
}

func NewID() (id ID) {
	//TODO will use an id generate service
	id = ID(rand.Int63())
	return
}

func (i ID) Less(than ID) bool {
	return i < than
}

func (i ID) IsEmpty() bool {
	return i == emptyID
}

var (
	emptyID ID
)
