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

package intersectall

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
)

type container struct {
	// operator state: Build, Probe or End
	state int

	// helper data structure during probe
	counter []uint64

	// process mark
	inBuckets []uint8

	// built for the smaller of the two relations
	hashTable *hashmap.StrHashMap

	inserted      []uint8
	resetInserted []uint8
}

type Argument struct {
	// execution container
	ctr *container
	// index in buckets
	IBucket uint64
	// buckets count
	NBucket uint64
}
