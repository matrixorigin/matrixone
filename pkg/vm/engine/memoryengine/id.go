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

package memoryengine

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"math/bits"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type ID uint64

func (i ID) Less(than ID) bool {
	return i < than
}

func (i ID) IsEmpty() bool {
	return i == emptyID
}

var (
	emptyID ID
)

func (i ID) ToRowID() types.Rowid {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, i); err != nil {
		panic(err)
	}
	if buf.Len() > types.RowidSize {
		panic("id size too large")
	}
	var rowID types.Rowid
	copy(rowID[:], buf.Bytes())
	return rowID
}

type IDGenerator interface {
	NewID(context.Context) (ID, error)
}

type randomIDGenerator struct{}

var _ IDGenerator = new(randomIDGenerator)

func init() {
	var seed int64
	binary.Read(crand.Reader, binary.LittleEndian, &seed)
	rand.Seed(seed)
}

var idCounter int64

func (r *randomIDGenerator) NewID(_ context.Context) (ID, error) {
	ts := time.Now().Unix()
	n := bits.LeadingZeros(uint(ts))
	return ID(ts<<n + atomic.AddInt64(&idCounter, 1)%(1<<n)), nil
}

var RandomIDGenerator = new(randomIDGenerator)

type hakeeperIDGenerator interface {
	// both CNHAKeeperClient and DNHAKeeperClient has this method
	AllocateID(ctx context.Context) (uint64, error)
}

type HakeeperIDGenerator struct {
	generator hakeeperIDGenerator
}

func NewHakeeperIDGenerator(
	generator hakeeperIDGenerator,
) *HakeeperIDGenerator {
	return &HakeeperIDGenerator{
		generator: generator,
	}
}

var _ IDGenerator = new(HakeeperIDGenerator)

func (h *HakeeperIDGenerator) NewID(ctx context.Context) (ID, error) {
	id, err := h.generator.AllocateID(ctx)
	if err != nil {
		return 0, err
	}
	return ID(id), nil
}
