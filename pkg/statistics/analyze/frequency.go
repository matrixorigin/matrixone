// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyze

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	CountMinAlgorithmV1  = "COUNT_MIN_MCV_V1"
	ReservoirAlgorithmV1 = "UNIFORM_PRIORITY_RESERVOIR_V1"
	DefaultCountMinDepth = 5
	DefaultCountMinWidth = 16_384
	DefaultReservoirSize = 8_192
	MaxMaterializedValue = 4 << 10

	domainCountMin  = "analyze/mcv-count-min/v1"
	domainReservoir = "analyze/occurrence-reservoir/v1"
)

var (
	ErrFrequencyConfig   = moerr.NewInvalidInputNoCtx("analyze: invalid frequency collector configuration")
	ErrFrequencyOverflow = moerr.NewInternalErrorNoCtx("analyze: frequency counter overflow")
	ErrReservoirState    = moerr.NewInvalidStateNoCtx("analyze: incompatible occurrence reservoir item")
)

type CountMin struct {
	seed   [32]byte
	column uint32
	depth  uint32
	width  uint32
	count  uint64
	rows   [][]uint64
}

func NewCountMin(seed [32]byte, column, depth, width uint32) (*CountMin, error) {
	if depth == 0 || width == 0 || uint64(depth)*uint64(width) > uint64(math.MaxInt) {
		return nil, ErrFrequencyConfig
	}
	rows := make([][]uint64, depth)
	for i := range rows {
		rows[i] = make([]uint64, width)
	}
	return &CountMin{seed: seed, column: column, depth: depth, width: width, rows: rows}, nil
}

func NewDefaultCountMin(seed [32]byte, column uint32) *CountMin {
	result, err := NewCountMin(seed, column, DefaultCountMinDepth, DefaultCountMinWidth)
	if err != nil {
		panic(err)
	}
	return result
}

func (c *CountMin) Add(value ValueHash, occurrences uint64) error {
	if c == nil || occurrences == 0 {
		return nil
	}
	if math.MaxUint64-c.count < occurrences {
		return ErrFrequencyOverflow
	}
	for row := uint32(0); row < c.depth; row++ {
		index := c.index(row, value)
		if math.MaxUint64-c.rows[row][index] < occurrences {
			return ErrFrequencyOverflow
		}
	}
	for row := uint32(0); row < c.depth; row++ {
		index := c.index(row, value)
		c.rows[row][index] += occurrences
	}
	c.count += occurrences
	return nil
}

func (c *CountMin) Estimate(value ValueHash) uint64 {
	if c == nil || c.depth == 0 {
		return 0
	}
	estimate := uint64(math.MaxUint64)
	for row := uint32(0); row < c.depth; row++ {
		if count := c.rows[row][c.index(row, value)]; count < estimate {
			estimate = count
		}
	}
	return estimate
}

func (c *CountMin) Merge(other *CountMin) error {
	if c == nil || other == nil || c.seed != other.seed || c.column != other.column ||
		c.depth != other.depth || c.width != other.width {
		return ErrFrequencyConfig
	}
	if math.MaxUint64-c.count < other.count {
		return ErrFrequencyOverflow
	}
	for row := uint32(0); row < c.depth; row++ {
		for col := uint32(0); col < c.width; col++ {
			if math.MaxUint64-c.rows[row][col] < other.rows[row][col] {
				return ErrFrequencyOverflow
			}
		}
	}
	for row := uint32(0); row < c.depth; row++ {
		for col := uint32(0); col < c.width; col++ {
			c.rows[row][col] += other.rows[row][col]
		}
	}
	c.count += other.count
	return nil
}

func (c *CountMin) Total() uint64 {
	if c == nil {
		return 0
	}
	return c.count
}

func (c *CountMin) CollisionBound() uint64 {
	if c == nil || c.width == 0 {
		return 0
	}
	return uint64(math.Ceil(math.E * float64(c.count) / float64(c.width)))
}

func (c *CountMin) index(row uint32, value ValueHash) uint32 {
	var column [4]byte
	var rowID [4]byte
	binary.BigEndian.PutUint32(column[:], c.column)
	binary.BigEndian.PutUint32(rowID[:], row)
	hash := hash128(c.seed, domainCountMin, column[:], rowID[:], value[:])
	return uint32(binary.BigEndian.Uint64(hash[:8]) % uint64(c.width))
}

type RowIdentity [24]byte

type ReservoirItem struct {
	Priority      [16]byte
	RowIdentity   RowIdentity
	Fold          uint8
	ValueHash     ValueHash
	TypedValue    []byte
	ValueTooLarge bool
	index         int
}

type reservoirHeap []*ReservoirItem

func (h reservoirHeap) Len() int { return len(h) }
func (h reservoirHeap) Less(i, j int) bool {
	return compareReservoirItem(h[i], h[j]) > 0
}
func (h reservoirHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *reservoirHeap) Push(value any) {
	item := value.(*ReservoirItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *reservoirHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	old[len(old)-1] = nil
	last.index = -1
	*h = old[:len(old)-1]
	return last
}

type OccurrenceReservoir struct {
	seed     [32]byte
	column   uint32
	capacity int
	items    map[RowIdentity]*ReservoirItem
	heap     reservoirHeap
}

func NewOccurrenceReservoir(seed [32]byte, column uint32, capacity int) (*OccurrenceReservoir, error) {
	if capacity <= 0 {
		return nil, ErrFrequencyConfig
	}
	result := &OccurrenceReservoir{
		seed: seed, column: column, capacity: capacity,
		items: make(map[RowIdentity]*ReservoirItem, capacity),
		heap:  make(reservoirHeap, 0, capacity),
	}
	heap.Init(&result.heap)
	return result, nil
}

func (r *OccurrenceReservoir) Add(identity RowIdentity, fold uint8, value ValueHash, typedValue []byte) error {
	if r == nil {
		return ErrFrequencyConfig
	}
	var column [4]byte
	binary.BigEndian.PutUint32(column[:], r.column)
	priority := hash128(r.seed, domainReservoir, column[:], identity[:])
	item := &ReservoirItem{
		Priority: priority, RowIdentity: identity, Fold: fold, ValueHash: value,
		ValueTooLarge: len(typedValue) > MaxMaterializedValue,
	}
	if !item.ValueTooLarge {
		item.TypedValue = append([]byte(nil), typedValue...)
	}
	return r.insert(item)
}

func (r *OccurrenceReservoir) insert(item *ReservoirItem) error {
	if current, exists := r.items[item.RowIdentity]; exists {
		if compareReservoirPayload(current, item) != 0 {
			return ErrReservoirState
		}
		return nil
	}
	if len(r.heap) < r.capacity {
		r.items[item.RowIdentity] = item
		heap.Push(&r.heap, item)
		return nil
	}
	if compareReservoirItem(item, r.heap[0]) >= 0 {
		return nil
	}
	removed := heap.Pop(&r.heap).(*ReservoirItem)
	delete(r.items, removed.RowIdentity)
	r.items[item.RowIdentity] = item
	heap.Push(&r.heap, item)
	return nil
}

func (r *OccurrenceReservoir) Merge(other *OccurrenceReservoir) error {
	if r == nil || other == nil || r.seed != other.seed || r.column != other.column || r.capacity != other.capacity {
		return ErrFrequencyConfig
	}
	for _, item := range other.items {
		clone := *item
		clone.TypedValue = append([]byte(nil), item.TypedValue...)
		clone.index = -1
		if err := r.insert(&clone); err != nil {
			return err
		}
	}
	return nil
}

func (r *OccurrenceReservoir) Items() []ReservoirItem {
	if r == nil {
		return nil
	}
	items := make([]ReservoirItem, 0, len(r.items))
	for _, item := range r.items {
		clone := *item
		clone.TypedValue = append([]byte(nil), item.TypedValue...)
		clone.index = -1
		items = append(items, clone)
	}
	sort.Slice(items, func(i, j int) bool { return compareReservoirItem(&items[i], &items[j]) < 0 })
	return items
}

func compareReservoirItem(left, right *ReservoirItem) int {
	if cmp := bytes.Compare(left.Priority[:], right.Priority[:]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(left.RowIdentity[:], right.RowIdentity[:])
}

func compareReservoirPayload(left, right *ReservoirItem) int {
	if left.Priority != right.Priority || left.Fold != right.Fold || left.ValueHash != right.ValueHash ||
		left.ValueTooLarge != right.ValueTooLarge {
		return 1
	}
	return bytes.Compare(left.TypedValue, right.TypedValue)
}
