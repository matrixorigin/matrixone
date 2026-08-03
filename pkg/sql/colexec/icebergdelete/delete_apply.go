// Copyright 2026 Matrix Origin
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

package icebergdelete

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const (
	defaultPositionEntryBytes  = int64(24)
	defaultPositionFileBytes   = int64(64)
	defaultEqualityEntryBytes  = int64(64)
	defaultEqualityLayoutBytes = int64(64)
)

type Profile struct {
	PositionRowsFiltered int64
	EqualityRowsFiltered int64
	DeleteFilesRead      int64
	MemoryBytes          int64
	SpillEnabled         bool
}

type Options struct {
	MaxMemoryBytes int64
	SpillEnabled   bool
	// BaseMemoryBytes is memory already consumed by sibling delete states in
	// the same operator. It lets per-row loading enforce one aggregate budget.
	BaseMemoryBytes int64
}

type PositionIndex struct {
	rows        map[string]map[int64]struct{}
	memoryBytes int64
}

func NewPositionIndex() *PositionIndex {
	return &PositionIndex{rows: make(map[string]map[int64]struct{})}
}

func (idx *PositionIndex) Add(dataFile string, rowOrdinal int64) {
	if idx.rows == nil {
		idx.rows = make(map[string]map[int64]struct{})
	}
	dataFile = strings.TrimSpace(dataFile)
	if dataFile == "" || rowOrdinal < 0 {
		return
	}
	rows := idx.rows[dataFile]
	if rows == nil {
		rows = make(map[int64]struct{})
		idx.rows[dataFile] = rows
		idx.memoryBytes = saturatingMemoryAdd(idx.memoryBytes, saturatingMemoryAdd(defaultPositionFileBytes, int64(len(dataFile))))
	}
	if _, exists := rows[rowOrdinal]; exists {
		return
	}
	rows[rowOrdinal] = struct{}{}
	idx.memoryBytes = saturatingMemoryAdd(idx.memoryBytes, defaultPositionEntryBytes)
}

func (idx *PositionIndex) ShouldDelete(dataFile string, rowOrdinal int64) bool {
	if idx == nil || len(idx.rows) == 0 {
		return false
	}
	_, ok := idx.rows[strings.TrimSpace(dataFile)][rowOrdinal]
	return ok
}

func (idx *PositionIndex) MemoryBytes() int64 {
	if idx == nil {
		return 0
	}
	return idx.memoryBytes
}

type EqualityIndex struct {
	keys        map[string]struct{}
	memoryBytes int64
}

func NewEqualityIndex() *EqualityIndex {
	return &EqualityIndex{keys: make(map[string]struct{})}
}

func (idx *EqualityIndex) AddKey(values ...any) {
	if idx.keys == nil {
		idx.keys = make(map[string]struct{})
	}
	key := equalityKey(values)
	if _, exists := idx.keys[key]; exists {
		return
	}
	idx.keys[key] = struct{}{}
	idx.memoryBytes = saturatingMemoryAdd(idx.memoryBytes, saturatingMemoryAdd(defaultEqualityEntryBytes, int64(len(key))))
}

func (idx *EqualityIndex) ShouldDelete(values ...any) bool {
	if idx == nil || len(idx.keys) == 0 {
		return false
	}
	_, ok := idx.keys[equalityKey(values)]
	return ok
}

func (idx *EqualityIndex) MemoryBytes() int64 {
	if idx == nil {
		return 0
	}
	return idx.memoryBytes
}

type ApplyState struct {
	Position        *PositionIndex
	Equality        *EqualityIndex
	EqualityLayouts map[string]*EqualityIndex
	Options         Options
	Profile         Profile
	layoutBytes     int64
}

func NewApplyState(opts Options) *ApplyState {
	return &ApplyState{
		Position: NewPositionIndex(),
		Equality: NewEqualityIndex(),
		Options:  opts,
		Profile:  Profile{SpillEnabled: opts.SpillEnabled},
	}
}

func (s *ApplyState) CheckMemory(ctx context.Context) error {
	if s == nil {
		return nil
	}
	memoryBytes := s.MemoryBytes()
	s.Profile.MemoryBytes = memoryBytes
	return CheckMemoryLimit(ctx, s.Options, saturatingMemoryAdd(s.Options.BaseMemoryBytes, memoryBytes))
}

func (s *ApplyState) MemoryBytes() int64 {
	if s == nil {
		return 0
	}
	memoryBytes := saturatingMemoryAdd(s.Position.MemoryBytes(), s.Equality.MemoryBytes())
	memoryBytes = saturatingMemoryAdd(memoryBytes, s.layoutBytes)
	for _, idx := range s.EqualityLayouts {
		memoryBytes = saturatingMemoryAdd(memoryBytes, idx.MemoryBytes())
	}
	return memoryBytes
}

func CheckMemoryLimit(ctx context.Context, opts Options, memoryBytes int64) error {
	if opts.MaxMemoryBytes <= 0 || memoryBytes <= opts.MaxMemoryBytes {
		return nil
	}
	if opts.SpillEnabled {
		return api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg delete apply spill is not implemented", map[string]string{
			"memory_bytes": fmt.Sprintf("%d", memoryBytes),
			"limit_bytes":  fmt.Sprintf("%d", opts.MaxMemoryBytes),
		}))
	}
	return api.ToMOErr(ctx, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg delete apply exceeded memory limit", map[string]string{
		"memory_bytes": fmt.Sprintf("%d", memoryBytes),
		"limit_bytes":  fmt.Sprintf("%d", opts.MaxMemoryBytes),
	}))
}

func (s *ApplyState) AddEqualityKey(layout string, values ...any) {
	if s == nil {
		return
	}
	layout = strings.TrimSpace(layout)
	if layout == "" {
		s.Equality.AddKey(values...)
		return
	}
	if s.EqualityLayouts == nil {
		s.EqualityLayouts = make(map[string]*EqualityIndex)
	}
	idx := s.EqualityLayouts[layout]
	if idx == nil {
		idx = NewEqualityIndex()
		s.EqualityLayouts[layout] = idx
		s.layoutBytes = saturatingMemoryAdd(s.layoutBytes, saturatingMemoryAdd(defaultEqualityLayoutBytes, int64(len(layout))))
	}
	idx.AddKey(values...)
}

func saturatingMemoryAdd(left, right int64) int64 {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func (s *ApplyState) ApplyEqualityMaskForLayout(ctx context.Context, layout string, rows [][]any) ([]bool, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	if err := s.CheckMemory(ctx); err != nil {
		return nil, err
	}
	keep := make([]bool, len(rows))
	for i := range keep {
		keep[i] = true
	}
	layout = strings.TrimSpace(layout)
	var idx *EqualityIndex
	if layout == "" {
		idx = s.Equality
	} else {
		idx = s.EqualityLayouts[layout]
	}
	if idx == nil || len(idx.keys) == 0 {
		return keep, nil
	}
	for i, row := range rows {
		if idx.ShouldDelete(row...) {
			s.Profile.EqualityRowsFiltered++
			keep[i] = false
		}
	}
	return keep, nil
}

func (s *ApplyState) ApplyPositionMask(ctx context.Context, dataFile string, startRowOrdinal int64, rowCount int) ([]bool, error) {
	if rowCount <= 0 {
		return nil, nil
	}
	if err := s.CheckMemory(ctx); err != nil {
		return nil, err
	}
	keep := make([]bool, rowCount)
	for i := range keep {
		rowOrdinal := startRowOrdinal + int64(i)
		if s.Position.ShouldDelete(dataFile, rowOrdinal) {
			s.Profile.PositionRowsFiltered++
			continue
		}
		keep[i] = true
	}
	return keep, nil
}

func (s *ApplyState) ApplyEqualityMask(ctx context.Context, rows [][]any) ([]bool, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	if err := s.CheckMemory(ctx); err != nil {
		return nil, err
	}
	keep := make([]bool, len(rows))
	for i, row := range rows {
		if s.Equality.ShouldDelete(row...) {
			s.Profile.EqualityRowsFiltered++
			continue
		}
		keep[i] = true
	}
	return keep, nil
}

func RowOrdinals(startRowOrdinal int64, rowCount int) []int64 {
	if rowCount <= 0 {
		return nil
	}
	out := make([]int64, rowCount)
	for i := range out {
		out[i] = startRowOrdinal + int64(i)
	}
	return out
}

func equalityKey(values []any) string {
	var b strings.Builder
	for _, value := range values {
		token := equalityValueToken(value)
		b.WriteString(strconv.Itoa(len(token)))
		b.WriteByte(':')
		b.WriteString(token)
	}
	return b.String()
}

func equalityValueToken(value any) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case bool:
		return "b:" + strconv.FormatBool(v)
	case int:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int8:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int16:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int32:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int64:
		return "i:" + strconv.FormatInt(v, 10)
	case uint:
		return unsignedEqualityValueToken(uint64(v))
	case uint8:
		return unsignedEqualityValueToken(uint64(v))
	case uint16:
		return unsignedEqualityValueToken(uint64(v))
	case uint32:
		return unsignedEqualityValueToken(uint64(v))
	case uint64:
		return unsignedEqualityValueToken(v)
	case float32:
		return "f:" + strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return "f:" + strconv.FormatFloat(v, 'g', -1, 64)
	case string:
		return "s:" + v
	case []byte:
		return "s:" + string(v)
	default:
		return fmt.Sprintf("%T=%#v", value, value)
	}
}

func unsignedEqualityValueToken(value uint64) string {
	if value <= math.MaxInt64 {
		return "i:" + strconv.FormatInt(int64(value), 10)
	}
	return "u:" + strconv.FormatUint(value, 10)
}
